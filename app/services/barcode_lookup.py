import json
import logging
import re
from datetime import datetime, timezone

import httpx
from sqlalchemy.orm import Session

from app.config import settings
from app.models import BarcodeCache
from app.utils import utcnow

logger = logging.getLogger(__name__)


# ── Provider Implementations ────────────────────────────────────────

def lookup_sparkyfitness(barcode: str) -> dict | None:
    """Query local SparkyFitness instance using the v2 API route. Returns product dict or None."""
    if not getattr(settings, "sparkyfitness_enabled", False):
        return None
    if not getattr(settings, "sparkyfitness_url_base", None) or not getattr(settings, "sparkyfitness_api_token", None):
        logger.warning("SparkyFitness enabled but URL base or API token is missing.")
        return None

    base_url = settings.sparkyfitness_url_base.rstrip("/")
    url = f"{base_url}/api/v2/foods/barcode/{barcode}"
    headers = {
        "Authorization": f"Bearer {settings.sparkyfitness_api_token}",
        "Accept": "application/json",
    }

    try:
        resp = httpx.get(url, headers=headers, timeout=5)
        logger.info(f"SparkyFitness {barcode}: HTTP {resp.status_code}")

        if resp.status_code != 200:
            return None

        data = resp.json()

        food = data.get("food")
        if not food:
            return None

        name = food.get("name") or ""
        if not name.strip():
            return None

        brand = food.get("brand") or ""

        variant = food.get("default_variant") or {}
        serving_size = variant.get("serving_size")
        serving_unit = variant.get("serving_unit") or ""

        quantity_str = None
        if serving_size is not None:
            quantity_str = f"{serving_size}{serving_unit}".strip()

        return {
            "title": name.strip(),
            "brand": brand.strip(),
            "product_type": None,
            "quantity": quantity_str,
            "source": "sparkyfitness",
        }
    except httpx.HTTPError as e:
        logger.error(f"SparkyFitness HTTP error for {barcode}: {e}")
        return None
    except Exception as e:
        logger.error(f"SparkyFitness lookup error for {barcode}: {e}")
        return None


def lookup_openfoodfacts(barcode: str) -> dict | None:
    """Query OpenFoodFacts. Returns product dict or None."""
    if not getattr(settings, "off_enabled", True):
        return None
    url = f"{settings.off_url_base}{barcode}.json"
    try:
        resp = httpx.get(url, timeout=5)
        logger.info(f"OpenFoodFacts {barcode}: HTTP {resp.status_code}")
        if resp.status_code == 404:
            return None
        if resp.status_code != 200:
            return None
        data = resp.json()
        if data.get("status") != 1:
            return None
        product = data.get("product", {})
        name = product.get("product_name") or ""
        if not name.strip():
            return None
        return {
            "title": name.strip(),
            "brand": (product.get("brands") or "").split(",")[0].strip(),
            "product_type": (product.get("product_type") or "").split(",")[0].strip() or None,
            "quantity": (product.get("quantity") or "").strip() or None,
            "source": "openfoodfacts",
        }
    except httpx.HTTPError as e:
        logger.error(f"OpenFoodFacts error for {barcode}: {e}")
        return None


def lookup_upcdatabase(barcode: str) -> dict | None:
    """Query UPCDatabase. Returns product dict or None."""
    if not getattr(settings, "upcdb_enabled", False):
        return None
    if not getattr(settings, "upcdb_api_key", None):
        return None
    url = f"{settings.upcdb_url_base}{barcode}"
    try:
        resp = httpx.get(url, params={"apikey": settings.upcdb_api_key}, timeout=5)
        logger.info(f"UPCDatabase {barcode}: HTTP {resp.status_code}")
        if resp.status_code != 200:
            return None
        # UPCDatabase sometimes prepends stray HTML before the JSON
        text = resp.text
        match = re.search(r'\{\s*"', text)
        if not match:
            logger.warning(f"UPCDatabase {barcode}: no JSON object found in response")
            return None
        clean = text[match.start():]
        try:
            data = json.loads(clean)
        except json.JSONDecodeError:
            logger.warning(f"UPCDatabase {barcode}: failed to parse extracted JSON")
            return None
        if not data.get("success"):
            return None
        title = data.get("title") or data.get("alias") or data.get("description") or ""
        if not title.strip():
            return None
        metadata = data.get("metadata") or {}
        return {
            "title": title.strip(),
            "brand": (data.get("brand") or "").split(",")[0].strip(),
            "product_type": (data.get("category") or "").split(",")[0].strip().lower() or None,
            "quantity": (metadata.get("quantity") or "").split(",")[0].strip() or None,
            "source": "upcdatabase",
        }
    except httpx.HTTPError as e:
        logger.error(f"UPCDatabase error for {barcode}: {e}")
        return None


# ── Provider Chain Resolution ────────────────────────────────────────

def _get_lookup_functions() -> tuple:
    """Return (primary_fn, secondary_fn) based on LOOKUP_PRIMARY config.

    Evaluates enabled states for SparkyFitness, Open Food Facts, and UPC Database.
    If the selected primary source is disabled, falls back to the next available provider.
    """
    providers = {
        "sparkyfitness": (
            lookup_sparkyfitness
            if getattr(settings, "sparkyfitness_enabled", False)
            and getattr(settings, "sparkyfitness_url_base", None)
            and getattr(settings, "sparkyfitness_api_token", None)
            else None
        ),
        "off": lookup_openfoodfacts if getattr(settings, "off_enabled", True) else None,
        "upcdb": (
            lookup_upcdatabase
            if getattr(settings, "upcdb_enabled", False) and getattr(settings, "upcdb_api_key", None)
            else None
        ),
    }

    primary_key = getattr(settings, "lookup_primary", "sparkyfitness")
    
    # Priority list starting with the designated primary
    order = ["sparkyfitness", "off", "upcdb"]
    if primary_key in order:
        order.remove(primary_key)
        order.insert(0, primary_key)

    available = [providers[k] for k in order if providers[k] is not None]

    primary = available[0] if len(available) > 0 else None
    secondary = available[1] if len(available) > 1 else None

    return primary, secondary


def _result_has_gaps(result: dict) -> bool:
    """True when any enrichment field is empty."""
    return not all(result.get(f) for f in ("brand", "quantity", "product_type"))


def _merge_gaps(base: dict, supplement: dict) -> bool:
    """Fill empty enrichment fields in *base* from *supplement*.

    Returns True if any field was actually filled.
    """
    changed = False
    for field in ("brand", "quantity", "product_type"):
        if not base.get(field) and supplement.get(field):
            base[field] = supplement[field]
            changed = True
    if changed:
        base["source"] = f"{base['source']}+{supplement['source']}"
    return changed


# ── Primary Lookup & Caching Engine ─────────────────────────────────

def perform_lookup(barcode: str, db: Session) -> BarcodeCache:
    """Lookup barcode in external APIs and upsert into barcode_cache.

    Strategy (``LOOKUP_STRATEGY``):
    * ``failover`` — try primary, use secondary only if primary returns nothing.
    * ``complement`` — try primary, respond with whatever it gives, then
      (optionally in background) fill gaps from the secondary.
      When ``LOOKUP_ENRICH_IN_BACKGROUND`` is *False* the secondary call
      is made synchronously before returning.
    """
    primary_fn, secondary_fn = _get_lookup_functions()

    result = None
    if primary_fn:
        result = primary_fn(barcode)

    if not result:
        # Primary returned nothing — try secondary as full fallback
        if secondary_fn:
            result = secondary_fn(barcode)
    elif (
        getattr(settings, "lookup_strategy", "failover") == "complement"
        and not getattr(settings, "lookup_enrich_in_background", True)
        and secondary_fn
        and _result_has_gaps(result)
    ):
        # Complement mode, synchronous: fill gaps immediately
        supplement = secondary_fn(barcode)
        if supplement:
            _merge_gaps(result, supplement)

    # --- Upsert Cache ---
    existing = db.get(BarcodeCache, barcode)
    now = utcnow()

    if result:
        if existing:
            existing.source = result["source"]
            existing.title = result["title"]
            existing.brand = result["brand"]
            existing.quantity = result["quantity"]
            existing.product_type = result["product_type"]
            existing.found = True
            existing.lookup_attempted_at = now
        else:
            existing = BarcodeCache(
                barcode=barcode,
                source=result["source"],
                title=result["title"],
                brand=result["brand"],
                quantity=result["quantity"],
                product_type=result["product_type"],
                found=True,
                lookup_attempted_at=now,
                created_at=now,
            )
            db.add(existing)
    else:
        if existing:
            existing.source = "not_found"
            existing.found = False
            existing.lookup_attempted_at = now
        else:
            existing = BarcodeCache(
                barcode=barcode,
                source="not_found",
                found=False,
                lookup_attempted_at=now,
                created_at=now,
            )
            db.add(existing)

    db.commit()
    db.refresh(existing)
    return existing


def needs_background_enrich(cached: BarcodeCache) -> bool:
    """Return True if a background enrichment call should be scheduled."""
    if getattr(settings, "lookup_strategy", "failover") != "complement":
        return False
    if not getattr(settings, "lookup_enrich_in_background", True):
        return False  # Already processed synchronously
    if not cached.found:
        return False
    _, secondary_fn = _get_lookup_functions()
    if secondary_fn is None:
        return False
    return not all([cached.brand, cached.quantity, cached.product_type])


def enrich_barcode_background(barcode: str) -> None:
    """Background task: call secondary API and fill gaps in cache.

    Runs outside the request lifecycle — creates its own DB session.
    """
    from app.database import SessionLocal

    _, secondary_fn = _get_lookup_functions()
    if secondary_fn is None:
        return

    supplement = secondary_fn(barcode)
    if not supplement:
        logger.info("Background enrich %s: secondary returned nothing", barcode)
        return

    db = SessionLocal()
    try:
        cached = db.get(BarcodeCache, barcode)
        if not cached or not cached.found:
            return

        changed = False
        for field in ("brand", "quantity", "product_type"):
            if not getattr(cached, field) and supplement.get(field):
                setattr(cached, field, supplement[field])
                changed = True

        if changed:
            cached.source = f"{cached.source}+{supplement['source']}"
            db.commit()
            logger.info("Background enrich %s: filled gaps → %s", barcode, cached.source)
        else:
            logger.debug("Background enrich %s: no new data from secondary", barcode)
    finally:
        db.close()