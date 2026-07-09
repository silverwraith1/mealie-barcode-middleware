import json
import logging
from datetime import datetime, timezone

import httpx
from sqlalchemy.orm import Session

from app.config import settings
from app.models import BarcodeCache, BarcodeMapping, Item, Activity, RetryQueue
from app.utils import utcnow

logger = logging.getLogger(__name__)


def _headers() -> dict:
    return {
        "Authorization": f"Bearer {settings.mealie_api_key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def check_connectivity() -> bool:
    """Check if Mealie is reachable."""
    try:
        resp = httpx.get(
            f"{settings.mealie_url}/api/app/about",
            headers=_headers(),
            timeout=5,
        )
        return resp.status_code == 200
    except httpx.HTTPError:
        return False


def sync_items(db: Session) -> int:
    """Fetch all items from Mealie, upsert into items table, detect stale. Returns count."""
    url = f"{settings.mealie_url}/api/foods"
    try:
        resp = httpx.get(url, headers=_headers(), params={"perPage": -1}, timeout=30)
        resp.raise_for_status()
    except httpx.HTTPError as e:
        logger.error(f"Failed to sync items from Mealie: {e}")
        raise

    data = resp.json()
    if isinstance(data, dict):
        items = data.get("items")
        if items is None:
            logger.error(f"Unexpected Mealie response structure: {list(data.keys())}")
            raise ValueError("Mealie API returned unexpected response (no 'items' key)")
    elif isinstance(data, list):
        items = data
    else:
        raise ValueError(f"Mealie API returned unexpected type: {type(data).__name__}")
    sync_started = utcnow()
    count = 0

    for food in items:
        item_id = food.get("id")
        if not item_id:
            continue
        name = food.get("name") or food.get("label") or ""
        aliases_raw = food.get("aliases") or []
        aliases_list = [a.get("name", a) if isinstance(a, dict) else a for a in aliases_raw]
        aliases_json = json.dumps(aliases_list)

        existing = db.get(Item, item_id)
        if existing:
            existing.name = name
            existing.aliases = aliases_json
            existing.synced_at = sync_started
        else:
            db.add(Item(id=item_id, name=name, source="mealie", aliases=aliases_json, synced_at=sync_started))
        count += 1

    db.flush()

    # Detect stale items (deleted in Mealie since last sync)
    stale_items = (
        db.query(Item)
        .filter(Item.source == "mealie", Item.synced_at < sync_started)
        .all()
    )
    for stale in stale_items:
        # Find broken mappings
        broken = db.query(BarcodeMapping).filter(BarcodeMapping.item_id == stale.id).all()
        for m in broken:
            db.add(Activity(
                barcode=m.barcode,
                title="Mapping broken",
                message=f"{stale.name} was deleted in Mealie — remap needed",
                result="broken",
            ))
            db.delete(m)
        db.delete(stale)
        if broken:
            logger.warning(f"Stale item '{stale.name}' removed, {len(broken)} mapping(s) broken")

    db.commit()
    logger.info(f"Synced {count} items from Mealie")
    return count


def add_shopping_item(item_id: str) -> tuple[bool, str | None]:
    """Add item to Mealie shopping list via food ID. Returns (success, created_item_id)."""
    payload = {
        "shoppingListId": settings.mealie_shopping_list_id,
        "foodId": item_id,
        "quantity": 1,
    }
    return _post_shopping_item(payload)


def add_shopping_note(note: str) -> tuple[bool, str | None]:
    """Add a plain note to the Mealie shopping list. Returns (success, created_item_id)."""
    payload = {
        "shoppingListId": settings.mealie_shopping_list_id,
        "note": note,
    }
    return _post_shopping_item(payload)


def add_to_shopping_list_by_item(item_id: str) -> bool:
    """Bool wrapper for callers that don't need the created item id."""
    return add_shopping_item(item_id)[0]


def add_to_shopping_list_by_note(note: str) -> bool:
    """Bool wrapper for callers that don't need the created item id."""
    return add_shopping_note(note)[0]


def _post_shopping_item(payload: dict) -> tuple[bool, str | None]:
    """POST to Mealie shopping items endpoint.

    Returns ``(success, created_item_id)``. ``success`` reflects the HTTP
    result only; ``created_item_id`` is the id of the newly created line
    (``createdItems[0].id`` in Mealie's ``ShoppingListItemsCollectionOut``)
    or ``None`` if it could not be parsed. A successful POST that returns no
    parseable id is still ``(True, None)`` so callers never mistake it for a
    failure and enqueue a duplicate retry.
    """
    url = f"{settings.mealie_url}/api/households/shopping/items"
    try:
        resp = httpx.post(url, headers=_headers(), json=payload, timeout=3)
        if resp.status_code in (200, 201):
            item_id = None
            try:
                created = resp.json().get("createdItems") or []
                if created:
                    item_id = created[0].get("id")
            except (ValueError, TypeError, AttributeError):
                logger.warning("Mealie shopping POST succeeded but created id could not be parsed")
            return True, item_id
        logger.warning(f"Mealie shopping POST returned {resp.status_code}: {resp.text}")
        return False, None
    except httpx.HTTPError as e:
        logger.error(f"Mealie shopping POST failed: {e}")
        return False, None


def _get_shopping_item(item_id: str) -> dict | None:
    """GET a single Mealie shopping list item. Returns the item dict or None."""
    url = f"{settings.mealie_url}/api/households/shopping/items/{item_id}"
    try:
        resp = httpx.get(url, headers=_headers(), timeout=3)
        if resp.status_code == 200:
            return resp.json()
        if resp.status_code != 404:
            logger.warning(f"Mealie shopping GET {item_id} returned {resp.status_code}")
        return None
    except (httpx.HTTPError, ValueError) as e:
        logger.error(f"Mealie shopping GET {item_id} failed: {e}")
        return None


def _put_shopping_item(item_id: str, payload: dict) -> bool:
    """PUT (update) a single Mealie shopping list item. Returns True on success."""
    url = f"{settings.mealie_url}/api/households/shopping/items/{item_id}"
    try:
        resp = httpx.put(url, headers=_headers(), json=payload, timeout=3)
        if resp.status_code in (200, 201):
            return True
        logger.warning(f"Mealie shopping PUT {item_id} returned {resp.status_code}: {resp.text}")
        return False
    except httpx.HTTPError as e:
        logger.error(f"Mealie shopping PUT {item_id} failed: {e}")
        return False


def reconcile_linked_barcode(barcode: str) -> None:
    """Reconcile Mealie after a barcode is linked to an item.

    Runs as a background task with its own DB session — never blocks the
    request and never raises into the caller. Two things happen:

    1. Any *pending* retry-queue payload for this barcode (a note that never
       made it to Mealie) is rewritten to reference the linked item, so when
       it eventually posts it lands correctly.
    2. If a shopping-list line was already created for this barcode via a note
       (tracked in ``BarcodeCache.shopping_item_id``) it is updated in place
       via PUT: linked to the food (mealie items) or renamed (manual items),
       preserving quantity/checked/position. Lines the user already checked
       off or deleted are left untouched.
    """
    from app.database import SessionLocal

    db = SessionLocal()
    try:
        mapping = db.get(BarcodeMapping, barcode)
        if not mapping:
            return
        item = db.get(Item, mapping.item_id)
        if not item:
            return

        # 1) Rewrite any pending retry-queue payload to the linked item.
        pending = db.query(RetryQueue).filter(RetryQueue.barcode == barcode).all()
        rewrote = False
        for entry in pending:
            try:
                payload = json.loads(entry.payload)
            except (ValueError, TypeError):
                continue
            if item.source == "mealie":
                payload.pop("note", None)
                payload["foodId"] = item.id
                payload.setdefault("quantity", 1)
            else:
                payload.pop("foodId", None)
                payload["note"] = item.name
            entry.payload = json.dumps(payload)
            rewrote = True
        if rewrote:
            db.commit()

        # 2) Reconcile an already-added note line, if one was tracked.
        cached = db.get(BarcodeCache, barcode)
        shopping_item_id = cached.shopping_item_id if cached else None
        if not shopping_item_id:
            return

        current = _get_shopping_item(shopping_item_id)
        if current is None or current.get("checked"):
            # Deleted in Mealie, or already checked off by the user — leave it
            # alone and just drop our stale handle.
            cached.shopping_item_id = None
            db.commit()
            return

        payload = {
            "shoppingListId": current.get("shoppingListId") or settings.mealie_shopping_list_id,
            "quantity": current.get("quantity", 1),
            "checked": current.get("checked", False),
            "position": current.get("position", 0),
        }
        if item.source == "mealie":
            payload["foodId"] = item.id
            payload["note"] = ""
        else:
            payload["foodId"] = None
            payload["note"] = item.name

        if _put_shopping_item(shopping_item_id, payload):
            cached.shopping_item_id = None
            db.commit()
            logger.info(
                "Reconciled shopping item %s for barcode %s -> %s",
                shopping_item_id, barcode, item.name,
            )
    finally:
        db.close()


def enqueue_retry(barcode: str, payload: dict, db: Session) -> None:
    """Add a failed Mealie request to the retry queue (skip if already pending)."""
    existing = db.query(RetryQueue).filter(RetryQueue.barcode == barcode).first()
    if existing:
        logger.info(f"Retry entry already pending for barcode={barcode}, skipping duplicate")
        return
    db.add(RetryQueue(
        barcode=barcode,
        payload=json.dumps(payload),
        attempts=0,
        next_retry_at=utcnow(),
        created_at=utcnow(),
    ))
    db.commit()
