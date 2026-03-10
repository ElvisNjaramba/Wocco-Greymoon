# ============================================================
#  normalizer.py
#  The great unifier. Raw Craigslist JSON and raw Facebook JSON
#  look nothing alike — this file bridges both into the same
#  clean ServiceLead-compatible dict.
#
#  Both sources produce:
#    post_id, url, title, post, phone, email, location,
#    category, state, latitude, longitude, source, raw_json
# ============================================================

import hashlib
import re
import json


# ── Shared helpers ────────────────────────────────────────────

def _extract_emails(text: str) -> str | None:
    """Pull the first email address found in a block of text."""
    if not text:
        return None
    matches = re.findall(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+", text)
    return matches[0] if matches else None


def _extract_phones(text: str) -> str | None:
    """Pull the first phone number found in a block of text."""
    if not text:
        return None
    matches = re.findall(
        r"(\+?1?\s?[\(\-]?\d{3}[\)\-\s]?\s?\d{3}[\-\s]?\d{4})", text
    )
    return matches[0].strip() if matches else None


def _content_hash(data: dict) -> str:
    """Stable fingerprint for deduplication across runs and sources."""
    key = json.dumps(data, sort_keys=True, default=str)
    return hashlib.sha256(key.encode()).hexdigest()


# ── Craigslist normalizer ─────────────────────────────────────

def normalize_craigslist(item: dict, service_category: str) -> dict:
    """
    Map a raw Craigslist scraper result → unified lead dict.
    """
    description = item.get("post") or item.get("description") or ""
    title = item.get("title") or ""

    phone_from_field = None
    phones = item.get("phoneNumbers", [])
    if phones:
        phone_from_field = phones[0] if isinstance(phones[0], str) else str(phones[0])

    phone = phone_from_field or _extract_phones(description)
    email = _extract_emails(description)

    post_id = str(item.get("id") or item.get("postId") or "")
    url = item.get("url") or ""

    normalized = {
        "post_id": post_id or _content_hash({"url": url, "title": title}),
        "url": url,
        "title": title,
        "post": description,
        "phone": phone,
        "email": email,
        "location": item.get("location") or item.get("city") or "",
        "category": item.get("category") or service_category,
        "service_category": service_category,
        "state": item.get("state") or "",
        "latitude": str(item.get("latitude") or ""),
        "longitude": str(item.get("longitude") or ""),
        "map_accuracy": str(item.get("mapAccuracy") or ""),
        "datetime": item.get("datetime") or item.get("date") or None,
        "source": "CRAIGSLIST",
        "raw_json": item,
    }

    normalized["content_hash"] = _content_hash({
        "title": title,
        "post": description,
        "url": url,
    })

    return normalized


# ── Facebook normalizer ───────────────────────────────────────

def normalize_facebook(item: dict, service_category: str, location_str: str) -> dict:
    """
    Map a raw Facebook Groups Scraper result → unified lead dict.

    Facebook posts don't always have phone/email in structured fields,
    so we mine the post text aggressively.
    """
    text = (
        item.get("text")
        or item.get("postText")
        or item.get("message")
        or item.get("body")
        or ""
    )

    author = item.get("authorName") or item.get("author") or "Unknown"
    group_name = item.get("groupName") or item.get("group") or ""
    post_url = (
        item.get("url")
        or item.get("postUrl")
        or item.get("link")
        or ""
    )
    post_id = str(
        item.get("id")
        or item.get("postId")
        or item.get("fbId")
        or ""
    )

    # Build a meaningful title from what we have
    title = (
        item.get("title")
        or (text[:120].replace("\n", " ").strip() if text else f"Post by {author}")
    )

    phone = _extract_phones(text)
    email = _extract_emails(text)

    post_date = (
        item.get("datetime")
        or item.get("date")
        or item.get("time")
        or item.get("timestamp")
        or None
    )

    normalized = {
        "post_id": post_id or _content_hash({"url": post_url, "text": text[:200]}),
        "url": post_url,
        "title": title,
        "post": text,
        "phone": phone,
        "email": email,
        "location": location_str,
        "category": service_category,
        "service_category": service_category,
        "state": "",        # not always available from FB
        "latitude": str(item.get("latitude") or ""),
        "longitude": str(item.get("longitude") or ""),
        "map_accuracy": "",
        "datetime": post_date,
        "source": "FACEBOOK",
        "raw_json": item,
    }

    normalized["content_hash"] = _content_hash({
        "title": title,
        "post": text,
        "url": post_url,
    })

    return normalized