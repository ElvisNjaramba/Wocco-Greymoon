

import hashlib
import re
import json

def _extract_emails(text: str) -> str | None:
    if not text:
        return None
    matches = re.findall(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+", text)
    return matches[0] if matches else None


def _extract_phones(text: str) -> str | None:
    if not text:
        return None
    matches = re.findall(
        r"(\+?1?\s?[\(\-]?\d{3}[\)\-\s]?\s?\d{3}[\-\s]?\d{4})", text
    )
    return matches[0].strip() if matches else None


def _content_hash(data: dict) -> str:
    key = json.dumps(data, sort_keys=True, default=str)
    return hashlib.sha256(key.encode()).hexdigest()

def normalize_craigslist(item: dict, service_category: str) -> dict:
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
        "post_id":          post_id or _content_hash({"url": url, "title": title}),
        "url":              url,
        "title":            title,
        "post":             description,
        "phone":            phone,
        "email":            email,
        "location":         item.get("location") or item.get("city") or "",
        "category":         item.get("category") or service_category,
        "service_category": service_category,
        "state":            item.get("state") or "",
        "latitude":         str(item.get("latitude") or ""),
        "longitude":        str(item.get("longitude") or ""),
        "map_accuracy":     str(item.get("mapAccuracy") or ""),
        "datetime":         item.get("datetime") or item.get("date") or None,
        "source":           "CRAIGSLIST",
        "zip_code":         item.get("zip_code") or "",
        "raw_json":         item,
    }

    normalized["content_hash"] = _content_hash({
        "title_norm": _normalize_title(title),
        "post":  description[:300],   # remove url from hash — url varies, content doesn't
    })

    return normalized


import re

def _normalize_title(title: str) -> str:
    """Lowercase, strip punctuation/whitespace for fuzzy title comparison."""
    if not title:
        return ""
    t = title.lower().strip()
    t = re.sub(r'[\W_]+', ' ', t)   # replace non-alphanumeric with space
    t = re.sub(r'\s+', ' ', t).strip()
    return t

def normalize_facebook(
    item: dict,
    service_category: str,
    location_str: str,
    zip_code: str | None = None,
) -> dict:

    text = (
        item.get("text")
        or item.get("postText")
        or item.get("message")
        or item.get("body")
        or ""
    )

    author     = item.get("authorName") or item.get("author") or "Unknown"
    group_name = (
        item.get("groupName") or item.get("group") or
        item.get("groupTitle") or item.get("group_name") or ""
    )
    group_url = (
        item.get("groupUrl") or item.get("group_url") or
        item.get("groupLink") or ""
    )

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

    resolved_zip = zip_code or _extract_zip_from_text(text) or ""

    normalized = {
        "post_id":          post_id or _content_hash({"url": post_url, "text": text[:200]}),
        "url":              post_url,
        "title":            title,
        "post":             text,
        "phone":            phone,
        "email":            email,
        "location":         location_str,
        "category":         service_category,
        "service_category": service_category,
        "state":            "",       
        "latitude":         str(item.get("latitude") or ""),
        "longitude":        str(item.get("longitude") or ""),
        "map_accuracy":     "",
        "datetime":         post_date,
        "source":           "FACEBOOK",
        "zip_code":         resolved_zip,
        "fb_group_name":    group_name,
        "fb_group_url":     group_url,
        "raw_json":         item,
    }

    normalized["content_hash"] = _content_hash({
        "title_norm": _normalize_title(title),
        "post":  text[:300],
    })
    return normalized


def _extract_zip_from_text(text: str) -> str | None:
    """Pull the first 5-digit ZIP code from post text."""
    if not text:
        return None
    match = re.search(r"\b(\d{5})(?:-\d{4})?\b", text)
    return match.group(1) if match else None