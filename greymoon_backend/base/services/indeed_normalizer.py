"""
indeed_normalizer.py
Maps a raw Indeed Scraper result → unified ServiceLead dict.
"""

import re
from .normalizer import _extract_emails, _extract_phones, _content_hash


def normalize_indeed(item: dict, service_category: str, location_str: str) -> dict:
    """
    Map a raw Indeed Scraper item to the unified lead dict.

    Key fields from the actor output:
      positionName, company, location, salary, jobType, rating,
      reviewsCount, url, id, postedAt, description, descriptionHTML
    """
    title       = item.get("positionName") or item.get("title") or ""
    company     = item.get("company") or ""
    description = item.get("description") or ""
    salary      = item.get("salary") or ""
    job_types   = item.get("jobType") or []
    job_type    = ", ".join(job_types) if isinstance(job_types, list) else str(job_types)
    location    = item.get("location") or location_str or ""
    url         = item.get("url") or ""
    post_id     = str(item.get("id") or "")
    posted_at   = item.get("postedAt") or item.get("scrapedAt") or None
    rating      = item.get("rating")

    # Build a richer title
    full_title = f"{title} @ {company}".strip(" @") if company else title

    # Try to extract phone/email from description
    phone = _extract_phones(description)
    email = _extract_emails(description)

    # Salary as part of the post body for scoring
    salary_note = f"\n\nSalary: {salary}" if salary else ""
    post_body = f"{description}{salary_note}".strip()

    normalized = {
        "post_id":          post_id or _content_hash({"url": url, "title": full_title}),
        "url":              url,
        "title":            full_title[:500],
        "post":             post_body,
        "phone":            phone,
        "email":            email,
        "location":         location,
        "category":         service_category,
        "service_category": service_category,
        "state":            "",
        "latitude":         "",
        "longitude":        "",
        "map_accuracy":     "",
        "datetime":         posted_at,
        "source":           "INDEED",
        "zip_code":         "",
        "fb_group_name":    "",
        "fb_group_url":     "",
        # Extra Indeed-specific fields stored in raw_json
        "raw_json": {
            **item,
            "_indeed_company":  company,
            "_indeed_salary":   salary,
            "_indeed_job_type": job_type,
            "_indeed_rating":   rating,
        },
    }

    normalized["content_hash"] = _content_hash({
        "title": full_title,
        "post":  description[:300],
        "url":   url,
    })

    return normalized