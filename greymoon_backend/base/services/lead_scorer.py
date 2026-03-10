# ============================================================
#  lead_scorer.py
#  Enhanced version of your original calculate_lead_score.
#  Now handles both Craigslist and Facebook leads, with
#  source-specific scoring signals.
# ============================================================

from datetime import datetime, timezone

HIGH_VALUE_KEYWORDS = [
    "emergency",
    "urgent",
    "asap",
    "immediately",
    "same day",
    "next day",
    "licensed",
    "insured",
    "bonded",
    "certified",
]

PREMIUM_CATEGORIES = [
    "hss",  # household services
    "sks",  # skilled trade
    "cleaning",
    "maintenance",
    "waste_management",
]


def calculate_lead_score(item: dict) -> tuple[int, dict]:
    """
    Score a normalized lead dict (from normalizer.py).
    Returns (score: int, reasons: dict).
    Works for both CRAIGSLIST and FACEBOOK sources.
    """
    score = 0
    reasons = {}

    source = item.get("source", "CRAIGSLIST")
    title = (item.get("title") or "").lower()
    description = (item.get("post") or "").lower()
    combined_text = title + " " + description

    # ── Contact signals (highest value — these are actual leads) ──

    if item.get("phone"):
        score += 30
        reasons["phone"] = 30

    if item.get("email"):
        score += 15
        reasons["email"] = 15

    # ── Location precision ────────────────────────────────────────

    if item.get("latitude") and item.get("longitude"):
        score += 10
        reasons["geolocation"] = 10
    elif item.get("location"):
        score += 3
        reasons["location_text"] = 3

    # ── Category match ────────────────────────────────────────────

    if item.get("service_category") in PREMIUM_CATEGORIES or \
       item.get("category") in PREMIUM_CATEGORIES:
        score += 10
        reasons["premium_category"] = 10

    # ── Urgency / quality keywords ────────────────────────────────

    matched_keywords = [w for w in HIGH_VALUE_KEYWORDS if w in combined_text]
    if matched_keywords:
        keyword_score = min(len(matched_keywords) * 5, 20)
        score += keyword_score
        reasons["keywords"] = keyword_score

    # ── Freshness ─────────────────────────────────────────────────

    post_date = item.get("datetime")
    if post_date:
        try:
            dt = datetime.fromisoformat(str(post_date).replace("Z", "+00:00"))
            age_hours = (datetime.now(timezone.utc) - dt).total_seconds() / 3600
            if age_hours < 24:
                score += 15
                reasons["freshness_24h"] = 15
            elif age_hours < 72:
                score += 8
                reasons["freshness_72h"] = 8
        except Exception:
            pass

    # ── Source-specific signals ───────────────────────────────────

    if source == "FACEBOOK":
        # FB posts with both phone AND location text are gold
        if item.get("phone") and item.get("location"):
            score += 5
            reasons["fb_contact_with_location"] = 5

    elif source == "CRAIGSLIST":
        # CL map accuracy suggests they verified their location
        if item.get("map_accuracy"):
            score += 3
            reasons["cl_map_accuracy"] = 3

    # Cap at 100
    score = min(score, 100)
    return score, reasons