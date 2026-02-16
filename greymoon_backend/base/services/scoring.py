from datetime import datetime, timezone

HIGH_VALUE_KEYWORDS = [
    "emergency",
    "urgent",
    "asap",
    "immediately",
]

PREMIUM_CATEGORIES = [
    "hss",  # household services
    "skd",  # skilled trade
]

def calculate_lead_score(item):
    score = 0
    reasons = {}

    title = (item.get("title") or "").lower()
    description = (item.get("post") or "").lower()

    # 🔹 Phone
    phone_numbers = item.get("phoneNumbers", [])
    if phone_numbers:
        score += 25
        reasons["phone"] = 25

    # 🔹 Email
    if "@" in description:
        score += 15
        reasons["email"] = 15

    # 🔹 Zip
    if any(char.isdigit() for char in description):
        score += 5
        reasons["zip"] = 5

    # 🔹 Geo location
    if item.get("latitude") and item.get("longitude"):
        score += 10
        reasons["geolocation"] = 10

    # 🔹 Premium category
    if item.get("category") in PREMIUM_CATEGORIES:
        score += 10
        reasons["premium_category"] = 10

    # 🔹 High urgency words
    for word in HIGH_VALUE_KEYWORDS:
        if word in title or word in description:
            score += 15
            reasons["urgency"] = 15
            break

    # 🔹 Freshness boost (new posts)
    post_date = item.get("datetime")
    if post_date:
        try:
            dt = datetime.fromisoformat(post_date.replace("Z", "+00:00"))
            age_hours = (datetime.now(timezone.utc) - dt).total_seconds() / 3600
            if age_hours < 24:
                score += 10
                reasons["freshness"] = 10
        except:
            pass

    # Cap score at 100
    score = min(score, 100)

    return score, reasons
