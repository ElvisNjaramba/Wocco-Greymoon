SERVICE_CATEGORY_MAP = {
    "cleaning": {
        "label": "Cleaning Services",
        "craigslist_codes": ["hss", "lbs"],          
        "facebook_keywords": [
            "house cleaning service",
            "residential cleaning contractor",
            "commercial cleaning service",
            "janitorial service",
            "maid service",
        ],
    },
    "maintenance": {
        "label": "Maintenance & Repair",
        "craigslist_codes": ["trd", "sks", "aos"],   
        "facebook_keywords": [
            "home maintenance contractor",
            "handyman service",
            "property maintenance",
            "repair service contractor",
            "building maintenance",
        ],
    },
    "waste_management": {
        "label": "Waste Management",
        "craigslist_codes": ["aos", "fgs"],          
        "facebook_keywords": [
            "junk removal service",
            "waste removal contractor",
            "trash hauling service",
            "debris removal",
            "dumpster rental service",
        ],
    },
}

# All valid category keys for validation
ALL_CATEGORIES = list(SERVICE_CATEGORY_MAP.keys())


def get_craigslist_codes(categories: list[str]) -> list[str]:
    """Return deduplicated CL category codes for a list of service categories."""
    codes = set()
    for cat in categories:
        entry = SERVICE_CATEGORY_MAP.get(cat)
        if entry:
            codes.update(entry["craigslist_codes"])
    return list(codes)


def get_facebook_keywords(categories: list[str]) -> list[str]:
    """Return all FB search keywords for a list of service categories."""
    keywords = []
    for cat in categories:
        entry = SERVICE_CATEGORY_MAP.get(cat)
        if entry:
            keywords.extend(entry["facebook_keywords"])
    return keywords