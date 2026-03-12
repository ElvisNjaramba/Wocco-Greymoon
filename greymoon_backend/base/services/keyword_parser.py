import re
import difflib


_SERVICE_WORDS = {
    # cleaning
    "cleaning", "cleaner", "clean", "cleaners",
    "maid", "housekeeping", "housekeeper",
    "janitorial", "janitor",
    "washing", "wash",
    "pressure", "power",
    "carpet", "window",
    "sanitize", "sanitizing", "sanitation",
    # maintenance
    "maintenance", "maintain",
    "repair", "repairs",
    "handyman",
    "plumbing", "plumber",
    "electrician", "electrical", "electric",
    "hvac",
    "roofing", "roofer", "roof",
    "painting", "painter", "paint",
    "lawn",
    "landscaping", "landscaper", "landscape",
    "pest", "exterminator",
    "remodel", "remodeling",
    "renovation", "renovating",
    "flooring", "floor",
    "drywall",
    "carpentry", "carpenter",
    # waste
    "removal", "remove",
    "junk",
    "trash",
    "debris",
    "rubbish",
    "garbage",
    "dumpster",
    "hauling", "haul",
    "waste",
    # generic descriptors
    "service", "services",
    "contractor", "contractors",
    "company", "companies",
    "professional", "professionals",
    "specialist", "specialists",
    "provider", "providers",
    "business",
    # qualifiers
    "commercial", "residential", "industrial",
    "emergency", "urgent", "asap",
    "licensed", "insured", "bonded", "certified",
    "affordable", "cheap", "best", "top", "quality",
    "local", "near", "nearby", "around", "area", "region",
    # stop words
    "and", "the", "for", "with", "of", "to", "a", "an",
    "in", "at", "on", "by", "or", "is", "are", "was",
}


def _is_service_word(token: str) -> bool:
    """
    Return True if token is a service/descriptor word rather than a location.
    Exact + simple depluralize + fuzzy check at 0.85 threshold.
    """
    t = token.lower().strip()
    if t in _SERVICE_WORDS:
        return True
    if t.rstrip("s") in _SERVICE_WORDS:
        return True
    matches = difflib.get_close_matches(t, _SERVICE_WORDS, n=1, cutoff=0.85)
    return bool(matches)

_CATEGORY_HINTS: list[tuple[str, str]] = sorted([
    # cleaning
    ("house cleaning",           "cleaning"),
    ("home cleaning",            "cleaning"),
    ("residential cleaning",     "cleaning"),
    ("commercial cleaning",      "cleaning"),
    ("office cleaning",          "cleaning"),
    ("deep cleaning",            "cleaning"),
    ("move out cleaning",        "cleaning"),
    ("move-out cleaning",        "cleaning"),
    ("post construction clean",  "cleaning"),
    ("carpet cleaning",          "cleaning"),
    ("window cleaning",          "cleaning"),
    ("window washing",           "cleaning"),
    ("pressure washing",         "cleaning"),
    ("power washing",            "cleaning"),
    ("power wash",               "cleaning"),
    ("janitorial service",       "cleaning"),
    ("janitorial",               "cleaning"),
    ("maid service",             "cleaning"),
    ("maid",                     "cleaning"),
    ("housekeeping",             "cleaning"),
    ("house cleaner",            "cleaning"),
    ("cleaning service",         "cleaning"),
    ("cleaning contractor",      "cleaning"),
    ("sanitation service",       "cleaning"),
    ("sanitation",               "cleaning"),
    ("cleaner",                  "cleaning"),
    ("cleaning",                 "cleaning"),
    # maintenance
    ("home repair",              "maintenance"),
    ("home maintenance",         "maintenance"),
    ("property maintenance",     "maintenance"),
    ("building maintenance",     "maintenance"),
    ("general contractor",       "maintenance"),
    ("handyman service",         "maintenance"),
    ("handyman",                 "maintenance"),
    ("plumbing service",         "maintenance"),
    ("plumber",                  "maintenance"),
    ("plumbing",                 "maintenance"),
    ("electrician",              "maintenance"),
    ("electrical service",       "maintenance"),
    ("electrical",               "maintenance"),
    ("hvac service",             "maintenance"),
    ("hvac",                     "maintenance"),
    ("roofing service",          "maintenance"),
    ("roofer",                   "maintenance"),
    ("roofing",                  "maintenance"),
    ("painting service",         "maintenance"),
    ("painter",                  "maintenance"),
    ("painting",                 "maintenance"),
    ("lawn mowing",              "maintenance"),
    ("grass cutting",            "maintenance"),
    ("grass mowing",             "maintenance"),
    ("mowing service",           "maintenance"),
    ("yard work",                "maintenance"),
    ("yard maintenance",         "maintenance"),
    ("tree trimming",            "maintenance"),
    ("tree removal",             "maintenance"),
    ("gutter cleaning",          "cleaning"),
    ("pressure clean",           "cleaning"),
    ("move in cleaning",         "cleaning"),
    ("move-in cleaning",         "cleaning"),
    ("post construction clean",  "cleaning"),
    ("lawn care",                "maintenance"),
    ("lawn service",             "maintenance"),
    ("landscaping service",      "maintenance"),
    ("landscaping",              "maintenance"),
    ("landscaper",               "maintenance"),
    ("pest control",             "maintenance"),
    ("exterminator",             "maintenance"),
    ("remodeling",               "maintenance"),
    ("remodel",                  "maintenance"),
    ("renovation",               "maintenance"),
    ("flooring service",         "maintenance"),
    ("flooring",                 "maintenance"),
    ("drywall",                  "maintenance"),
    ("carpentry",                "maintenance"),
    ("carpenter",                "maintenance"),
    ("repair service",           "maintenance"),
    ("repair contractor",        "maintenance"),
    ("maintenance service",      "maintenance"),
    ("maintenance contractor",   "maintenance"),
    ("maintenance",              "maintenance"),
    # waste management
    ("junk removal service",     "waste_management"),
    ("junk removal",             "waste_management"),
    ("trash removal",            "waste_management"),
    ("waste removal",            "waste_management"),
    ("debris removal",           "waste_management"),
    ("rubbish removal",          "waste_management"),
    ("garbage removal",          "waste_management"),
    ("dumpster rental",          "waste_management"),
    ("dumpster service",         "waste_management"),
    ("dumpster",                 "waste_management"),
    ("hauling service",          "waste_management"),
    ("haul away",                "waste_management"),
    ("junk hauling",             "waste_management"),
    ("trash hauling",            "waste_management"),
    ("waste management",         "waste_management"),
    ("trash pickup",             "waste_management"),
    ("bulk pickup",              "waste_management"),
    ("junk",                     "waste_management"),
], key=lambda x: len(x[0]), reverse=True)


def _fuzzy_contains(text: str, phrase: str, threshold: float = 0.82) -> bool:

    text_l   = text.lower()
    phrase_l = phrase.lower()

    # Fast path: exact substring
    if phrase_l in text_l:
        return True

    phrase_words = phrase_l.split()

    if len(phrase_words) == 1:
        # Single-word: character sliding window.
        # Use a slightly tighter threshold (0.86) to prevent short-word
        # false positives like "removel" matching "remodel".
        single_threshold = max(threshold, 0.86)
        pw = len(phrase_l)
        if pw <= len(text_l):
            for start in range(len(text_l) - pw + 1):
                window = text_l[start:start + pw]
                if difflib.SequenceMatcher(None, window, phrase_l).ratio() >= single_threshold:
                    return True
    else:
        # Multi-word: word-level n-gram window
        # Compare full n-gram chunk so word order and content both matter
        text_words = text_l.split()
        n = len(phrase_words)
        for start in range(len(text_words) - n + 1):
            chunk = " ".join(text_words[start:start + n])
            if difflib.SequenceMatcher(None, chunk, phrase_l).ratio() >= threshold:
                return True

    return False

_STATE_ABBREVS = {
    "AL": "Alabama",       "AK": "Alaska",        "AZ": "Arizona",
    "AR": "Arkansas",      "CA": "California",    "CO": "Colorado",
    "CT": "Connecticut",   "DE": "Delaware",      "DC": "District of Columbia",
    "FL": "Florida",       "GA": "Georgia",       "HI": "Hawaii",
    "ID": "Idaho",         "IL": "Illinois",      "IN": "Indiana",
    "IA": "Iowa",          "KS": "Kansas",        "KY": "Kentucky",
    "LA": "Louisiana",     "ME": "Maine",         "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan",      "MN": "Minnesota",
    "MS": "Mississippi",   "MO": "Missouri",      "MT": "Montana",
    "NE": "Nebraska",      "NV": "Nevada",        "NH": "New Hampshire",
    "NJ": "New Jersey",    "NM": "New Mexico",    "NY": "New York",
    "NC": "North Carolina","ND": "North Dakota",  "OH": "Ohio",
    "OK": "Oklahoma",      "OR": "Oregon",        "PA": "Pennsylvania",
    "RI": "Rhode Island",  "SC": "South Carolina","SD": "South Dakota",
    "TN": "Tennessee",     "TX": "Texas",         "UT": "Utah",
    "VT": "Vermont",       "VA": "Virginia",      "WA": "Washington",
    "WV": "West Virginia", "WI": "Wisconsin",     "WY": "Wyoming",
}

_STATE_NAMES_LOWER = {v.lower() for v in _STATE_ABBREVS.values()}

# Multi-word entries must appear before single-word so n-gram wins
_KNOWN_LOCATIONS: list[str] = sorted([
    # multi-word states
    "new hampshire", "new jersey", "new mexico", "new york",
    "north carolina", "north dakota", "south carolina", "south dakota",
    "west virginia", "rhode island", "district of columbia",
    # single-word states
    "alabama", "alaska", "arizona", "arkansas", "california", "colorado",
    "connecticut", "delaware", "florida", "georgia", "hawaii", "idaho",
    "illinois", "indiana", "iowa", "kansas", "kentucky", "louisiana",
    "maine", "maryland", "massachusetts", "michigan", "minnesota",
    "mississippi", "missouri", "montana", "nebraska", "nevada", "ohio",
    "oklahoma", "oregon", "pennsylvania", "tennessee", "texas", "utah",
    "vermont", "virginia", "washington", "wisconsin", "wyoming",
    # multi-word cities
    "los angeles", "san francisco", "san antonio", "san diego", "san jose",
    "las vegas", "new orleans", "kansas city", "virginia beach",
    "long beach", "colorado springs", "el paso", "fort worth",
    "corpus christi", "santa ana", "st louis", "saint louis",
    "st paul", "saint paul", "oklahoma city", "fort wayne",
    "grand rapids", "little rock", "baton rouge", "jersey city",
    "salt lake city", "sioux falls", "des moines", "cedar rapids",
    "st petersburg", "saint petersburg",
    # single major cities
    "houston", "dallas", "chicago", "phoenix", "philadelphia",
    "jacksonville", "columbus", "indianapolis", "seattle", "denver",
    "nashville", "boston", "portland", "memphis", "louisville",
    "baltimore", "milwaukee", "albuquerque", "tucson", "fresno",
    "sacramento", "mesa", "atlanta", "omaha", "raleigh", "tampa",
    "honolulu", "anaheim", "lexington", "aurora", "pittsburgh",
    "anchorage", "stockton", "cincinnati", "toledo", "greensboro",
    "newark", "plano", "henderson", "lincoln", "buffalo", "orlando",
    "cleveland", "laredo", "madison", "durham", "lubbock", "garland",
    "glendale", "hialeah", "reno", "boise", "irvine", "scottsdale",
    "norfolk", "chandler", "fremont", "gilbert", "birmingham",
    "rochester", "richmond", "spokane", "montgomery", "modesto",
    "fayetteville", "tacoma", "shreveport", "augusta", "oxnard",
    "fontana", "huntington beach", "akron", "yonkers", "chattanooga",
    "fort lauderdale", "tempe", "ontario", "oceanside", "santa clara",
    "tallahassee", "huntsville", "worcester", "knoxville", "providence",
    "overland park", "brownsville", "santa rosa", "peoria", "mobile",
], key=lambda x: (0 if " " in x else 1, -len(x)))


# ── Public API ────────────────────────────────────────────────

def extract_categories(phrases: list[str]) -> list[str]:

    from .category_map import ALL_CATEGORIES

    text    = " ".join(phrases)
    matched: set[str] = set()

    for hint, cat in _CATEGORY_HINTS:
        if _fuzzy_contains(text, hint, threshold=0.82):
            matched.add(cat)

    return list(matched) if matched else list(ALL_CATEGORIES)


def extract_location(phrases: list[str]) -> tuple[str | None, str | None]:

    combined = " ".join(phrases)

    # ── 1. 2-letter state abbreviation ───────────────────────
    abbrev_m = re.search(r'\b([A-Z]{2})\b', combined)
    if abbrev_m and abbrev_m.group(1) in _STATE_ABBREVS:
        return "state", _STATE_ABBREVS[abbrev_m.group(1)]

    # ── 2. Preposition-scoped extraction ─────────────────────
    prep_m = re.search(
        r'\b(?:in|near|around|for|at|within|serving|covering)\s+(.+?)(?:[,.]|$)',
        combined,
        re.IGNORECASE,
    )
    if prep_m:
        result = _fuzzy_match_location_tokens(prep_m.group(1).strip())
        if result:
            return result

    # ── 3. Full-phrase window scan ────────────────────────────
    result = _fuzzy_match_location_tokens(combined)
    if result:
        return result

    return None, None


def parse_custom_search(phrases: list[str]) -> dict:

    categories        = extract_categories(phrases)
    loc_type, loc_val = extract_location(phrases)
    clean_keywords    = _strip_location_tokens(phrases, loc_val)

    return {
        "categories":     categories,
        "location_type":  loc_type,
        "location_value": loc_val,
        "clean_keywords": clean_keywords,
    }


# ── Internal helpers ──────────────────────────────────────────

def _fuzzy_match_location_tokens(
    text: str,
    threshold: float = 0.78,
) -> tuple[str, str] | None:

    words = text.split()

    for n in [3, 2, 1]:
        for start in range(len(words) - n + 1):
            window_tokens = words[start:start + n]
            window        = " ".join(window_tokens)

            # Skip windows that are entirely service/stop words
            if all(_is_service_word(w) for w in window_tokens):
                continue

            # Skip very short tokens
            if len(window.strip()) < 3:
                continue

            # Fuzzy match against reference list
            best_score = 0.0
            best_ref   = None
            for ref in _KNOWN_LOCATIONS:
                score = difflib.SequenceMatcher(
                    None, window.lower(), ref.lower()
                ).ratio()
                if score > best_score:
                    best_score = score
                    best_ref   = ref

            if best_score >= threshold and best_ref is not None:
                loc_type = "state" if best_ref.lower() in _STATE_NAMES_LOWER else "city"
                # Return the original user tokens, not the corrected ref
                return loc_type, window

    return None


def _strip_location_tokens(phrases: list[str], location_value: str | None) -> list[str]:

    if not location_value:
        return phrases

    loc_pat    = re.escape(location_value)
    prep_loc   = re.compile(
        r'\s*(?:in|near|around|for|at|within|serving|covering)\s+' + loc_pat + r'\b.*$',
        re.IGNORECASE,
    )
    bare_loc   = re.compile(r'\s*,?\s*' + loc_pat + r'\b.*$', re.IGNORECASE)
    abbrev_loc = re.compile(r'\s+[A-Z]{2}\b\s*$')

    cleaned = []
    for phrase in phrases:
        p = prep_loc.sub("", phrase).strip()
        p = bare_loc.sub("", p).strip()
        p = abbrev_loc.sub("", p).strip()
        cleaned.append(p if p else phrase)

    return cleaned