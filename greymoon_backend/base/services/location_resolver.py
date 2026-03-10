# ============================================================
#  location_resolver.py
#
#  Resolves user location input (state / city / zip) into:
#    - Craigslist city codes
#    - Human-readable location string for Facebook keyword search
#
#  Robust version: handles common mismatches like
#    - Full state name passed as state code ("Florida" → FL)
#    - City name accidentally sent as state type
#    - State code sent as city type
# ============================================================

from .city_structure import US_CITY_STRUCTURE

# ── Flat lookups built once at import time ────────────────────

# craigslist_code → {code, name, state (full name)}
_CODE_TO_REGION = {
    region["code"]: {**region, "state": state_name}
    for state_name, state_info in US_CITY_STRUCTURE.items()
    for region in state_info["regions"]
}

# full state name (e.g. "Texas") → [craigslist city codes]
_STATE_NAME_TO_CODES = {
    state_name: [r["code"] for r in state_info["regions"]]
    for state_name, state_info in US_CITY_STRUCTURE.items()
}

# 2-letter abbreviation → full state name  (generated from structure)
# We derive abbreviations by matching against common US state abbrevs.
_STATE_ABBREV = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "DC": "District of Columbia", "FL": "Florida", "GA": "Georgia", "HI": "Hawaii",
    "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa",
    "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine",
    "MD": "Maryland", "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota",
    "MS": "Mississippi", "MO": "Missouri", "MT": "Montana", "NE": "Nebraska",
    "NV": "Nevada", "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico",
    "NY": "New York", "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio",
    "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island",
    "SC": "South Carolina", "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas",
    "UT": "Utah", "VT": "Vermont", "VA": "Virginia", "WA": "Washington",
    "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming",
}

# Reverse: full state name → 2-letter abbrev
_STATE_NAME_TO_ABBREV = {v: k for k, v in _STATE_ABBREV.items()}

# city name (lower) → craigslist code
_CITY_NAME_TO_CODE = {
    region["name"].lower(): region["code"]
    for state_info in US_CITY_STRUCTURE.values()
    for region in state_info["regions"]
}

# Zip-to-city mapping (expand or replace with a proper zip DB)
ZIP_PREFIX_MAP = {
    "100": "newyork", "101": "newyork", "102": "newyork",
    "900": "losangeles", "901": "losangeles", "902": "losangeles",
    "606": "chicago", "607": "chicago",
    "770": "houston", "771": "houston",
    "850": "phoenix", "851": "phoenix",
    "191": "philadelphia", "192": "philadelphia",
    "782": "sanantonio",
    "752": "dallas", "753": "dallas",
    "951": "sandiego",
    "303": "denver",
    "971": "portland",
    "981": "seattle",
    "301": "washingtondc", "302": "washingtondc",
    "941": "sfbay",
    "331": "miami", "330": "miami",
    "305": "miami",
    "404": "atlanta",
    "617": "boston",
    "713": "houston",
    "312": "chicago",
    "214": "dallas",
    "602": "phoenix",
    "210": "sanantonio",
    "619": "sandiego",
    "415": "sfbay",
    "503": "portland",
    "206": "seattle",
}


class LocationResolutionError(Exception):
    pass


def resolve_location(location_type: str, location_value: str) -> dict:
    """
    Returns:
        {
            "craigslist_cities": ["houston", "galveston"],
            "facebook_location_str": "Houston TX",
            "display": "Houston, TX"
        }
    Raises LocationResolutionError if nothing matches.

    Smart fallback logic:
      - If type=state but value looks like a city → try city resolution
      - If type=state but value is a full state name → accept it
      - If type=city but value looks like a 2-letter state code → try state
    """
    loc = location_value.strip()

    if location_type == "state":
        return _resolve_state_smart(loc)
    elif location_type == "city":
        return _resolve_city_smart(loc)
    elif location_type == "zip":
        return _resolve_by_zip(loc)
    else:
        raise LocationResolutionError(
            f"Unknown location_type '{location_type}'. Use: state, city, or zip."
        )


# ── Smart state resolver ──────────────────────────────────────

def _resolve_state_smart(value: str) -> dict:
    """
    Accepts:
      - 2-letter abbreviation  ("TX", "tx")
      - Full state name        ("Texas", "TEXAS")
      - Gracefully falls back to city if nothing matches as a state
    """
    # 0. Looks like a ZIP code? Route straight to ZIP resolver
    stripped = value.strip()
    if stripped.isdigit() and len(stripped) in (5, 9):
        print(f"[LocationResolver] '{stripped}' looks like a ZIP — resolving as ZIP")
        return _resolve_by_zip(stripped[:5])

    upper = value.upper()

    # 1. Try 2-letter abbrev
    state_name = _STATE_ABBREV.get(upper)
    if state_name:
        return _build_state_result(state_name)

    # 2. Try full state name (case-insensitive)
    title = value.title()
    if title in _STATE_NAME_TO_CODES:
        return _build_state_result(title)

    # Check lower case match
    for sname in _STATE_NAME_TO_CODES:
        if sname.lower() == value.lower():
            return _build_state_result(sname)

    # 3. Might be a city typed into the state field — fall back gracefully
    try:
        result = _resolve_by_city(value)
        print(f"[LocationResolver] '{value}' treated as city (not a state code)")
        return result
    except LocationResolutionError:
        pass

    raise LocationResolutionError(
        f"'{value}' is not a recognised US state. "
        f"Use a 2-letter state code (e.g. TX, CA) or a city name."
    )


def _build_state_result(state_name: str) -> dict:
    codes = _STATE_NAME_TO_CODES[state_name]
    abbrev = _STATE_NAME_TO_ABBREV.get(state_name, "")
    return {
        "craigslist_cities": codes,
        "facebook_location_str": f"{state_name} {abbrev}".strip(),
        "display": state_name,
    }


# ── Smart city resolver ───────────────────────────────────────

def _resolve_city_smart(value: str) -> dict:
    """
    Accepts:
      - Exact city name   ("Houston", "New York City")
      - Partial match     ("housto")
      - ZIP code typed into the city box  ("90001")
      - Gracefully handles a 2-letter state code typed in the city box
    """
    # 0. Looks like a ZIP code? Route straight to ZIP resolver
    stripped = value.strip()
    if stripped.isdigit() and len(stripped) in (5, 9):
        print(f"[LocationResolver] '{stripped}' looks like a ZIP — resolving as ZIP")
        return _resolve_by_zip(stripped[:5])

    # 1. Exact or fuzzy city match (main path)
    try:
        return _resolve_by_city(value)
    except LocationResolutionError:
        pass

    # 2. Looks like a 2-letter state code? Fall back to state
    if len(value.strip()) == 2 and value.strip().upper() in _STATE_ABBREV:
        print(f"[LocationResolver] '{value}' looks like a state code — resolving as state")
        return _build_state_result(_STATE_ABBREV[value.strip().upper()])

    # 3. Full state name in the city box
    title = value.title()
    for sname in _STATE_NAME_TO_CODES:
        if sname.lower() == value.lower():
            print(f"[LocationResolver] '{value}' is a state name — resolving as state")
            return _build_state_result(sname)

    raise LocationResolutionError(
        f"City '{value}' not found in our coverage map. "
        f"Try a different spelling or use a state code."
    )


def _resolve_by_city(city_name: str) -> dict:
    """Core city lookup — exact then fuzzy partial match."""
    code = _CITY_NAME_TO_CODE.get(city_name.lower())

    if not code:
        # Fuzzy fallback: partial match
        matches = [
            (name, c)
            for name, c in _CITY_NAME_TO_CODE.items()
            if city_name.lower() in name
        ]
        if not matches:
            raise LocationResolutionError(
                f"City '{city_name}' not found in our coverage map."
            )
        # Take best match (shortest name = most specific)
        matches.sort(key=lambda x: len(x[0]))
        _, code = matches[0]

    region = _CODE_TO_REGION[code]
    state_name = region["state"]
    abbrev = _STATE_NAME_TO_ABBREV.get(state_name, state_name)
    display = f"{region['name']}, {abbrev}"
    return {
        "craigslist_cities": [code],
        "facebook_location_str": display,
        "display": display,
    }


# ── ZIP resolver ──────────────────────────────────────────────

def _resolve_by_zip(zip_code: str) -> dict:
    if len(zip_code) < 3:
        raise LocationResolutionError("ZIP code too short.")

    prefix = zip_code[:3]
    code = ZIP_PREFIX_MAP.get(prefix)

    if not code:
        raise LocationResolutionError(
            f"ZIP code '{zip_code}' not currently mapped. Try searching by city or state."
        )

    region = _CODE_TO_REGION.get(code)
    if not region:
        raise LocationResolutionError(f"ZIP resolved to unknown CL region '{code}'.")

    state_name = region["state"]
    abbrev = _STATE_NAME_TO_ABBREV.get(state_name, state_name)
    display = f"{region['name']}, {abbrev} ({zip_code})"
    return {
        "craigslist_cities": [code],
        "facebook_location_str": f"{region['name']} {abbrev}",
        "display": display,
    }


# ── Utility ───────────────────────────────────────────────────

def get_all_city_codes() -> list[str]:
    return list(_CODE_TO_REGION.keys())