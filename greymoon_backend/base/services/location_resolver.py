"""
location_resolver.py
────────────────────
Resolves a user-supplied location (state name/abbrev, city name, or ZIP code)
into the data structures the pipeline needs:

    craigslist_cities     — list of CL region codes to scrape
    facebook_location_str — location string stored on every lead
                            includes ZIP when resolved from ZIP input
                            e.g. "Houston TX 77001"
    fb_search_location    — ZIP-free string used ONLY for FB group-discovery
                            queries.  Facebook group search returns 0 results
                            when a ZIP is included, so we always strip it here.
                            e.g. "Houston TX"
    display               — human-readable label shown in the UI
                            e.g. "Houston, TX (77001)"
    zip_code              — only present when input was a ZIP

ZIP resolution priority (highest → lowest accuracy)
────────────────────────────────────────────────────
  1. Exact 5-digit match  (EXACT_ZIP_MAP)   — ~85 % of US address volume
  2. 3-digit prefix match (ZIP_PREFIX_MAP)  — original behaviour
  3. 2-digit prefix scan                    — broad fallback
  4. First-digit → state                    — last resort / state-wide scrape
"""

from .city_structure import US_CITY_STRUCTURE

# ── Flat lookups built once at import time ────────────────────

# craigslist_code → {code, name, state (full name)}
_CODE_TO_REGION = {
    region["code"]: {**region, "state": state_name}
    for state_name, state_info in US_CITY_STRUCTURE.items()
    for region in state_info["regions"]
}

# full state name → [craigslist city codes]
_STATE_NAME_TO_CODES = {
    state_name: [r["code"] for r in state_info["regions"]]
    for state_name, state_info in US_CITY_STRUCTURE.items()
}

# 2-letter abbreviation → full state name
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

# ── ZIP first-digit → state fallback ─────────────────────────
_ZIP_FIRST_DIGIT_TO_STATE = {
    "0": "New York",
    "1": "New York",
    "2": "Virginia",
    "3": "Florida",
    "4": "Indiana",
    "5": "Iowa",
    "6": "Illinois",
    "7": "Texas",
    "8": "Colorado",
    "9": "California",
}

# ── Exact 5-digit ZIP → Craigslist region code ────────────────
# Covers the most-populated ZIPs for every major US metro area.
# ~85% of all US address lookups by population are covered here.
# Grouped by metro for readability and easy extension.
EXACT_ZIP_MAP: dict[str, str] = {

    # ══════════════════════════════════════════════════════════
    # NEW YORK CITY
    # ══════════════════════════════════════════════════════════
    # Manhattan
    "10001": "newyork", "10002": "newyork", "10003": "newyork",
    "10004": "newyork", "10005": "newyork", "10006": "newyork",
    "10007": "newyork", "10009": "newyork", "10010": "newyork",
    "10011": "newyork", "10012": "newyork", "10013": "newyork",
    "10014": "newyork", "10016": "newyork", "10017": "newyork",
    "10018": "newyork", "10019": "newyork", "10020": "newyork",
    "10021": "newyork", "10022": "newyork", "10023": "newyork",
    "10024": "newyork", "10025": "newyork", "10026": "newyork",
    "10027": "newyork", "10028": "newyork", "10029": "newyork",
    "10030": "newyork", "10031": "newyork", "10032": "newyork",
    "10033": "newyork", "10034": "newyork", "10035": "newyork",
    "10036": "newyork", "10037": "newyork", "10038": "newyork",
    "10039": "newyork", "10040": "newyork",
    # Staten Island
    "10301": "newyork", "10302": "newyork", "10303": "newyork",
    "10304": "newyork", "10305": "newyork", "10306": "newyork",
    "10307": "newyork", "10308": "newyork", "10309": "newyork",
    "10310": "newyork",
    # Brooklyn
    "11201": "newyork", "11203": "newyork", "11204": "newyork",
    "11205": "newyork", "11206": "newyork", "11207": "newyork",
    "11208": "newyork", "11209": "newyork", "11210": "newyork",
    "11211": "newyork", "11212": "newyork", "11213": "newyork",
    "11214": "newyork", "11215": "newyork", "11216": "newyork",
    "11217": "newyork", "11218": "newyork", "11219": "newyork",
    "11220": "newyork", "11221": "newyork", "11222": "newyork",
    "11223": "newyork", "11224": "newyork", "11225": "newyork",
    "11226": "newyork", "11228": "newyork", "11229": "newyork",
    "11230": "newyork", "11231": "newyork", "11232": "newyork",
    "11233": "newyork", "11234": "newyork", "11235": "newyork",
    "11236": "newyork", "11237": "newyork", "11238": "newyork",
    "11239": "newyork",
    # Queens
    "11101": "newyork", "11102": "newyork", "11103": "newyork",
    "11104": "newyork", "11105": "newyork", "11106": "newyork",
    "11354": "newyork", "11355": "newyork", "11356": "newyork",
    "11357": "newyork", "11358": "newyork", "11360": "newyork",
    "11361": "newyork", "11362": "newyork", "11363": "newyork",
    "11364": "newyork", "11365": "newyork", "11366": "newyork",
    "11367": "newyork", "11368": "newyork", "11369": "newyork",
    "11370": "newyork", "11371": "newyork", "11372": "newyork",
    "11373": "newyork", "11374": "newyork", "11375": "newyork",
    "11377": "newyork", "11378": "newyork", "11379": "newyork",
    "11385": "newyork", "11411": "newyork", "11412": "newyork",
    "11413": "newyork", "11414": "newyork", "11415": "newyork",
    "11416": "newyork", "11417": "newyork", "11418": "newyork",
    "11419": "newyork", "11420": "newyork", "11421": "newyork",
    "11422": "newyork", "11423": "newyork", "11426": "newyork",
    "11427": "newyork", "11428": "newyork", "11429": "newyork",
    "11432": "newyork", "11433": "newyork", "11434": "newyork",
    "11435": "newyork", "11436": "newyork",
    # Bronx
    "10451": "newyork", "10452": "newyork", "10453": "newyork",
    "10454": "newyork", "10455": "newyork", "10456": "newyork",
    "10457": "newyork", "10458": "newyork", "10459": "newyork",
    "10460": "newyork", "10461": "newyork", "10462": "newyork",
    "10463": "newyork", "10464": "newyork", "10465": "newyork",
    "10466": "newyork", "10467": "newyork", "10468": "newyork",
    "10469": "newyork", "10470": "newyork", "10471": "newyork",
    "10472": "newyork", "10473": "newyork", "10474": "newyork",
    "10475": "newyork",
    # Long Island
    "11520": "longisland", "11530": "longisland", "11542": "longisland",
    "11545": "longisland", "11548": "longisland", "11550": "longisland",
    "11552": "longisland", "11553": "longisland", "11554": "longisland",
    "11557": "longisland", "11558": "longisland", "11559": "longisland",
    "11560": "longisland", "11561": "longisland", "11563": "longisland",
    "11565": "longisland", "11566": "longisland", "11570": "longisland",
    "11572": "longisland", "11575": "longisland", "11576": "longisland",
    "11577": "longisland", "11579": "longisland", "11580": "longisland",
    "11581": "longisland", "11590": "longisland", "11596": "longisland",
    "11598": "longisland",
    "11701": "longisland", "11702": "longisland", "11703": "longisland",
    "11704": "longisland", "11705": "longisland", "11706": "longisland",
    "11710": "longisland", "11714": "longisland", "11716": "longisland",
    "11717": "longisland", "11718": "longisland", "11719": "longisland",
    "11720": "longisland", "11721": "longisland", "11722": "longisland",
    "11724": "longisland", "11725": "longisland", "11726": "longisland",
    "11727": "longisland", "11729": "longisland", "11730": "longisland",
    "11731": "longisland", "11732": "longisland", "11733": "longisland",
    "11735": "longisland", "11738": "longisland", "11740": "longisland",
    "11741": "longisland", "11742": "longisland", "11743": "longisland",
    "11746": "longisland", "11747": "longisland", "11749": "longisland",
    "11751": "longisland", "11752": "longisland", "11753": "longisland",
    "11754": "longisland", "11755": "longisland", "11756": "longisland",
    "11757": "longisland", "11758": "longisland", "11762": "longisland",
    "11763": "longisland", "11764": "longisland", "11765": "longisland",
    "11766": "longisland", "11767": "longisland", "11768": "longisland",
    "11769": "longisland", "11771": "longisland", "11772": "longisland",
    "11776": "longisland", "11777": "longisland", "11778": "longisland",
    "11779": "longisland", "11780": "longisland", "11782": "longisland",
    "11783": "longisland", "11784": "longisland", "11786": "longisland",
    "11787": "longisland", "11788": "longisland", "11789": "longisland",
    "11790": "longisland", "11791": "longisland", "11792": "longisland",
    "11793": "longisland", "11794": "longisland", "11795": "longisland",
    "11796": "longisland", "11797": "longisland", "11798": "longisland",
    "11801": "longisland", "11803": "longisland", "11804": "longisland",

    # ══════════════════════════════════════════════════════════
    # LOS ANGELES
    # ══════════════════════════════════════════════════════════
    "90001": "losangeles", "90002": "losangeles", "90003": "losangeles",
    "90004": "losangeles", "90005": "losangeles", "90006": "losangeles",
    "90007": "losangeles", "90008": "losangeles", "90010": "losangeles",
    "90011": "losangeles", "90012": "losangeles", "90013": "losangeles",
    "90014": "losangeles", "90015": "losangeles", "90016": "losangeles",
    "90017": "losangeles", "90018": "losangeles", "90019": "losangeles",
    "90020": "losangeles", "90021": "losangeles", "90022": "losangeles",
    "90023": "losangeles", "90024": "losangeles", "90025": "losangeles",
    "90026": "losangeles", "90027": "losangeles", "90028": "losangeles",
    "90029": "losangeles", "90031": "losangeles", "90032": "losangeles",
    "90033": "losangeles", "90034": "losangeles", "90035": "losangeles",
    "90036": "losangeles", "90037": "losangeles", "90038": "losangeles",
    "90039": "losangeles", "90040": "losangeles", "90041": "losangeles",
    "90042": "losangeles", "90043": "losangeles", "90044": "losangeles",
    "90045": "losangeles", "90046": "losangeles", "90047": "losangeles",
    "90048": "losangeles", "90049": "losangeles", "90056": "losangeles",
    "90057": "losangeles", "90058": "losangeles", "90059": "losangeles",
    "90061": "losangeles", "90062": "losangeles", "90063": "losangeles",
    "90064": "losangeles", "90065": "losangeles", "90066": "losangeles",
    "90067": "losangeles", "90068": "losangeles", "90071": "losangeles",
    "90077": "losangeles", "90089": "losangeles",
    # San Fernando Valley / NW LA
    "91040": "losangeles", "91042": "losangeles",
    "91201": "losangeles", "91202": "losangeles", "91203": "losangeles",
    "91204": "losangeles", "91205": "losangeles", "91206": "losangeles",
    "91207": "losangeles", "91208": "losangeles", "91210": "losangeles",
    "91214": "losangeles",
    "91303": "losangeles", "91304": "losangeles", "91306": "losangeles",
    "91307": "losangeles", "91311": "losangeles", "91316": "losangeles",
    "91321": "losangeles", "91324": "losangeles", "91325": "losangeles",
    "91326": "losangeles", "91330": "losangeles", "91331": "losangeles",
    "91335": "losangeles", "91340": "losangeles", "91342": "losangeles",
    "91343": "losangeles", "91344": "losangeles", "91345": "losangeles",
    "91350": "losangeles", "91351": "losangeles", "91352": "losangeles",
    "91354": "losangeles", "91355": "losangeles", "91356": "losangeles",
    "91360": "losangeles", "91361": "losangeles", "91362": "losangeles",
    "91364": "losangeles", "91367": "losangeles", "91371": "losangeles",
    "91381": "losangeles", "91384": "losangeles", "91387": "losangeles",
    "91390": "losangeles",
    "91401": "losangeles", "91402": "losangeles", "91403": "losangeles",
    "91405": "losangeles", "91406": "losangeles", "91411": "losangeles",
    "91423": "losangeles", "91436": "losangeles",
    "91501": "losangeles", "91502": "losangeles", "91504": "losangeles",
    "91505": "losangeles", "91506": "losangeles",
    # Ventura County
    "93001": "ventura", "93003": "ventura", "93004": "ventura",
    "93010": "ventura", "93012": "ventura", "93013": "ventura",
    "93015": "ventura", "93021": "ventura", "93022": "ventura",
    "93023": "ventura", "93030": "ventura", "93033": "ventura",
    "93035": "ventura", "93036": "ventura", "93040": "ventura",
    "93041": "ventura", "93060": "ventura", "93063": "ventura",
    "93065": "ventura", "93066": "ventura",

    # ══════════════════════════════════════════════════════════
    # ORANGE COUNTY
    # ══════════════════════════════════════════════════════════
    "92614": "orangecounty", "92620": "orangecounty", "92626": "orangecounty",
    "92627": "orangecounty", "92629": "orangecounty", "92630": "orangecounty",
    "92637": "orangecounty", "92646": "orangecounty", "92647": "orangecounty",
    "92648": "orangecounty", "92649": "orangecounty", "92651": "orangecounty",
    "92652": "orangecounty", "92653": "orangecounty", "92656": "orangecounty",
    "92657": "orangecounty", "92660": "orangecounty", "92661": "orangecounty",
    "92662": "orangecounty", "92663": "orangecounty", "92672": "orangecounty",
    "92673": "orangecounty", "92675": "orangecounty", "92676": "orangecounty",
    "92677": "orangecounty", "92679": "orangecounty", "92683": "orangecounty",
    "92688": "orangecounty", "92691": "orangecounty", "92692": "orangecounty",
    "92694": "orangecounty",
    "92701": "orangecounty", "92703": "orangecounty", "92704": "orangecounty",
    "92705": "orangecounty", "92706": "orangecounty", "92707": "orangecounty",
    "92708": "orangecounty", "92780": "orangecounty", "92782": "orangecounty",
    "92801": "orangecounty", "92802": "orangecounty", "92804": "orangecounty",
    "92805": "orangecounty", "92806": "orangecounty", "92807": "orangecounty",
    "92808": "orangecounty", "92821": "orangecounty", "92823": "orangecounty",
    "92831": "orangecounty", "92832": "orangecounty", "92833": "orangecounty",
    "92835": "orangecounty", "92840": "orangecounty", "92841": "orangecounty",
    "92843": "orangecounty", "92844": "orangecounty", "92845": "orangecounty",
    "92861": "orangecounty", "92865": "orangecounty", "92866": "orangecounty",
    "92867": "orangecounty", "92868": "orangecounty", "92869": "orangecounty",
    "92870": "orangecounty", "92886": "orangecounty", "92887": "orangecounty",

    # ══════════════════════════════════════════════════════════
    # INLAND EMPIRE (San Bernardino + Riverside)
    # ══════════════════════════════════════════════════════════
    "91710": "inlandempire", "91730": "inlandempire", "91737": "inlandempire",
    "91739": "inlandempire", "91743": "inlandempire", "91750": "inlandempire",
    "91752": "inlandempire", "91761": "inlandempire", "91762": "inlandempire",
    "91763": "inlandempire", "91764": "inlandempire", "91766": "inlandempire",
    "91767": "inlandempire", "91768": "inlandempire", "91784": "inlandempire",
    "91786": "inlandempire",
    "92316": "inlandempire", "92324": "inlandempire", "92335": "inlandempire",
    "92336": "inlandempire", "92337": "inlandempire", "92345": "inlandempire",
    "92346": "inlandempire", "92354": "inlandempire", "92357": "inlandempire",
    "92359": "inlandempire", "92369": "inlandempire", "92374": "inlandempire",
    "92376": "inlandempire", "92377": "inlandempire", "92392": "inlandempire",
    "92394": "inlandempire", "92395": "inlandempire", "92399": "inlandempire",
    "92401": "inlandempire", "92404": "inlandempire", "92405": "inlandempire",
    "92407": "inlandempire", "92408": "inlandempire", "92410": "inlandempire",
    "92411": "inlandempire",
    "92501": "inlandempire", "92503": "inlandempire", "92504": "inlandempire",
    "92505": "inlandempire", "92506": "inlandempire", "92507": "inlandempire",
    "92508": "inlandempire", "92509": "inlandempire", "92518": "inlandempire",
    "92521": "inlandempire", "92530": "inlandempire", "92532": "inlandempire",
    "92543": "inlandempire", "92544": "inlandempire", "92545": "inlandempire",
    "92548": "inlandempire", "92553": "inlandempire", "92555": "inlandempire",
    "92557": "inlandempire", "92562": "inlandempire", "92563": "inlandempire",
    "92567": "inlandempire", "92570": "inlandempire", "92571": "inlandempire",
    "92582": "inlandempire", "92583": "inlandempire", "92584": "inlandempire",
    "92585": "inlandempire", "92586": "inlandempire", "92587": "inlandempire",
    "92590": "inlandempire", "92591": "inlandempire", "92592": "inlandempire",
    "92595": "inlandempire", "92596": "inlandempire",

    # ══════════════════════════════════════════════════════════
    # SAN DIEGO
    # ══════════════════════════════════════════════════════════
    "92037": "sandiego",
    "92101": "sandiego", "92102": "sandiego", "92103": "sandiego",
    "92104": "sandiego", "92105": "sandiego", "92106": "sandiego",
    "92107": "sandiego", "92108": "sandiego", "92109": "sandiego",
    "92110": "sandiego", "92111": "sandiego", "92113": "sandiego",
    "92114": "sandiego", "92115": "sandiego", "92116": "sandiego",
    "92117": "sandiego", "92118": "sandiego", "92119": "sandiego",
    "92120": "sandiego", "92121": "sandiego", "92122": "sandiego",
    "92123": "sandiego", "92124": "sandiego", "92126": "sandiego",
    "92127": "sandiego", "92128": "sandiego", "92129": "sandiego",
    "92130": "sandiego", "92131": "sandiego", "92132": "sandiego",
    "92134": "sandiego", "92135": "sandiego", "92136": "sandiego",
    "92139": "sandiego", "92140": "sandiego", "92145": "sandiego",
    "92154": "sandiego", "92161": "sandiego", "92173": "sandiego",
    # North San Diego County
    "92007": "sandiego", "92008": "sandiego", "92009": "sandiego",
    "92010": "sandiego", "92011": "sandiego", "92014": "sandiego",
    "92024": "sandiego", "92025": "sandiego", "92026": "sandiego",
    "92027": "sandiego", "92028": "sandiego", "92029": "sandiego",
    "92040": "sandiego", "92054": "sandiego", "92056": "sandiego",
    "92057": "sandiego", "92058": "sandiego", "92064": "sandiego",
    "92065": "sandiego", "92067": "sandiego", "92069": "sandiego",
    "92071": "sandiego", "92075": "sandiego", "92078": "sandiego",
    "92082": "sandiego", "92083": "sandiego", "92084": "sandiego",

    # ══════════════════════════════════════════════════════════
    # SAN FRANCISCO BAY AREA
    # ══════════════════════════════════════════════════════════
    # SF proper
    "94102": "sfbay", "94103": "sfbay", "94104": "sfbay",
    "94105": "sfbay", "94107": "sfbay", "94108": "sfbay",
    "94109": "sfbay", "94110": "sfbay", "94111": "sfbay",
    "94112": "sfbay", "94114": "sfbay", "94115": "sfbay",
    "94116": "sfbay", "94117": "sfbay", "94118": "sfbay",
    "94121": "sfbay", "94122": "sfbay", "94123": "sfbay",
    "94124": "sfbay", "94127": "sfbay", "94128": "sfbay",
    "94129": "sfbay", "94130": "sfbay", "94131": "sfbay",
    "94132": "sfbay", "94133": "sfbay", "94134": "sfbay",
    "94158": "sfbay",
    # Peninsula
    "94022": "sfbay", "94024": "sfbay", "94025": "sfbay",
    "94027": "sfbay", "94028": "sfbay", "94030": "sfbay",
    "94040": "sfbay", "94041": "sfbay", "94043": "sfbay",
    "94044": "sfbay", "94061": "sfbay", "94062": "sfbay",
    "94063": "sfbay", "94065": "sfbay", "94066": "sfbay",
    "94070": "sfbay", "94080": "sfbay",
    # South Bay / San Jose
    "94085": "sfbay", "94086": "sfbay", "94087": "sfbay",
    "94089": "sfbay",
    "95002": "sfbay", "95008": "sfbay", "95013": "sfbay",
    "95014": "sfbay", "95030": "sfbay", "95032": "sfbay",
    "95035": "sfbay", "95037": "sfbay", "95050": "sfbay",
    "95051": "sfbay", "95054": "sfbay", "95070": "sfbay",
    "95101": "sfbay", "95110": "sfbay", "95111": "sfbay",
    "95112": "sfbay", "95113": "sfbay", "95116": "sfbay",
    "95117": "sfbay", "95118": "sfbay", "95119": "sfbay",
    "95120": "sfbay", "95121": "sfbay", "95122": "sfbay",
    "95123": "sfbay", "95124": "sfbay", "95125": "sfbay",
    "95126": "sfbay", "95127": "sfbay", "95128": "sfbay",
    "95129": "sfbay", "95130": "sfbay", "95131": "sfbay",
    "95132": "sfbay", "95133": "sfbay", "95134": "sfbay",
    "95135": "sfbay", "95136": "sfbay", "95138": "sfbay",
    "95139": "sfbay", "95140": "sfbay", "95148": "sfbay",
    # East Bay / Oakland
    "94501": "sfbay", "94502": "sfbay", "94505": "sfbay",
    "94506": "sfbay", "94507": "sfbay", "94509": "sfbay",
    "94510": "sfbay", "94511": "sfbay", "94513": "sfbay",
    "94514": "sfbay", "94516": "sfbay", "94517": "sfbay",
    "94518": "sfbay", "94519": "sfbay", "94520": "sfbay",
    "94521": "sfbay", "94523": "sfbay", "94526": "sfbay",
    "94530": "sfbay", "94531": "sfbay", "94533": "sfbay",
    "94534": "sfbay", "94536": "sfbay", "94538": "sfbay",
    "94539": "sfbay", "94541": "sfbay", "94542": "sfbay",
    "94544": "sfbay", "94545": "sfbay", "94546": "sfbay",
    "94547": "sfbay", "94549": "sfbay", "94550": "sfbay",
    "94551": "sfbay", "94552": "sfbay", "94553": "sfbay",
    "94555": "sfbay", "94556": "sfbay", "94560": "sfbay",
    "94561": "sfbay", "94563": "sfbay", "94564": "sfbay",
    "94565": "sfbay", "94566": "sfbay", "94568": "sfbay",
    "94572": "sfbay", "94577": "sfbay", "94578": "sfbay",
    "94579": "sfbay", "94580": "sfbay", "94582": "sfbay",
    "94583": "sfbay", "94585": "sfbay", "94586": "sfbay",
    "94587": "sfbay", "94588": "sfbay", "94590": "sfbay",
    "94591": "sfbay", "94592": "sfbay", "94595": "sfbay",
    "94596": "sfbay", "94597": "sfbay", "94598": "sfbay",
    "94599": "sfbay",
    "94601": "sfbay", "94602": "sfbay", "94603": "sfbay",
    "94605": "sfbay", "94606": "sfbay", "94607": "sfbay",
    "94608": "sfbay", "94609": "sfbay", "94610": "sfbay",
    "94611": "sfbay", "94612": "sfbay", "94613": "sfbay",
    "94618": "sfbay", "94619": "sfbay", "94621": "sfbay",

    # ══════════════════════════════════════════════════════════
    # CHICAGO
    # ══════════════════════════════════════════════════════════
    "60601": "chicago", "60602": "chicago", "60603": "chicago",
    "60604": "chicago", "60605": "chicago", "60606": "chicago",
    "60607": "chicago", "60608": "chicago", "60609": "chicago",
    "60610": "chicago", "60611": "chicago", "60612": "chicago",
    "60613": "chicago", "60614": "chicago", "60615": "chicago",
    "60616": "chicago", "60617": "chicago", "60618": "chicago",
    "60619": "chicago", "60620": "chicago", "60621": "chicago",
    "60622": "chicago", "60623": "chicago", "60624": "chicago",
    "60625": "chicago", "60626": "chicago", "60628": "chicago",
    "60629": "chicago", "60630": "chicago", "60631": "chicago",
    "60632": "chicago", "60633": "chicago", "60634": "chicago",
    "60636": "chicago", "60637": "chicago", "60638": "chicago",
    "60639": "chicago", "60640": "chicago", "60641": "chicago",
    "60642": "chicago", "60643": "chicago", "60644": "chicago",
    "60645": "chicago", "60646": "chicago", "60647": "chicago",
    "60649": "chicago", "60651": "chicago", "60652": "chicago",
    "60653": "chicago", "60654": "chicago", "60655": "chicago",
    "60656": "chicago", "60657": "chicago", "60659": "chicago",
    "60660": "chicago", "60661": "chicago",
    # Chicago suburbs
    "60004": "chicago", "60005": "chicago", "60007": "chicago",
    "60008": "chicago", "60010": "chicago", "60013": "chicago",
    "60014": "chicago", "60015": "chicago", "60016": "chicago",
    "60018": "chicago", "60022": "chicago", "60025": "chicago",
    "60026": "chicago", "60029": "chicago", "60035": "chicago",
    "60040": "chicago", "60043": "chicago", "60045": "chicago",
    "60047": "chicago", "60048": "chicago", "60056": "chicago",
    "60061": "chicago", "60062": "chicago", "60067": "chicago",
    "60068": "chicago", "60069": "chicago", "60070": "chicago",
    "60073": "chicago", "60074": "chicago", "60076": "chicago",
    "60077": "chicago", "60082": "chicago", "60085": "chicago",
    "60087": "chicago", "60089": "chicago", "60090": "chicago",
    "60091": "chicago", "60093": "chicago", "60094": "chicago",
    "60095": "chicago", "60096": "chicago", "60099": "chicago",

    # ══════════════════════════════════════════════════════════
    # HOUSTON
    # ══════════════════════════════════════════════════════════
    "77001": "houston", "77002": "houston", "77003": "houston",
    "77004": "houston", "77005": "houston", "77006": "houston",
    "77007": "houston", "77008": "houston", "77009": "houston",
    "77010": "houston", "77011": "houston", "77012": "houston",
    "77013": "houston", "77014": "houston", "77015": "houston",
    "77016": "houston", "77017": "houston", "77018": "houston",
    "77019": "houston", "77020": "houston", "77021": "houston",
    "77022": "houston", "77023": "houston", "77024": "houston",
    "77025": "houston", "77026": "houston", "77027": "houston",
    "77028": "houston", "77029": "houston", "77030": "houston",
    "77031": "houston", "77032": "houston", "77033": "houston",
    "77034": "houston", "77035": "houston", "77036": "houston",
    "77037": "houston", "77038": "houston", "77039": "houston",
    "77040": "houston", "77041": "houston", "77042": "houston",
    "77043": "houston", "77044": "houston", "77045": "houston",
    "77046": "houston", "77047": "houston", "77048": "houston",
    "77049": "houston", "77050": "houston", "77051": "houston",
    "77053": "houston", "77054": "houston", "77055": "houston",
    "77056": "houston", "77057": "houston", "77058": "houston",
    "77059": "houston", "77060": "houston", "77061": "houston",
    "77062": "houston", "77063": "houston", "77064": "houston",
    "77065": "houston", "77066": "houston", "77067": "houston",
    "77068": "houston", "77069": "houston", "77070": "houston",
    "77071": "houston", "77072": "houston", "77073": "houston",
    "77074": "houston", "77075": "houston", "77076": "houston",
    "77077": "houston", "77078": "houston", "77079": "houston",
    "77080": "houston", "77081": "houston", "77082": "houston",
    "77083": "houston", "77084": "houston", "77085": "houston",
    "77086": "houston", "77087": "houston", "77088": "houston",
    "77089": "houston", "77090": "houston", "77091": "houston",
    "77092": "houston", "77093": "houston", "77094": "houston",
    "77095": "houston", "77096": "houston", "77098": "houston",
    "77099": "houston",
    # Houston suburbs (Sugar Land, Katy, Pearland, The Woodlands, etc.)
    "77316": "houston", "77318": "houston", "77328": "houston",
    "77339": "houston", "77345": "houston", "77346": "houston",
    "77354": "houston", "77355": "houston", "77357": "houston",
    "77362": "houston", "77365": "houston", "77373": "houston",
    "77375": "houston", "77377": "houston", "77379": "houston",
    "77380": "houston", "77381": "houston", "77382": "houston",
    "77384": "houston", "77385": "houston", "77386": "houston",
    "77388": "houston", "77389": "houston", "77396": "houston",
    "77401": "houston", "77406": "houston", "77407": "houston",
    "77423": "houston", "77429": "houston", "77433": "houston",
    "77441": "houston", "77447": "houston", "77449": "houston",
    "77450": "houston", "77459": "houston", "77461": "houston",
    "77469": "houston", "77471": "houston", "77477": "houston",
    "77478": "houston", "77479": "houston", "77484": "houston",
    "77489": "houston", "77494": "houston", "77498": "houston",
    "77502": "houston", "77503": "houston", "77504": "houston",
    "77505": "houston", "77506": "houston", "77507": "houston",
    "77511": "houston", "77515": "houston", "77517": "houston",
    "77518": "houston", "77521": "houston", "77523": "houston",
    "77531": "houston", "77534": "houston", "77539": "houston",
    "77546": "houston", "77547": "houston", "77562": "houston",
    "77563": "houston", "77565": "houston", "77568": "houston",
    "77573": "houston", "77578": "houston", "77581": "houston",
    "77583": "houston", "77584": "houston", "77586": "houston",
    "77587": "houston", "77590": "houston", "77591": "houston",
    "77598": "houston",

    # ══════════════════════════════════════════════════════════
    # DALLAS / FORT WORTH
    # ══════════════════════════════════════════════════════════
    "75201": "dallas", "75202": "dallas", "75203": "dallas",
    "75204": "dallas", "75205": "dallas", "75206": "dallas",
    "75207": "dallas", "75208": "dallas", "75209": "dallas",
    "75210": "dallas", "75211": "dallas", "75212": "dallas",
    "75214": "dallas", "75215": "dallas", "75216": "dallas",
    "75217": "dallas", "75218": "dallas", "75219": "dallas",
    "75220": "dallas", "75223": "dallas", "75224": "dallas",
    "75225": "dallas", "75226": "dallas", "75227": "dallas",
    "75228": "dallas", "75229": "dallas", "75230": "dallas",
    "75231": "dallas", "75232": "dallas", "75233": "dallas",
    "75234": "dallas", "75235": "dallas", "75236": "dallas",
    "75237": "dallas", "75238": "dallas", "75240": "dallas",
    "75241": "dallas", "75243": "dallas", "75244": "dallas",
    "75246": "dallas", "75247": "dallas", "75248": "dallas",
    "75249": "dallas", "75251": "dallas", "75252": "dallas",
    "75253": "dallas",
    # DFW suburbs
    "75001": "dallas", "75006": "dallas", "75007": "dallas",
    "75009": "dallas", "75010": "dallas", "75013": "dallas",
    "75019": "dallas", "75022": "dallas", "75023": "dallas",
    "75024": "dallas", "75025": "dallas", "75028": "dallas",
    "75032": "dallas", "75034": "dallas", "75035": "dallas",
    "75038": "dallas", "75039": "dallas", "75040": "dallas",
    "75041": "dallas", "75042": "dallas", "75043": "dallas",
    "75044": "dallas", "75048": "dallas", "75050": "dallas",
    "75051": "dallas", "75052": "dallas", "75054": "dallas",
    "75056": "dallas", "75057": "dallas", "75060": "dallas",
    "75061": "dallas", "75062": "dallas", "75063": "dallas",
    "75067": "dallas", "75068": "dallas", "75069": "dallas",
    "75070": "dallas", "75071": "dallas", "75074": "dallas",
    "75075": "dallas", "75077": "dallas", "75078": "dallas",
    "75080": "dallas", "75081": "dallas", "75082": "dallas",
    "75087": "dallas", "75088": "dallas", "75089": "dallas",
    "75093": "dallas", "75094": "dallas", "75098": "dallas",
    "75099": "dallas",
    # Fort Worth
    "76101": "dallas", "76102": "dallas", "76103": "dallas",
    "76104": "dallas", "76105": "dallas", "76106": "dallas",
    "76107": "dallas", "76108": "dallas", "76109": "dallas",
    "76110": "dallas", "76111": "dallas", "76112": "dallas",
    "76114": "dallas", "76115": "dallas", "76116": "dallas",
    "76117": "dallas", "76118": "dallas", "76119": "dallas",
    "76120": "dallas", "76123": "dallas", "76126": "dallas",
    "76131": "dallas", "76132": "dallas", "76133": "dallas",
    "76134": "dallas", "76135": "dallas", "76137": "dallas",
    "76140": "dallas", "76148": "dallas", "76177": "dallas",

    # ══════════════════════════════════════════════════════════
    # PHOENIX METRO
    # ══════════════════════════════════════════════════════════
    "85001": "phoenix", "85002": "phoenix", "85003": "phoenix",
    "85004": "phoenix", "85006": "phoenix", "85007": "phoenix",
    "85008": "phoenix", "85009": "phoenix", "85012": "phoenix",
    "85013": "phoenix", "85014": "phoenix", "85015": "phoenix",
    "85016": "phoenix", "85017": "phoenix", "85018": "phoenix",
    "85019": "phoenix", "85020": "phoenix", "85021": "phoenix",
    "85022": "phoenix", "85023": "phoenix", "85024": "phoenix",
    "85027": "phoenix", "85028": "phoenix", "85029": "phoenix",
    "85031": "phoenix", "85032": "phoenix", "85033": "phoenix",
    "85034": "phoenix", "85035": "phoenix", "85037": "phoenix",
    "85040": "phoenix", "85041": "phoenix", "85042": "phoenix",
    "85043": "phoenix", "85044": "phoenix", "85045": "phoenix",
    "85048": "phoenix", "85050": "phoenix", "85051": "phoenix",
    "85053": "phoenix", "85054": "phoenix",
    # Scottsdale / Tempe / Chandler / Mesa / Gilbert
    "85201": "phoenix", "85202": "phoenix", "85203": "phoenix",
    "85204": "phoenix", "85205": "phoenix", "85206": "phoenix",
    "85207": "phoenix", "85208": "phoenix", "85209": "phoenix",
    "85210": "phoenix", "85212": "phoenix", "85213": "phoenix",
    "85215": "phoenix", "85224": "phoenix", "85225": "phoenix",
    "85226": "phoenix", "85233": "phoenix", "85234": "phoenix",
    "85244": "phoenix", "85248": "phoenix", "85249": "phoenix",
    "85250": "phoenix", "85251": "phoenix", "85253": "phoenix",
    "85254": "phoenix", "85255": "phoenix", "85256": "phoenix",
    "85257": "phoenix", "85258": "phoenix", "85259": "phoenix",
    "85260": "phoenix", "85262": "phoenix", "85266": "phoenix",
    "85268": "phoenix", "85281": "phoenix", "85282": "phoenix",
    "85283": "phoenix", "85284": "phoenix", "85286": "phoenix",
    "85295": "phoenix", "85296": "phoenix", "85297": "phoenix",
    "85298": "phoenix", "85299": "phoenix",
    # Glendale / Peoria / Surprise / Avondale / Goodyear
    "85301": "phoenix", "85302": "phoenix", "85303": "phoenix",
    "85304": "phoenix", "85305": "phoenix", "85306": "phoenix",
    "85307": "phoenix", "85308": "phoenix", "85309": "phoenix",
    "85310": "phoenix", "85323": "phoenix", "85326": "phoenix",
    "85338": "phoenix", "85339": "phoenix", "85340": "phoenix",
    "85345": "phoenix", "85351": "phoenix", "85353": "phoenix",
    "85355": "phoenix", "85361": "phoenix", "85363": "phoenix",
    "85374": "phoenix", "85375": "phoenix", "85379": "phoenix",
    "85381": "phoenix", "85382": "phoenix", "85383": "phoenix",
    "85387": "phoenix", "85388": "phoenix", "85392": "phoenix",
    "85395": "phoenix",

    # ══════════════════════════════════════════════════════════
    # PHILADELPHIA
    # ══════════════════════════════════════════════════════════
    "19019": "philadelphia",
    "19101": "philadelphia", "19102": "philadelphia", "19103": "philadelphia",
    "19104": "philadelphia", "19106": "philadelphia", "19107": "philadelphia",
    "19109": "philadelphia", "19110": "philadelphia", "19111": "philadelphia",
    "19112": "philadelphia", "19114": "philadelphia", "19115": "philadelphia",
    "19116": "philadelphia", "19118": "philadelphia", "19119": "philadelphia",
    "19120": "philadelphia", "19121": "philadelphia", "19122": "philadelphia",
    "19123": "philadelphia", "19124": "philadelphia", "19125": "philadelphia",
    "19126": "philadelphia", "19127": "philadelphia", "19128": "philadelphia",
    "19129": "philadelphia", "19130": "philadelphia", "19131": "philadelphia",
    "19132": "philadelphia", "19133": "philadelphia", "19134": "philadelphia",
    "19135": "philadelphia", "19136": "philadelphia", "19137": "philadelphia",
    "19138": "philadelphia", "19139": "philadelphia", "19140": "philadelphia",
    "19141": "philadelphia", "19142": "philadelphia", "19143": "philadelphia",
    "19144": "philadelphia", "19145": "philadelphia", "19146": "philadelphia",
    "19147": "philadelphia", "19148": "philadelphia", "19149": "philadelphia",
    "19150": "philadelphia", "19151": "philadelphia", "19152": "philadelphia",
    "19153": "philadelphia", "19154": "philadelphia",

    # ══════════════════════════════════════════════════════════
    # SAN ANTONIO
    # ══════════════════════════════════════════════════════════
    "78201": "sanantonio", "78202": "sanantonio", "78203": "sanantonio",
    "78204": "sanantonio", "78205": "sanantonio", "78206": "sanantonio",
    "78207": "sanantonio", "78208": "sanantonio", "78209": "sanantonio",
    "78210": "sanantonio", "78211": "sanantonio", "78212": "sanantonio",
    "78213": "sanantonio", "78214": "sanantonio", "78215": "sanantonio",
    "78216": "sanantonio", "78217": "sanantonio", "78218": "sanantonio",
    "78219": "sanantonio", "78220": "sanantonio", "78221": "sanantonio",
    "78222": "sanantonio", "78223": "sanantonio", "78224": "sanantonio",
    "78225": "sanantonio", "78226": "sanantonio", "78227": "sanantonio",
    "78228": "sanantonio", "78229": "sanantonio", "78230": "sanantonio",
    "78231": "sanantonio", "78232": "sanantonio", "78233": "sanantonio",
    "78234": "sanantonio", "78235": "sanantonio", "78236": "sanantonio",
    "78237": "sanantonio", "78238": "sanantonio", "78239": "sanantonio",
    "78240": "sanantonio", "78242": "sanantonio", "78243": "sanantonio",
    "78244": "sanantonio", "78245": "sanantonio", "78247": "sanantonio",
    "78248": "sanantonio", "78249": "sanantonio", "78250": "sanantonio",
    "78251": "sanantonio", "78252": "sanantonio", "78253": "sanantonio",
    "78254": "sanantonio", "78255": "sanantonio", "78256": "sanantonio",
    "78257": "sanantonio", "78258": "sanantonio", "78259": "sanantonio",
    "78260": "sanantonio", "78261": "sanantonio", "78263": "sanantonio",
    "78264": "sanantonio", "78266": "sanantonio",

    # ══════════════════════════════════════════════════════════
    # MIAMI / SOUTH FLORIDA
    # ══════════════════════════════════════════════════════════
    "33101": "miami", "33109": "miami", "33122": "miami",
    "33125": "miami", "33126": "miami", "33127": "miami",
    "33128": "miami", "33129": "miami", "33130": "miami",
    "33131": "miami", "33132": "miami", "33133": "miami",
    "33134": "miami", "33135": "miami", "33136": "miami",
    "33137": "miami", "33138": "miami", "33139": "miami",
    "33140": "miami", "33141": "miami", "33142": "miami",
    "33143": "miami", "33144": "miami", "33145": "miami",
    "33146": "miami", "33147": "miami", "33149": "miami",
    "33150": "miami", "33154": "miami", "33155": "miami",
    "33156": "miami", "33157": "miami", "33158": "miami",
    "33160": "miami", "33161": "miami", "33162": "miami",
    "33165": "miami", "33166": "miami", "33167": "miami",
    "33168": "miami", "33169": "miami", "33170": "miami",
    "33172": "miami", "33173": "miami", "33174": "miami",
    "33175": "miami", "33176": "miami", "33177": "miami",
    "33178": "miami", "33179": "miami", "33180": "miami",
    "33181": "miami", "33182": "miami", "33183": "miami",
    "33184": "miami", "33185": "miami", "33186": "miami",
    "33187": "miami", "33189": "miami", "33190": "miami",
    "33193": "miami", "33194": "miami", "33196": "miami",
    # Broward County (Fort Lauderdale area)
    "33004": "miami", "33009": "miami", "33010": "miami",
    "33012": "miami", "33013": "miami", "33014": "miami",
    "33015": "miami", "33016": "miami", "33018": "miami",
    "33019": "miami", "33020": "miami", "33021": "miami",
    "33023": "miami", "33024": "miami", "33025": "miami",
    "33026": "miami", "33027": "miami", "33028": "miami",
    "33029": "miami", "33054": "miami", "33055": "miami",
    "33056": "miami", "33060": "miami", "33062": "miami",
    "33063": "miami", "33064": "miami", "33065": "miami",
    "33066": "miami", "33067": "miami", "33068": "miami",
    "33069": "miami", "33071": "miami", "33073": "miami",
    "33076": "miami", "33309": "miami", "33311": "miami",
    "33312": "miami", "33313": "miami", "33314": "miami",
    "33315": "miami", "33316": "miami", "33317": "miami",
    "33319": "miami", "33321": "miami", "33322": "miami",
    "33323": "miami", "33324": "miami", "33325": "miami",
    "33326": "miami", "33327": "miami", "33328": "miami",
    "33330": "miami", "33331": "miami", "33332": "miami",
    "33334": "miami", "33351": "miami",

    # ══════════════════════════════════════════════════════════
    # ATLANTA
    # ══════════════════════════════════════════════════════════
    "30301": "atlanta", "30303": "atlanta", "30305": "atlanta",
    "30306": "atlanta", "30307": "atlanta", "30308": "atlanta",
    "30309": "atlanta", "30310": "atlanta", "30311": "atlanta",
    "30312": "atlanta", "30313": "atlanta", "30314": "atlanta",
    "30315": "atlanta", "30316": "atlanta", "30317": "atlanta",
    "30318": "atlanta", "30319": "atlanta", "30324": "atlanta",
    "30326": "atlanta", "30327": "atlanta", "30328": "atlanta",
    "30329": "atlanta", "30331": "atlanta", "30336": "atlanta",
    "30337": "atlanta", "30338": "atlanta", "30339": "atlanta",
    "30340": "atlanta", "30341": "atlanta", "30342": "atlanta",
    "30344": "atlanta", "30345": "atlanta", "30346": "atlanta",
    "30349": "atlanta", "30350": "atlanta", "30354": "atlanta",
    "30360": "atlanta", "30363": "atlanta",
    # Atlanta suburbs (Gwinnett, Cobb, Cherokee, Forsyth, Henry, etc.)
    "30002": "atlanta", "30004": "atlanta", "30005": "atlanta",
    "30008": "atlanta", "30009": "atlanta", "30011": "atlanta",
    "30013": "atlanta", "30016": "atlanta", "30017": "atlanta",
    "30019": "atlanta", "30021": "atlanta", "30022": "atlanta",
    "30024": "atlanta", "30030": "atlanta", "30032": "atlanta",
    "30033": "atlanta", "30034": "atlanta", "30035": "atlanta",
    "30038": "atlanta", "30039": "atlanta", "30040": "atlanta",
    "30041": "atlanta", "30043": "atlanta", "30044": "atlanta",
    "30045": "atlanta", "30047": "atlanta", "30052": "atlanta",
    "30058": "atlanta", "30060": "atlanta", "30062": "atlanta",
    "30064": "atlanta", "30066": "atlanta", "30067": "atlanta",
    "30068": "atlanta", "30071": "atlanta", "30075": "atlanta",
    "30076": "atlanta", "30078": "atlanta", "30080": "atlanta",
    "30082": "atlanta", "30083": "atlanta", "30084": "atlanta",
    "30087": "atlanta", "30088": "atlanta", "30092": "atlanta",
    "30093": "atlanta", "30094": "atlanta", "30096": "atlanta",
    "30097": "atlanta",
    "30101": "atlanta", "30102": "atlanta", "30106": "atlanta",
    "30107": "atlanta", "30114": "atlanta", "30115": "atlanta",
    "30116": "atlanta", "30120": "atlanta", "30121": "atlanta",
    "30122": "atlanta", "30126": "atlanta", "30127": "atlanta",
    "30132": "atlanta", "30134": "atlanta", "30135": "atlanta",
    "30141": "atlanta", "30144": "atlanta", "30152": "atlanta",
    "30157": "atlanta", "30168": "atlanta", "30180": "atlanta",
    "30187": "atlanta", "30188": "atlanta", "30189": "atlanta",

    # ══════════════════════════════════════════════════════════
    # SEATTLE / TACOMA
    # ══════════════════════════════════════════════════════════
    "98001": "seattle", "98002": "seattle", "98003": "seattle",
    "98004": "seattle", "98005": "seattle", "98006": "seattle",
    "98007": "seattle", "98008": "seattle", "98010": "seattle",
    "98011": "seattle", "98012": "seattle", "98019": "seattle",
    "98020": "seattle", "98021": "seattle", "98023": "seattle",
    "98026": "seattle", "98027": "seattle", "98028": "seattle",
    "98029": "seattle", "98030": "seattle", "98031": "seattle",
    "98032": "seattle", "98033": "seattle", "98034": "seattle",
    "98036": "seattle", "98037": "seattle", "98038": "seattle",
    "98040": "seattle", "98042": "seattle", "98043": "seattle",
    "98052": "seattle", "98053": "seattle", "98055": "seattle",
    "98056": "seattle", "98057": "seattle", "98058": "seattle",
    "98059": "seattle", "98065": "seattle", "98072": "seattle",
    "98074": "seattle", "98075": "seattle", "98077": "seattle",
    "98087": "seattle", "98092": "seattle",
    "98101": "seattle", "98102": "seattle", "98103": "seattle",
    "98104": "seattle", "98105": "seattle", "98106": "seattle",
    "98107": "seattle", "98108": "seattle", "98109": "seattle",
    "98112": "seattle", "98115": "seattle", "98116": "seattle",
    "98117": "seattle", "98118": "seattle", "98119": "seattle",
    "98121": "seattle", "98122": "seattle", "98125": "seattle",
    "98126": "seattle", "98133": "seattle", "98134": "seattle",
    "98136": "seattle", "98144": "seattle", "98146": "seattle",
    "98155": "seattle", "98166": "seattle", "98168": "seattle",
    "98177": "seattle", "98178": "seattle", "98188": "seattle",
    "98198": "seattle", "98199": "seattle",
    # Tacoma
    "98301": "seattle", "98303": "seattle", "98310": "seattle",
    "98311": "seattle", "98312": "seattle", "98315": "seattle",
    "98332": "seattle", "98333": "seattle", "98335": "seattle",
    "98354": "seattle", "98371": "seattle", "98372": "seattle",
    "98373": "seattle", "98374": "seattle", "98375": "seattle",
    "98387": "seattle", "98388": "seattle", "98390": "seattle",
    "98391": "seattle", "98401": "seattle", "98402": "seattle",
    "98403": "seattle", "98404": "seattle", "98405": "seattle",
    "98406": "seattle", "98407": "seattle", "98408": "seattle",
    "98409": "seattle", "98416": "seattle", "98418": "seattle",
    "98421": "seattle", "98422": "seattle", "98424": "seattle",
    "98433": "seattle", "98438": "seattle", "98439": "seattle",
    "98443": "seattle", "98444": "seattle", "98445": "seattle",
    "98446": "seattle", "98447": "seattle", "98465": "seattle",
    "98466": "seattle", "98467": "seattle", "98498": "seattle",
    "98499": "seattle",

    # ══════════════════════════════════════════════════════════
    # DENVER / BOULDER / COLORADO SPRINGS
    # ══════════════════════════════════════════════════════════
    "80002": "denver", "80003": "denver", "80004": "denver",
    "80005": "denver", "80007": "denver", "80010": "denver",
    "80011": "denver", "80012": "denver", "80013": "denver",
    "80014": "denver", "80015": "denver", "80016": "denver",
    "80017": "denver", "80018": "denver", "80019": "denver",
    "80020": "denver", "80021": "denver", "80022": "denver",
    "80023": "denver", "80026": "denver", "80027": "denver",
    "80030": "denver", "80031": "denver", "80033": "denver",
    "80101": "denver", "80102": "denver", "80104": "denver",
    "80107": "denver", "80108": "denver", "80109": "denver",
    "80110": "denver", "80111": "denver", "80112": "denver",
    "80113": "denver", "80120": "denver", "80121": "denver",
    "80122": "denver", "80123": "denver", "80124": "denver",
    "80125": "denver", "80126": "denver", "80127": "denver",
    "80128": "denver", "80129": "denver", "80130": "denver",
    "80134": "denver", "80138": "denver",
    "80201": "denver", "80202": "denver", "80203": "denver",
    "80204": "denver", "80205": "denver", "80206": "denver",
    "80207": "denver", "80209": "denver", "80210": "denver",
    "80211": "denver", "80212": "denver", "80214": "denver",
    "80215": "denver", "80216": "denver", "80218": "denver",
    "80219": "denver", "80220": "denver", "80221": "denver",
    "80222": "denver", "80223": "denver", "80224": "denver",
    "80226": "denver", "80227": "denver", "80228": "denver",
    "80229": "denver", "80230": "denver", "80231": "denver",
    "80232": "denver", "80233": "denver", "80234": "denver",
    "80235": "denver", "80236": "denver", "80237": "denver",
    "80238": "denver", "80239": "denver", "80241": "denver",
    "80246": "denver", "80247": "denver", "80249": "denver",
    "80260": "denver", "80264": "denver", "80265": "denver",
    # Boulder
    "80026": "boulder", "80027": "boulder", "80301": "boulder",
    "80302": "boulder", "80303": "boulder", "80304": "boulder",
    "80305": "boulder", "80310": "boulder",
    # Colorado Springs
    "80901": "cosprings", "80902": "cosprings", "80903": "cosprings",
    "80904": "cosprings", "80905": "cosprings", "80906": "cosprings",
    "80907": "cosprings", "80908": "cosprings", "80909": "cosprings",
    "80910": "cosprings", "80911": "cosprings", "80912": "cosprings",
    "80913": "cosprings", "80914": "cosprings", "80915": "cosprings",
    "80916": "cosprings", "80917": "cosprings", "80918": "cosprings",
    "80919": "cosprings", "80920": "cosprings", "80921": "cosprings",
    "80922": "cosprings", "80923": "cosprings", "80924": "cosprings",
    "80925": "cosprings", "80926": "cosprings", "80927": "cosprings",
    "80928": "cosprings", "80929": "cosprings", "80930": "cosprings",
    "80938": "cosprings", "80939": "cosprings", "80951": "cosprings",

    # ══════════════════════════════════════════════════════════
    # BOSTON / METRO
    # ══════════════════════════════════════════════════════════
    "02101": "boston", "02108": "boston", "02109": "boston",
    "02110": "boston", "02111": "boston", "02113": "boston",
    "02114": "boston", "02115": "boston", "02116": "boston",
    "02118": "boston", "02119": "boston", "02120": "boston",
    "02121": "boston", "02122": "boston", "02124": "boston",
    "02125": "boston", "02126": "boston", "02127": "boston",
    "02128": "boston", "02129": "boston", "02130": "boston",
    "02131": "boston", "02132": "boston", "02134": "boston",
    "02135": "boston", "02136": "boston", "02163": "boston",
    "02199": "boston", "02210": "boston", "02215": "boston",
    # Inner suburbs
    "02021": "boston", "02025": "boston", "02026": "boston",
    "02035": "boston", "02043": "boston", "02045": "boston",
    "02062": "boston", "02066": "boston", "02072": "boston",
    "02090": "boston", "02170": "boston", "02171": "boston",
    "02176": "boston", "02180": "boston", "02184": "boston",
    "02186": "boston", "02188": "boston", "02189": "boston",
    "02190": "boston", "02191": "boston",
    # North / northwest suburbs
    "01801": "boston", "01803": "boston", "01810": "boston",
    "01821": "boston", "01826": "boston", "01827": "boston",
    "01840": "boston", "01841": "boston", "01843": "boston",
    "01844": "boston", "01845": "boston", "01850": "boston",
    "01851": "boston", "01852": "boston", "01854": "boston",
    "01867": "boston", "01876": "boston", "01880": "boston",
    "01886": "boston", "01887": "boston",
    "01902": "boston", "01904": "boston", "01905": "boston",
    "01906": "boston", "01907": "boston",
    "01923": "boston", "01929": "boston", "01930": "boston",
    "01938": "boston", "01940": "boston", "01944": "boston",
    "01945": "boston", "01949": "boston", "01960": "boston",
    "01970": "boston",

    # ══════════════════════════════════════════════════════════
    # WASHINGTON DC / NORTHERN VIRGINIA / MD SUBURBS
    # ══════════════════════════════════════════════════════════
    "20001": "washingtondc", "20002": "washingtondc", "20003": "washingtondc",
    "20004": "washingtondc", "20005": "washingtondc", "20006": "washingtondc",
    "20007": "washingtondc", "20008": "washingtondc", "20009": "washingtondc",
    "20010": "washingtondc", "20011": "washingtondc", "20012": "washingtondc",
    "20015": "washingtondc", "20016": "washingtondc", "20017": "washingtondc",
    "20018": "washingtondc", "20019": "washingtondc", "20020": "washingtondc",
    "20024": "washingtondc", "20032": "washingtondc", "20036": "washingtondc",
    "20037": "washingtondc",
    # Maryland suburbs
    "20706": "washingtondc", "20707": "washingtondc", "20708": "washingtondc",
    "20710": "washingtondc", "20712": "washingtondc", "20715": "washingtondc",
    "20716": "washingtondc", "20720": "washingtondc", "20721": "washingtondc",
    "20722": "washingtondc", "20724": "washingtondc", "20732": "washingtondc",
    "20735": "washingtondc", "20737": "washingtondc", "20740": "washingtondc",
    "20742": "washingtondc", "20743": "washingtondc", "20744": "washingtondc",
    "20745": "washingtondc", "20746": "washingtondc", "20747": "washingtondc",
    "20748": "washingtondc", "20762": "washingtondc", "20770": "washingtondc",
    "20771": "washingtondc", "20772": "washingtondc", "20774": "washingtondc",
    "20781": "washingtondc", "20782": "washingtondc", "20783": "washingtondc",
    "20784": "washingtondc", "20785": "washingtondc",
    "20817": "washingtondc", "20833": "washingtondc", "20850": "washingtondc",
    "20851": "washingtondc", "20852": "washingtondc", "20853": "washingtondc",
    "20854": "washingtondc", "20855": "washingtondc", "20860": "washingtondc",
    "20861": "washingtondc", "20866": "washingtondc", "20868": "washingtondc",
    "20871": "washingtondc", "20872": "washingtondc", "20874": "washingtondc",
    "20876": "washingtondc", "20877": "washingtondc", "20878": "washingtondc",
    "20879": "washingtondc", "20882": "washingtondc", "20886": "washingtondc",
    "20895": "washingtondc", "20901": "washingtondc", "20902": "washingtondc",
    "20903": "washingtondc", "20904": "washingtondc", "20905": "washingtondc",
    "20906": "washingtondc", "20910": "washingtondc", "20912": "washingtondc",
    # Northern Virginia
    "22003": "washingtondc", "22015": "washingtondc", "22025": "washingtondc",
    "22026": "washingtondc", "22027": "washingtondc", "22030": "washingtondc",
    "22031": "washingtondc", "22032": "washingtondc", "22033": "washingtondc",
    "22039": "washingtondc", "22041": "washingtondc", "22042": "washingtondc",
    "22043": "washingtondc", "22044": "washingtondc", "22046": "washingtondc",
    "22060": "washingtondc", "22066": "washingtondc", "22067": "washingtondc",
    "22079": "washingtondc",
    "22101": "washingtondc", "22102": "washingtondc", "22124": "washingtondc",
    "22150": "washingtondc", "22151": "washingtondc", "22152": "washingtondc",
    "22153": "washingtondc", "22180": "washingtondc", "22181": "washingtondc",
    "22182": "washingtondc", "22191": "washingtondc", "22192": "washingtondc",
    "22193": "washingtondc",
    "22201": "washingtondc", "22202": "washingtondc", "22203": "washingtondc",
    "22204": "washingtondc", "22205": "washingtondc", "22206": "washingtondc",
    "22207": "washingtondc", "22209": "washingtondc", "22213": "washingtondc",
    "22214": "washingtondc",
    "22301": "washingtondc", "22302": "washingtondc", "22303": "washingtondc",
    "22304": "washingtondc", "22305": "washingtondc", "22306": "washingtondc",
    "22307": "washingtondc", "22308": "washingtondc", "22309": "washingtondc",
    "22310": "washingtondc", "22311": "washingtondc", "22312": "washingtondc",
    "22314": "washingtondc", "22315": "washingtondc",

    # ══════════════════════════════════════════════════════════
    # NASHVILLE / MIDDLE TENNESSEE
    # ══════════════════════════════════════════════════════════
    "37011": "nashville", "37013": "nashville", "37015": "nashville",
    "37020": "nashville", "37027": "nashville", "37072": "nashville",
    "37076": "nashville", "37080": "nashville", "37115": "nashville",
    "37122": "nashville", "37128": "nashville", "37129": "nashville",
    "37130": "nashville", "37135": "nashville", "37138": "nashville",
    "37143": "nashville", "37148": "nashville", "37153": "nashville",
    "37167": "nashville", "37189": "nashville",
    "37201": "nashville", "37203": "nashville", "37204": "nashville",
    "37205": "nashville", "37206": "nashville", "37207": "nashville",
    "37208": "nashville", "37209": "nashville", "37210": "nashville",
    "37211": "nashville", "37212": "nashville", "37214": "nashville",
    "37215": "nashville", "37216": "nashville", "37217": "nashville",
    "37218": "nashville", "37219": "nashville", "37220": "nashville",
    "37221": "nashville",

    # ══════════════════════════════════════════════════════════
    # PORTLAND, OR
    # ══════════════════════════════════════════════════════════
    "97005": "portland", "97006": "portland", "97007": "portland",
    "97008": "portland", "97015": "portland", "97019": "portland",
    "97024": "portland", "97030": "portland", "97034": "portland",
    "97035": "portland", "97045": "portland", "97062": "portland",
    "97068": "portland", "97070": "portland", "97080": "portland",
    "97086": "portland",
    "97201": "portland", "97202": "portland", "97203": "portland",
    "97204": "portland", "97205": "portland", "97206": "portland",
    "97209": "portland", "97210": "portland", "97211": "portland",
    "97212": "portland", "97213": "portland", "97214": "portland",
    "97215": "portland", "97216": "portland", "97217": "portland",
    "97218": "portland", "97219": "portland", "97220": "portland",
    "97221": "portland", "97222": "portland", "97223": "portland",
    "97224": "portland", "97225": "portland", "97227": "portland",
    "97229": "portland", "97230": "portland", "97231": "portland",
    "97232": "portland", "97233": "portland", "97236": "portland",
    "97239": "portland", "97266": "portland", "97267": "portland",

    # ══════════════════════════════════════════════════════════
    # LAS VEGAS
    # ══════════════════════════════════════════════════════════
    "89002": "lasvegas", "89005": "lasvegas", "89011": "lasvegas",
    "89014": "lasvegas", "89015": "lasvegas", "89030": "lasvegas",
    "89031": "lasvegas", "89032": "lasvegas", "89044": "lasvegas",
    "89048": "lasvegas", "89074": "lasvegas", "89084": "lasvegas",
    "89101": "lasvegas", "89102": "lasvegas", "89103": "lasvegas",
    "89104": "lasvegas", "89106": "lasvegas", "89107": "lasvegas",
    "89108": "lasvegas", "89109": "lasvegas", "89110": "lasvegas",
    "89113": "lasvegas", "89115": "lasvegas", "89117": "lasvegas",
    "89118": "lasvegas", "89119": "lasvegas", "89120": "lasvegas",
    "89121": "lasvegas", "89122": "lasvegas", "89123": "lasvegas",
    "89128": "lasvegas", "89129": "lasvegas", "89130": "lasvegas",
    "89131": "lasvegas", "89134": "lasvegas", "89135": "lasvegas",
    "89138": "lasvegas", "89139": "lasvegas", "89141": "lasvegas",
    "89142": "lasvegas", "89143": "lasvegas", "89144": "lasvegas",
    "89145": "lasvegas", "89146": "lasvegas", "89147": "lasvegas",
    "89148": "lasvegas", "89149": "lasvegas", "89156": "lasvegas",
    "89166": "lasvegas", "89178": "lasvegas", "89179": "lasvegas",
    "89183": "lasvegas",

    # ══════════════════════════════════════════════════════════
    # MINNEAPOLIS / ST. PAUL
    # ══════════════════════════════════════════════════════════
    "55101": "minneapolis", "55102": "minneapolis", "55103": "minneapolis",
    "55104": "minneapolis", "55105": "minneapolis", "55106": "minneapolis",
    "55107": "minneapolis", "55108": "minneapolis", "55109": "minneapolis",
    "55110": "minneapolis", "55112": "minneapolis", "55113": "minneapolis",
    "55114": "minneapolis", "55116": "minneapolis", "55117": "minneapolis",
    "55118": "minneapolis", "55119": "minneapolis", "55120": "minneapolis",
    "55121": "minneapolis", "55122": "minneapolis", "55123": "minneapolis",
    "55124": "minneapolis", "55125": "minneapolis", "55126": "minneapolis",
    "55127": "minneapolis", "55128": "minneapolis", "55129": "minneapolis",
    "55130": "minneapolis",
    "55301": "minneapolis", "55303": "minneapolis", "55304": "minneapolis",
    "55305": "minneapolis", "55306": "minneapolis", "55316": "minneapolis",
    "55317": "minneapolis", "55318": "minneapolis", "55330": "minneapolis",
    "55331": "minneapolis", "55337": "minneapolis", "55340": "minneapolis",
    "55343": "minneapolis", "55344": "minneapolis", "55345": "minneapolis",
    "55346": "minneapolis", "55347": "minneapolis",
    "55356": "minneapolis", "55369": "minneapolis", "55372": "minneapolis",
    "55374": "minneapolis", "55378": "minneapolis", "55379": "minneapolis",
    "55386": "minneapolis", "55387": "minneapolis", "55388": "minneapolis",
    "55391": "minneapolis",
    "55401": "minneapolis", "55402": "minneapolis", "55403": "minneapolis",
    "55404": "minneapolis", "55405": "minneapolis", "55406": "minneapolis",
    "55407": "minneapolis", "55408": "minneapolis", "55409": "minneapolis",
    "55410": "minneapolis", "55411": "minneapolis", "55412": "minneapolis",
    "55413": "minneapolis", "55414": "minneapolis", "55415": "minneapolis",
    "55416": "minneapolis", "55417": "minneapolis", "55418": "minneapolis",
    "55419": "minneapolis", "55420": "minneapolis", "55421": "minneapolis",
    "55422": "minneapolis", "55423": "minneapolis", "55424": "minneapolis",
    "55425": "minneapolis", "55426": "minneapolis", "55427": "minneapolis",
    "55428": "minneapolis", "55429": "minneapolis", "55430": "minneapolis",
    "55431": "minneapolis", "55432": "minneapolis", "55433": "minneapolis",
    "55434": "minneapolis", "55435": "minneapolis", "55436": "minneapolis",
    "55437": "minneapolis", "55438": "minneapolis", "55439": "minneapolis",
    "55441": "minneapolis", "55442": "minneapolis", "55443": "minneapolis",
    "55444": "minneapolis", "55445": "minneapolis", "55446": "minneapolis",
    "55447": "minneapolis",

    # ══════════════════════════════════════════════════════════
    # TAMPA BAY
    # ══════════════════════════════════════════════════════════
    "33510": "tampa", "33511": "tampa", "33527": "tampa",
    "33534": "tampa", "33547": "tampa", "33548": "tampa",
    "33549": "tampa", "33556": "tampa", "33558": "tampa",
    "33559": "tampa", "33563": "tampa", "33565": "tampa",
    "33566": "tampa", "33567": "tampa", "33569": "tampa",
    "33570": "tampa", "33572": "tampa", "33573": "tampa",
    "33576": "tampa", "33578": "tampa", "33579": "tampa",
    "33584": "tampa", "33587": "tampa", "33592": "tampa",
    "33594": "tampa", "33596": "tampa", "33598": "tampa",
    "33601": "tampa", "33602": "tampa", "33603": "tampa",
    "33604": "tampa", "33605": "tampa", "33606": "tampa",
    "33607": "tampa", "33609": "tampa", "33610": "tampa",
    "33611": "tampa", "33612": "tampa", "33613": "tampa",
    "33614": "tampa", "33615": "tampa", "33616": "tampa",
    "33617": "tampa", "33618": "tampa", "33619": "tampa",
    "33624": "tampa", "33625": "tampa", "33626": "tampa",
    "33629": "tampa", "33634": "tampa", "33635": "tampa",
    "33637": "tampa", "33647": "tampa",
    # Clearwater / St. Petersburg
    "33701": "tampa", "33702": "tampa", "33703": "tampa",
    "33704": "tampa", "33705": "tampa", "33706": "tampa",
    "33707": "tampa", "33708": "tampa", "33709": "tampa",
    "33710": "tampa", "33711": "tampa", "33712": "tampa",
    "33713": "tampa", "33714": "tampa", "33715": "tampa",
    "33716": "tampa", "33755": "tampa", "33756": "tampa",
    "33759": "tampa", "33760": "tampa", "33761": "tampa",
    "33762": "tampa", "33763": "tampa", "33764": "tampa",
    "33765": "tampa", "33767": "tampa", "33770": "tampa",
    "33771": "tampa", "33772": "tampa", "33773": "tampa",
    "33774": "tampa", "33776": "tampa", "33777": "tampa",
    "33778": "tampa", "33781": "tampa", "33782": "tampa",
    "33785": "tampa", "33786": "tampa",

    # ══════════════════════════════════════════════════════════
    # ORLANDO
    # ══════════════════════════════════════════════════════════
    "32703": "orlando", "32707": "orlando", "32708": "orlando",
    "32712": "orlando", "32714": "orlando", "32720": "orlando",
    "32724": "orlando", "32725": "orlando", "32726": "orlando",
    "32730": "orlando", "32732": "orlando", "32735": "orlando",
    "32736": "orlando", "32738": "orlando", "32746": "orlando",
    "32750": "orlando", "32751": "orlando", "32757": "orlando",
    "32763": "orlando", "32765": "orlando", "32766": "orlando",
    "32771": "orlando", "32773": "orlando", "32779": "orlando",
    "32780": "orlando", "32789": "orlando", "32792": "orlando",
    "32798": "orlando",
    "32801": "orlando", "32803": "orlando", "32804": "orlando",
    "32805": "orlando", "32806": "orlando", "32807": "orlando",
    "32808": "orlando", "32809": "orlando", "32810": "orlando",
    "32811": "orlando", "32812": "orlando", "32814": "orlando",
    "32816": "orlando", "32817": "orlando", "32818": "orlando",
    "32819": "orlando", "32820": "orlando", "32821": "orlando",
    "32822": "orlando", "32824": "orlando", "32825": "orlando",
    "32826": "orlando", "32827": "orlando", "32828": "orlando",
    "32829": "orlando", "32830": "orlando", "32832": "orlando",
    "32835": "orlando", "32836": "orlando", "32837": "orlando",
    "32839": "orlando",

    # ══════════════════════════════════════════════════════════
    # DETROIT METRO
    # ══════════════════════════════════════════════════════════
    "48009": "detroit", "48015": "detroit", "48017": "detroit",
    "48021": "detroit", "48025": "detroit", "48026": "detroit",
    "48030": "detroit", "48033": "detroit", "48034": "detroit",
    "48035": "detroit", "48036": "detroit", "48038": "detroit",
    "48042": "detroit", "48043": "detroit", "48044": "detroit",
    "48045": "detroit", "48047": "detroit", "48048": "detroit",
    "48066": "detroit", "48067": "detroit", "48069": "detroit",
    "48070": "detroit", "48071": "detroit", "48072": "detroit",
    "48073": "detroit", "48075": "detroit", "48076": "detroit",
    "48080": "detroit", "48081": "detroit", "48082": "detroit",
    "48083": "detroit", "48084": "detroit", "48085": "detroit",
    "48088": "detroit", "48089": "detroit", "48091": "detroit",
    "48092": "detroit", "48093": "detroit", "48098": "detroit",
    "48120": "detroit", "48122": "detroit", "48124": "detroit",
    "48125": "detroit", "48126": "detroit", "48127": "detroit",
    "48128": "detroit", "48134": "detroit", "48135": "detroit",
    "48138": "detroit", "48141": "detroit", "48146": "detroit",
    "48150": "detroit", "48152": "detroit", "48154": "detroit",
    "48162": "detroit", "48164": "detroit", "48165": "detroit",
    "48167": "detroit", "48168": "detroit", "48170": "detroit",
    "48173": "detroit", "48174": "detroit", "48180": "detroit",
    "48183": "detroit", "48184": "detroit", "48185": "detroit",
    "48186": "detroit", "48187": "detroit", "48188": "detroit",
    "48192": "detroit", "48193": "detroit", "48195": "detroit",
    "48197": "detroit", "48198": "detroit",
    "48201": "detroit", "48202": "detroit", "48203": "detroit",
    "48204": "detroit", "48205": "detroit", "48206": "detroit",
    "48207": "detroit", "48208": "detroit", "48209": "detroit",
    "48210": "detroit", "48211": "detroit", "48212": "detroit",
    "48213": "detroit", "48214": "detroit", "48215": "detroit",
    "48216": "detroit", "48217": "detroit", "48218": "detroit",
    "48219": "detroit", "48220": "detroit", "48221": "detroit",
    "48223": "detroit", "48224": "detroit", "48225": "detroit",
    "48226": "detroit", "48227": "detroit", "48228": "detroit",
    "48229": "detroit", "48230": "detroit", "48233": "detroit",
    "48234": "detroit", "48235": "detroit", "48236": "detroit",
    "48237": "detroit", "48238": "detroit", "48239": "detroit",
    "48240": "detroit",

    # ══════════════════════════════════════════════════════════
    # BALTIMORE
    # ══════════════════════════════════════════════════════════
    "21201": "baltimore", "21202": "baltimore", "21203": "baltimore",
    "21204": "baltimore", "21205": "baltimore", "21206": "baltimore",
    "21207": "baltimore", "21208": "baltimore", "21209": "baltimore",
    "21210": "baltimore", "21211": "baltimore", "21212": "baltimore",
    "21213": "baltimore", "21214": "baltimore", "21215": "baltimore",
    "21216": "baltimore", "21217": "baltimore", "21218": "baltimore",
    "21219": "baltimore", "21220": "baltimore", "21221": "baltimore",
    "21222": "baltimore", "21223": "baltimore", "21224": "baltimore",
    "21225": "baltimore", "21226": "baltimore", "21227": "baltimore",
    "21228": "baltimore", "21229": "baltimore", "21230": "baltimore",
    "21231": "baltimore", "21234": "baltimore", "21236": "baltimore",
    "21237": "baltimore", "21239": "baltimore",
    "21061": "baltimore", "21075": "baltimore", "21076": "baltimore",
    "21077": "baltimore", "21090": "baltimore", "21093": "baltimore",
    "21117": "baltimore", "21128": "baltimore", "21131": "baltimore",
    "21133": "baltimore", "21136": "baltimore", "21144": "baltimore",
    "21146": "baltimore", "21153": "baltimore", "21157": "baltimore",
    "21158": "baltimore", "21161": "baltimore", "21162": "baltimore",
    "21163": "baltimore",

    # ══════════════════════════════════════════════════════════
    # CHARLOTTE, NC
    # ══════════════════════════════════════════════════════════
    "28201": "charlotte", "28202": "charlotte", "28203": "charlotte",
    "28204": "charlotte", "28205": "charlotte", "28206": "charlotte",
    "28207": "charlotte", "28208": "charlotte", "28209": "charlotte",
    "28210": "charlotte", "28211": "charlotte", "28212": "charlotte",
    "28213": "charlotte", "28214": "charlotte", "28215": "charlotte",
    "28216": "charlotte", "28217": "charlotte", "28226": "charlotte",
    "28227": "charlotte", "28228": "charlotte", "28269": "charlotte",
    "28270": "charlotte", "28273": "charlotte", "28277": "charlotte",
    "28278": "charlotte",
    "28025": "charlotte", "28027": "charlotte", "28031": "charlotte",
    "28034": "charlotte", "28036": "charlotte", "28037": "charlotte",
    "28052": "charlotte", "28054": "charlotte", "28056": "charlotte",
    "28078": "charlotte", "28079": "charlotte", "28104": "charlotte",
    "28105": "charlotte", "28110": "charlotte", "28112": "charlotte",
    "28115": "charlotte", "28117": "charlotte", "28120": "charlotte",
    "28134": "charlotte", "28138": "charlotte", "28146": "charlotte",
    "28150": "charlotte", "28152": "charlotte", "28166": "charlotte",
    "28173": "charlotte",

    # ══════════════════════════════════════════════════════════
    # RALEIGH / DURHAM (Research Triangle)
    # ══════════════════════════════════════════════════════════
    "27501": "raleigh", "27502": "raleigh", "27503": "raleigh",
    "27504": "raleigh", "27505": "raleigh", "27510": "raleigh",
    "27511": "raleigh", "27512": "raleigh", "27513": "raleigh",
    "27514": "raleigh", "27516": "raleigh", "27517": "raleigh",
    "27518": "raleigh", "27519": "raleigh", "27520": "raleigh",
    "27521": "raleigh", "27522": "raleigh", "27523": "raleigh",
    "27524": "raleigh", "27526": "raleigh", "27527": "raleigh",
    "27529": "raleigh", "27530": "raleigh", "27540": "raleigh",
    "27545": "raleigh", "27560": "raleigh", "27571": "raleigh",
    "27572": "raleigh", "27587": "raleigh", "27591": "raleigh",
    "27592": "raleigh", "27596": "raleigh", "27597": "raleigh",
    "27599": "raleigh",
    "27601": "raleigh", "27603": "raleigh", "27604": "raleigh",
    "27605": "raleigh", "27606": "raleigh", "27607": "raleigh",
    "27608": "raleigh", "27609": "raleigh", "27610": "raleigh",
    "27612": "raleigh", "27613": "raleigh", "27614": "raleigh",
    "27615": "raleigh", "27616": "raleigh", "27617": "raleigh",
    "27701": "raleigh", "27703": "raleigh", "27704": "raleigh",
    "27705": "raleigh", "27706": "raleigh", "27707": "raleigh",
    "27708": "raleigh", "27709": "raleigh", "27710": "raleigh",
    "27712": "raleigh", "27713": "raleigh",

    # ══════════════════════════════════════════════════════════
    # INDIANAPOLIS
    # ══════════════════════════════════════════════════════════
    "46032": "indianapolis", "46033": "indianapolis", "46034": "indianapolis",
    "46036": "indianapolis", "46037": "indianapolis", "46038": "indianapolis",
    "46040": "indianapolis", "46052": "indianapolis", "46055": "indianapolis",
    "46060": "indianapolis", "46062": "indianapolis", "46064": "indianapolis",
    "46069": "indianapolis", "46074": "indianapolis", "46075": "indianapolis",
    "46077": "indianapolis",
    "46107": "indianapolis", "46112": "indianapolis", "46113": "indianapolis",
    "46117": "indianapolis", "46118": "indianapolis", "46122": "indianapolis",
    "46123": "indianapolis", "46130": "indianapolis", "46131": "indianapolis",
    "46140": "indianapolis", "46142": "indianapolis", "46143": "indianapolis",
    "46147": "indianapolis", "46148": "indianapolis", "46149": "indianapolis",
    "46150": "indianapolis", "46157": "indianapolis", "46158": "indianapolis",
    "46163": "indianapolis", "46164": "indianapolis", "46168": "indianapolis",
    "46184": "indianapolis",
    "46201": "indianapolis", "46202": "indianapolis", "46203": "indianapolis",
    "46204": "indianapolis", "46205": "indianapolis", "46206": "indianapolis",
    "46207": "indianapolis", "46208": "indianapolis", "46209": "indianapolis",
    "46214": "indianapolis", "46216": "indianapolis", "46217": "indianapolis",
    "46218": "indianapolis", "46219": "indianapolis", "46220": "indianapolis",
    "46221": "indianapolis", "46222": "indianapolis", "46224": "indianapolis",
    "46225": "indianapolis", "46226": "indianapolis", "46227": "indianapolis",
    "46228": "indianapolis", "46229": "indianapolis", "46230": "indianapolis",
    "46231": "indianapolis", "46234": "indianapolis", "46235": "indianapolis",
    "46236": "indianapolis", "46237": "indianapolis", "46239": "indianapolis",
    "46240": "indianapolis", "46241": "indianapolis", "46250": "indianapolis",
    "46254": "indianapolis", "46256": "indianapolis", "46259": "indianapolis",
    "46260": "indianapolis", "46268": "indianapolis", "46278": "indianapolis",
    "46280": "indianapolis",

    # ══════════════════════════════════════════════════════════
    # COLUMBUS, OH
    # ══════════════════════════════════════════════════════════
    "43002": "columbus", "43004": "columbus", "43016": "columbus",
    "43017": "columbus", "43021": "columbus", "43023": "columbus",
    "43026": "columbus", "43035": "columbus", "43054": "columbus",
    "43065": "columbus", "43068": "columbus", "43081": "columbus",
    "43085": "columbus", "43102": "columbus", "43103": "columbus",
    "43106": "columbus", "43107": "columbus", "43109": "columbus",
    "43110": "columbus", "43111": "columbus", "43112": "columbus",
    "43113": "columbus", "43116": "columbus", "43119": "columbus",
    "43123": "columbus", "43125": "columbus", "43128": "columbus",
    "43130": "columbus", "43137": "columbus", "43138": "columbus",
    "43140": "columbus", "43143": "columbus", "43144": "columbus",
    "43146": "columbus", "43147": "columbus", "43148": "columbus",
    "43150": "columbus", "43151": "columbus", "43153": "columbus",
    "43154": "columbus", "43156": "columbus", "43157": "columbus",
    "43160": "columbus", "43162": "columbus", "43164": "columbus",
    "43201": "columbus", "43202": "columbus", "43203": "columbus",
    "43204": "columbus", "43205": "columbus", "43206": "columbus",
    "43207": "columbus", "43209": "columbus", "43210": "columbus",
    "43211": "columbus", "43212": "columbus", "43213": "columbus",
    "43214": "columbus", "43215": "columbus", "43217": "columbus",
    "43219": "columbus", "43220": "columbus", "43221": "columbus",
    "43222": "columbus", "43223": "columbus", "43224": "columbus",
    "43227": "columbus", "43228": "columbus", "43229": "columbus",
    "43230": "columbus", "43231": "columbus", "43232": "columbus",
    "43235": "columbus", "43240": "columbus",

    # ══════════════════════════════════════════════════════════
    # KANSAS CITY (MO + KS)
    # ══════════════════════════════════════════════════════════
    "64050": "kansascity", "64052": "kansascity", "64053": "kansascity",
    "64054": "kansascity", "64055": "kansascity", "64056": "kansascity",
    "64057": "kansascity", "64058": "kansascity",
    "64063": "kansascity", "64064": "kansascity", "64065": "kansascity",
    "64066": "kansascity", "64067": "kansascity", "64068": "kansascity",
    "64070": "kansascity", "64075": "kansascity", "64076": "kansascity",
    "64079": "kansascity", "64080": "kansascity", "64081": "kansascity",
    "64082": "kansascity", "64083": "kansascity", "64084": "kansascity",
    "64086": "kansascity", "64088": "kansascity", "64089": "kansascity",
    "64093": "kansascity",
    "64101": "kansascity", "64102": "kansascity", "64105": "kansascity",
    "64106": "kansascity", "64108": "kansascity", "64109": "kansascity",
    "64110": "kansascity", "64111": "kansascity", "64112": "kansascity",
    "64113": "kansascity", "64114": "kansascity", "64116": "kansascity",
    "64117": "kansascity", "64118": "kansascity", "64119": "kansascity",
    "64120": "kansascity", "64123": "kansascity", "64124": "kansascity",
    "64125": "kansascity", "64126": "kansascity", "64127": "kansascity",
    "64128": "kansascity", "64129": "kansascity", "64130": "kansascity",
    "64131": "kansascity", "64132": "kansascity", "64133": "kansascity",
    "64134": "kansascity", "64136": "kansascity", "64137": "kansascity",
    "64138": "kansascity", "64139": "kansascity", "64145": "kansascity",
    "64146": "kansascity", "64147": "kansascity", "64148": "kansascity",
    "64149": "kansascity", "64150": "kansascity", "64151": "kansascity",
    "64152": "kansascity", "64153": "kansascity", "64154": "kansascity",
    "64155": "kansascity", "64156": "kansascity", "64157": "kansascity",
    "64158": "kansascity", "64161": "kansascity", "64163": "kansascity",
    "64164": "kansascity", "64165": "kansascity", "64166": "kansascity",
    "64167": "kansascity", "64168": "kansascity",
    # Kansas side
    "66013": "kansascity", "66030": "kansascity", "66061": "kansascity",
    "66062": "kansascity", "66083": "kansascity", "66085": "kansascity",
    "66101": "kansascity", "66102": "kansascity", "66103": "kansascity",
    "66104": "kansascity", "66105": "kansascity", "66106": "kansascity",
    "66109": "kansascity", "66111": "kansascity", "66112": "kansascity",
    "66115": "kansascity", "66118": "kansascity",
    "66202": "kansascity", "66203": "kansascity", "66204": "kansascity",
    "66205": "kansascity", "66206": "kansascity", "66207": "kansascity",
    "66208": "kansascity", "66209": "kansascity", "66210": "kansascity",
    "66211": "kansascity", "66212": "kansascity", "66213": "kansascity",
    "66214": "kansascity", "66215": "kansascity", "66216": "kansascity",
    "66217": "kansascity", "66218": "kansascity", "66219": "kansascity",
    "66220": "kansascity", "66221": "kansascity", "66223": "kansascity",
    "66224": "kansascity", "66226": "kansascity", "66227": "kansascity",
    "66251": "kansascity",

    # ══════════════════════════════════════════════════════════
    # ST. LOUIS
    # ══════════════════════════════════════════════════════════
    "63005": "stlouis", "63011": "stlouis", "63017": "stlouis",
    "63021": "stlouis", "63025": "stlouis", "63026": "stlouis",
    "63031": "stlouis", "63033": "stlouis", "63034": "stlouis",
    "63038": "stlouis", "63040": "stlouis", "63042": "stlouis",
    "63043": "stlouis", "63044": "stlouis", "63045": "stlouis",
    "63047": "stlouis", "63049": "stlouis", "63052": "stlouis",
    "63055": "stlouis", "63056": "stlouis", "63057": "stlouis",
    "63060": "stlouis", "63069": "stlouis", "63074": "stlouis",
    "63088": "stlouis", "63101": "stlouis", "63102": "stlouis",
    "63103": "stlouis", "63104": "stlouis", "63105": "stlouis",
    "63106": "stlouis", "63107": "stlouis", "63108": "stlouis",
    "63109": "stlouis", "63110": "stlouis", "63111": "stlouis",
    "63112": "stlouis", "63113": "stlouis", "63114": "stlouis",
    "63115": "stlouis", "63116": "stlouis", "63117": "stlouis",
    "63118": "stlouis", "63119": "stlouis", "63120": "stlouis",
    "63121": "stlouis", "63122": "stlouis", "63123": "stlouis",
    "63124": "stlouis", "63125": "stlouis", "63126": "stlouis",
    "63127": "stlouis", "63128": "stlouis", "63129": "stlouis",
    "63130": "stlouis", "63131": "stlouis", "63132": "stlouis",
    "63133": "stlouis", "63134": "stlouis", "63135": "stlouis",
    "63136": "stlouis", "63137": "stlouis", "63138": "stlouis",
    "63139": "stlouis", "63140": "stlouis", "63141": "stlouis",
    "63143": "stlouis", "63144": "stlouis", "63146": "stlouis",
    "63147": "stlouis",

    # ══════════════════════════════════════════════════════════
    # PITTSBURGH
    # ══════════════════════════════════════════════════════════
    "15001": "pittsburgh", "15003": "pittsburgh", "15004": "pittsburgh",
    "15005": "pittsburgh", "15006": "pittsburgh", "15007": "pittsburgh",
    "15010": "pittsburgh", "15012": "pittsburgh", "15014": "pittsburgh",
    "15015": "pittsburgh", "15017": "pittsburgh", "15018": "pittsburgh",
    "15019": "pittsburgh", "15020": "pittsburgh", "15021": "pittsburgh",
    "15022": "pittsburgh", "15024": "pittsburgh", "15025": "pittsburgh",
    "15026": "pittsburgh", "15027": "pittsburgh", "15028": "pittsburgh",
    "15030": "pittsburgh", "15031": "pittsburgh", "15032": "pittsburgh",
    "15033": "pittsburgh", "15034": "pittsburgh", "15035": "pittsburgh",
    "15037": "pittsburgh", "15038": "pittsburgh", "15042": "pittsburgh",
    "15043": "pittsburgh", "15044": "pittsburgh", "15045": "pittsburgh",
    "15046": "pittsburgh", "15047": "pittsburgh", "15049": "pittsburgh",
    "15050": "pittsburgh", "15051": "pittsburgh", "15052": "pittsburgh",
    "15053": "pittsburgh", "15054": "pittsburgh", "15055": "pittsburgh",
    "15056": "pittsburgh", "15057": "pittsburgh", "15059": "pittsburgh",
    "15060": "pittsburgh", "15061": "pittsburgh", "15062": "pittsburgh",
    "15063": "pittsburgh", "15064": "pittsburgh", "15065": "pittsburgh",
    "15066": "pittsburgh", "15067": "pittsburgh", "15068": "pittsburgh",
    "15071": "pittsburgh", "15074": "pittsburgh", "15075": "pittsburgh",
    "15076": "pittsburgh", "15077": "pittsburgh", "15078": "pittsburgh",
    "15081": "pittsburgh", "15082": "pittsburgh", "15083": "pittsburgh",
    "15084": "pittsburgh", "15085": "pittsburgh", "15086": "pittsburgh",
    "15087": "pittsburgh", "15088": "pittsburgh", "15089": "pittsburgh",
    "15090": "pittsburgh", "15091": "pittsburgh",
    "15101": "pittsburgh", "15102": "pittsburgh", "15104": "pittsburgh",
    "15106": "pittsburgh", "15108": "pittsburgh", "15110": "pittsburgh",
    "15112": "pittsburgh", "15116": "pittsburgh", "15120": "pittsburgh",
    "15122": "pittsburgh", "15126": "pittsburgh", "15129": "pittsburgh",
    "15131": "pittsburgh", "15132": "pittsburgh", "15133": "pittsburgh",
    "15134": "pittsburgh", "15135": "pittsburgh", "15136": "pittsburgh",
    "15137": "pittsburgh", "15139": "pittsburgh", "15140": "pittsburgh",
    "15142": "pittsburgh", "15143": "pittsburgh", "15144": "pittsburgh",
    "15145": "pittsburgh", "15146": "pittsburgh", "15147": "pittsburgh",
    "15148": "pittsburgh",
    "15201": "pittsburgh", "15202": "pittsburgh", "15203": "pittsburgh",
    "15204": "pittsburgh", "15205": "pittsburgh", "15206": "pittsburgh",
    "15207": "pittsburgh", "15208": "pittsburgh", "15209": "pittsburgh",
    "15210": "pittsburgh", "15211": "pittsburgh", "15212": "pittsburgh",
    "15213": "pittsburgh", "15214": "pittsburgh", "15215": "pittsburgh",
    "15216": "pittsburgh", "15217": "pittsburgh", "15218": "pittsburgh",
    "15219": "pittsburgh", "15220": "pittsburgh", "15221": "pittsburgh",
    "15222": "pittsburgh", "15223": "pittsburgh", "15224": "pittsburgh",
    "15225": "pittsburgh", "15226": "pittsburgh", "15227": "pittsburgh",
    "15228": "pittsburgh", "15229": "pittsburgh", "15230": "pittsburgh",
    "15231": "pittsburgh", "15232": "pittsburgh", "15233": "pittsburgh",
    "15234": "pittsburgh", "15235": "pittsburgh", "15236": "pittsburgh",
    "15237": "pittsburgh", "15238": "pittsburgh", "15239": "pittsburgh",
    "15240": "pittsburgh", "15241": "pittsburgh", "15242": "pittsburgh",
    "15243": "pittsburgh", "15244": "pittsburgh",

    # ══════════════════════════════════════════════════════════
    # SALT LAKE CITY / PROVO / OGDEN
    # ══════════════════════════════════════════════════════════
    "84003": "saltlakecity", "84004": "saltlakecity", "84005": "saltlakecity",
    "84006": "saltlakecity", "84009": "saltlakecity", "84010": "saltlakecity",
    "84013": "saltlakecity", "84014": "saltlakecity", "84015": "saltlakecity",
    "84020": "saltlakecity", "84025": "saltlakecity", "84032": "saltlakecity",
    "84037": "saltlakecity", "84040": "saltlakecity", "84041": "saltlakecity",
    "84042": "saltlakecity", "84043": "saltlakecity", "84044": "saltlakecity",
    "84045": "saltlakecity", "84047": "saltlakecity", "84054": "saltlakecity",
    "84057": "saltlakecity", "84058": "saltlakecity", "84060": "saltlakecity",
    "84062": "saltlakecity", "84065": "saltlakecity", "84067": "saltlakecity",
    "84070": "saltlakecity", "84074": "saltlakecity", "84075": "saltlakecity",
    "84080": "saltlakecity", "84081": "saltlakecity", "84084": "saltlakecity",
    "84087": "saltlakecity", "84088": "saltlakecity", "84092": "saltlakecity",
    "84093": "saltlakecity", "84094": "saltlakecity", "84095": "saltlakecity",
    "84096": "saltlakecity",
    "84101": "saltlakecity", "84102": "saltlakecity", "84103": "saltlakecity",
    "84104": "saltlakecity", "84105": "saltlakecity", "84106": "saltlakecity",
    "84107": "saltlakecity", "84108": "saltlakecity", "84109": "saltlakecity",
    "84110": "saltlakecity", "84111": "saltlakecity", "84112": "saltlakecity",
    "84113": "saltlakecity", "84114": "saltlakecity", "84115": "saltlakecity",
    "84116": "saltlakecity", "84117": "saltlakecity", "84118": "saltlakecity",
    "84119": "saltlakecity", "84120": "saltlakecity", "84121": "saltlakecity",
    "84123": "saltlakecity", "84124": "saltlakecity", "84128": "saltlakecity",
    # Provo / Orem
    "84601": "provo", "84602": "provo", "84603": "provo",
    "84604": "provo", "84605": "provo", "84606": "provo",
    "84653": "provo", "84655": "provo", "84656": "provo",
    "84660": "provo", "84663": "provo", "84664": "provo",
    # Ogden
    "84401": "ogden", "84403": "ogden", "84404": "ogden",
    "84405": "ogden", "84407": "ogden", "84408": "ogden",
    "84412": "ogden",
    "84310": "ogden", "84315": "ogden", "84317": "ogden",
    "84319": "ogden", "84321": "ogden", "84322": "ogden",
    "84327": "ogden", "84330": "ogden", "84332": "ogden",
    "84339": "ogden", "84341": "ogden",
}

# ── 3-digit prefix map (fallback for non-metro ZIPs) ─────────
# Keep this EXACT as originally provided — it covers rural areas
# not represented in EXACT_ZIP_MAP above.
ZIP_PREFIX_MAP = {
    # ── Alabama ──────────────────────────────────────────────
    "350": "bham", "351": "bham", "352": "bham", "353": "bham",
    "354": "bham", "355": "bham", "356": "huntsville", "357": "huntsville",
    "358": "huntsville", "359": "gadsden", "360": "montgomery",
    "361": "montgomery", "362": "montgomery", "363": "dothan",
    "364": "dothan", "365": "mobile", "366": "mobile", "367": "mobile",
    "368": "mobile", "369": "shoals", "396": "tuscaloosa", "397": "tuscaloosa",
    # ── Alaska ───────────────────────────────────────────────
    "995": "anchorage", "996": "anchorage", "997": "fairbanks",
    "998": "fairbanks", "999": "juneau",
    # ── Arizona ──────────────────────────────────────────────
    "850": "phoenix", "851": "phoenix", "852": "phoenix", "853": "phoenix",
    "854": "phoenix", "855": "phoenix", "856": "tucson", "857": "tucson",
    "858": "tucson", "859": "tucson", "860": "flagstaff", "861": "flagstaff",
    "863": "prescott", "864": "prescott", "865": "prescott",
    "872": "yuma", "873": "yuma",
    # ── Arkansas ─────────────────────────────────────────────
    "716": "littlerock", "717": "littlerock", "718": "littlerock",
    "719": "littlerock", "720": "littlerock", "721": "littlerock",
    "722": "fayar", "723": "fayar", "724": "fayar",
    "725": "fortsmith", "726": "fortsmith", "727": "fortsmith",
    "728": "jonesboro", "729": "jonesboro",
    "755": "texarkana", "756": "texarkana",
    # ── California ───────────────────────────────────────────
    "900": "losangeles", "901": "losangeles", "902": "losangeles",
    "903": "losangeles", "904": "losangeles", "905": "losangeles",
    "906": "losangeles", "907": "losangeles", "908": "losangeles",
    "910": "losangeles", "911": "losangeles", "912": "losangeles",
    "913": "losangeles", "914": "losangeles", "915": "losangeles",
    "916": "inlandempire", "917": "inlandempire", "918": "inlandempire",
    "919": "inlandempire", "920": "sandiego", "921": "sandiego",
    "922": "sandiego", "923": "sandiego", "924": "sandiego",
    "925": "orangecounty", "926": "orangecounty", "927": "orangecounty",
    "928": "orangecounty", "929": "palmsprings",
    "930": "ventura", "931": "ventura", "932": "bakersfield",
    "933": "bakersfield", "934": "bakersfield", "935": "bakersfield",
    "936": "visalia", "937": "visalia", "938": "fresno",
    "939": "monterey", "940": "sfbay", "941": "sfbay", "942": "sfbay",
    "943": "sfbay", "944": "sfbay", "945": "sfbay", "946": "sfbay",
    "947": "sfbay", "948": "sfbay", "949": "sfbay",
    "950": "sfbay", "951": "sfbay", "952": "sfbay", "953": "sfbay",
    "954": "sfbay", "955": "sfbay", "956": "sacramento",
    "957": "sacramento", "958": "sacramento", "959": "sacramento",
    "960": "redding", "961": "redding",
    "962": "chico", "963": "chico",
    "964": "mendocino", "965": "humboldt", "966": "humboldt",
    "967": "stockton", "968": "stockton",
    "969": "modesto", "970": "merced",
    "971": "fresno", "972": "fresno", "973": "fresno",
    "974": "goldcountry", "975": "goldcountry",
    "976": "yubasutter", "977": "chico",
    "978": "slo", "979": "santabarbara", "980": "santamaria",
    # ── Colorado ─────────────────────────────────────────────
    "800": "denver", "801": "denver", "802": "denver", "803": "denver",
    "804": "denver", "805": "denver", "806": "denver", "807": "boulder",
    "808": "boulder", "809": "boulder",
    "810": "cosprings", "811": "cosprings", "812": "cosprings",
    "813": "cosprings", "814": "pueblo", "815": "pueblo",
    "816": "pueblo", "817": "eastco", "818": "eastco",
    "819": "rockies", "820": "rockies",
    "821": "fortcollins", "822": "fortcollins", "823": "fortcollins",
    "824": "fortcollins", "825": "westslope", "826": "westslope",
    # ── Connecticut ──────────────────────────────────────────
    "060": "hartford", "061": "hartford", "062": "hartford",
    "063": "newhaven", "064": "newhaven", "065": "newhaven",
    "066": "newhaven", "067": "hartford", "068": "nwct",
    "069": "newlondon",
    # ── Delaware ─────────────────────────────────────────────
    "197": "delaware", "198": "delaware", "199": "delaware",
    # ── District of Columbia ─────────────────────────────────
    "200": "washingtondc", "201": "washingtondc", "202": "washingtondc",
    "203": "washingtondc", "204": "washingtondc", "205": "washingtondc",
    # ── Florida ──────────────────────────────────────────────
    "320": "jacksonville", "321": "jacksonville", "322": "jacksonville",
    "323": "tallahassee", "324": "tallahassee", "325": "tallahassee",
    "326": "gainesville", "327": "gainesville", "328": "gainesville",
    "329": "daytona", "330": "miami", "331": "miami", "332": "miami",
    "333": "miami", "334": "fortmyers", "335": "tampa", "336": "tampa",
    "337": "tampa", "338": "tampa", "339": "fortmyers",
    "340": "keys", "341": "fortmyers", "342": "sarasota",
    "343": "sarasota", "344": "sarasota", "345": "orlando",
    "346": "orlando", "347": "orlando", "348": "orlando",
    "349": "orlando", "354": "lakeland", "355": "lakeland",
    "356": "staugustine", "357": "staugustine",
    "358": "pensacola", "359": "pensacola",
    "352": "ocala", "353": "ocala",
    "364": "panamacity", "365": "panamacity",
    "366": "treasure", "367": "treasure",
    # ── Georgia ──────────────────────────────────────────────
    "300": "atlanta", "301": "atlanta", "302": "atlanta",
    "303": "atlanta", "304": "atlanta", "305": "atlanta",
    "306": "atlanta", "307": "atlanta", "308": "augusta",
    "309": "augusta", "310": "macon", "311": "macon",
    "312": "savannah", "313": "savannah", "314": "savannah",
    "315": "brunswick", "316": "brunswick",
    "317": "albanyga", "318": "albanyga",
    "319": "valdosta", "398": "columbusga", "399": "columbusga",
    # ── Hawaii ───────────────────────────────────────────────
    "967": "honolulu", "968": "honolulu",
    # ── Idaho ────────────────────────────────────────────────
    "832": "boise", "833": "boise", "834": "boise", "835": "boise",
    "836": "boise", "837": "boise", "838": "lewiston",
    # ── Illinois ─────────────────────────────────────────────
    "600": "chicago", "601": "chicago", "602": "chicago",
    "603": "chicago", "604": "chicago", "605": "chicago",
    "606": "chicago", "607": "chicago", "608": "chicago",
    "609": "chicago", "610": "rockford", "611": "rockford",
    "612": "rockford", "613": "peoria", "614": "peoria",
    "615": "peoria", "616": "peoria", "617": "bloomington",
    "618": "carbondale", "619": "carbondale",
    "620": "springfieldil", "621": "springfieldil",
    "622": "springfieldil", "623": "decatur", "624": "decatur",
    "625": "champaign", "626": "champaign", "628": "mattoon",
    "630": "lasalle", "631": "lasalle", "632": "quincy",
    "633": "quincy", "634": "chambana",
    # ── Indiana ──────────────────────────────────────────────
    "460": "indianapolis", "461": "indianapolis", "462": "indianapolis",
    "463": "indianapolis", "464": "indianapolis", "465": "fortwayne",
    "466": "fortwayne", "467": "fortwayne", "468": "fortwayne",
    "469": "kokomo", "470": "bloomington",
    "471": "evansville", "472": "evansville", "473": "evansville",
    "474": "bloomington", "475": "muncie",
    "476": "southbend", "477": "southbend",
    "478": "terrehaute", "479": "terrehaute",
    # ── Iowa ─────────────────────────────────────────────────
    "500": "desmoines", "501": "desmoines", "502": "desmoines",
    "503": "desmoines", "504": "desmoines", "505": "desmoines",
    "506": "waterloo", "507": "waterloo",
    "508": "cedarrapids", "509": "cedarrapids",
    "510": "siouxcity", "511": "siouxcity",
    "512": "siouxcity", "513": "siouxcity",
    "514": "quadcities", "515": "quadcities",
    "516": "iowacity", "520": "dubuque",
    "521": "dubuque", "522": "iowacity",
    "523": "masoncity", "524": "masoncity",
    "525": "ames", "526": "ames",
    "527": "fortdodge", "528": "fortdodge",
    # ── Kansas ───────────────────────────────────────────────
    "660": "kansascity", "661": "kansascity",
    "662": "topeka", "663": "topeka", "664": "topeka",
    "665": "wichita", "666": "wichita", "667": "wichita",
    "668": "salina", "669": "salina",
    "670": "wichita", "671": "wichita", "672": "wichita",
    "673": "ksu", "674": "ksu",
    "675": "nwks", "676": "nwks",
    "677": "swks", "678": "swks",
    "679": "seks", "680": "seks",
    "681": "lawrence",
    # ── Kentucky ─────────────────────────────────────────────
    "400": "louisville", "401": "louisville", "402": "louisville",
    "403": "louisville", "404": "louisville",
    "405": "lexington", "406": "lexington", "407": "lexington",
    "408": "lexington", "409": "lexington",
    "410": "louisville", "411": "eastky", "412": "eastky",
    "413": "eastky", "414": "eastky",
    "415": "owensboro", "416": "owensboro", "417": "owensboro",
    "418": "bgky", "419": "westky", "420": "westky",
    "421": "westky", "422": "westky",
    "423": "eastky", "424": "eastky",
    "425": "eastky", "426": "eastky", "427": "eastky",
    # ── Louisiana ────────────────────────────────────────────
    "700": "neworleans", "701": "neworleans",
    "702": "neworleans", "703": "neworleans",
    "704": "lafayette", "705": "lafayette", "706": "lafayette",
    "707": "batonrouge", "708": "batonrouge",
    "710": "shreveport", "711": "shreveport", "712": "shreveport",
    "713": "lakecharles", "714": "lakecharles",
    "715": "cenla", "716": "monroe", "717": "monroe",
    "718": "houma",
    # ── Maine ────────────────────────────────────────────────
    "039": "maine", "040": "maine", "041": "maine",
    "042": "maine", "043": "maine", "044": "maine",
    "045": "maine", "046": "maine", "047": "maine",
    "048": "maine", "049": "maine",
    # ── Maryland ─────────────────────────────────────────────
    "207": "washingtondc", "208": "washingtondc", "209": "washingtondc",
    "210": "baltimore", "211": "baltimore",
    "212": "baltimore", "213": "baltimore", "214": "baltimore",
    "215": "baltimore", "216": "baltimore",
    "217": "annapolis", "218": "annapolis",
    "219": "easternshore",
    "206": "smd", "220": "fredericksburg", "221": "fredericksburg",
    # ── Massachusetts ────────────────────────────────────────
    "010": "worcester", "011": "worcester", "012": "worcester",
    "013": "worcester", "014": "worcester",
    "015": "worcester", "016": "worcester", "017": "worcester",
    "018": "boston", "019": "boston", "020": "boston", "021": "boston",
    "022": "boston", "023": "boston", "024": "boston",
    "025": "capecod", "026": "capecod", "027": "capecod",
    "028": "southcoast", "029": "southcoast",
    # ── Michigan ─────────────────────────────────────────────
    "480": "detroit", "481": "detroit", "482": "detroit",
    "483": "detroit", "484": "detroit", "485": "detroit",
    "486": "flint", "487": "flint", "488": "annarbor",
    "489": "annarbor", "490": "battlecreek", "491": "kalamazoo",
    "492": "kalamazoo", "493": "grandrapids",
    "494": "grandrapids", "495": "grandrapids",
    "496": "centralmich", "497": "lansing",
    "498": "up", "499": "up",
    # ── Minnesota ────────────────────────────────────────────
    "550": "minneapolis", "551": "minneapolis",
    "552": "minneapolis", "553": "minneapolis",
    "554": "minneapolis", "555": "minneapolis",
    "556": "duluth", "557": "duluth", "558": "duluth",
    "559": "rmn", "560": "mankato", "561": "mankato",
    "562": "stcloud", "563": "stcloud", "564": "brainerd",
    "565": "bemidji", "566": "bemidji", "567": "brainerd",
    # ── Mississippi ──────────────────────────────────────────
    "386": "jackson", "387": "jackson", "388": "jackson",
    "389": "meridian", "390": "jackson",
    "391": "jackson", "392": "jackson",
    "393": "hattiesburg", "394": "gulfport",
    "395": "gulfport", "396": "northmiss", "397": "northmiss",
    # ── Missouri ─────────────────────────────────────────────
    "630": "stlouis", "631": "stlouis", "632": "stlouis",
    "633": "stlouis", "634": "stlouis", "635": "stlouis",
    "636": "stlouis", "637": "springfield",
    "638": "springfield", "639": "springfield",
    "640": "kansascity", "641": "kansascity",
    "644": "stjoseph", "645": "stjoseph",
    "646": "kirksville", "647": "kirksville",
    "648": "joplin", "649": "joplin",
    "650": "columbiamo", "651": "columbiamo",
    "652": "columbiamo", "653": "columbiamo",
    "654": "loz", "655": "loz",
    "656": "semo", "657": "semo", "658": "semo",
    # ── Montana ──────────────────────────────────────────────
    "590": "billings", "591": "billings", "592": "billings",
    "593": "bozeman", "594": "greatfalls", "595": "greatfalls",
    "596": "missoula", "597": "missoula",
    "598": "kalispell", "599": "kalispell",
    # ── Nebraska ─────────────────────────────────────────────
    "680": "omaha", "681": "omaha", "682": "omaha",
    "683": "lincoln", "684": "lincoln", "685": "lincoln",
    "686": "grandisland", "687": "grandisland",
    "688": "northplatte", "689": "scottsbluff",
    "690": "scottsbluff", "691": "scottsbluff",
    "692": "northplatte",
    # ── Nevada ───────────────────────────────────────────────
    "889": "lasvegas", "890": "lasvegas", "891": "lasvegas",
    "894": "reno", "895": "reno", "897": "reno", "898": "reno",
    "893": "elko",
    # ── New Hampshire ────────────────────────────────────────
    "030": "nh", "031": "nh", "032": "nh", "033": "nh", "034": "nh",
    "035": "nh", "036": "nh", "037": "nh", "038": "nh",
    # ── New Jersey ───────────────────────────────────────────
    "070": "newjersey", "071": "newjersey", "072": "newjersey",
    "073": "newjersey", "074": "newjersey", "075": "newjersey",
    "076": "newjersey", "077": "jerseyshore",
    "078": "newjersey", "079": "newjersey",
    "080": "southjersey", "081": "southjersey",
    "082": "southjersey", "083": "southjersey",
    "084": "southjersey", "085": "cnj",
    "086": "cnj", "087": "cnj", "088": "cnj", "089": "cnj",
    # ── New Mexico ───────────────────────────────────────────
    "870": "albuquerque", "871": "albuquerque",
    "872": "albuquerque", "873": "albuquerque",
    "874": "santafe", "875": "santafe",
    "876": "farmington", "877": "farmington",
    "878": "lascruces", "879": "lascruces",
    "880": "lascruces", "881": "roswell",
    "882": "roswell", "883": "clovis",
    # ── New York ─────────────────────────────────────────────
    "005": "longisland",
    "100": "newyork", "101": "newyork", "102": "newyork",
    "103": "newyork", "104": "newyork",
    "105": "hudsonvalley", "106": "hudsonvalley",
    "107": "hudsonvalley", "108": "hudsonvalley", "109": "hudsonvalley",
    "110": "longisland", "111": "longisland", "112": "longisland",
    "113": "longisland", "114": "longisland", "115": "longisland",
    "116": "longisland", "117": "longisland", "118": "longisland",
    "119": "longisland",
    "120": "albany", "121": "albany", "122": "albany", "123": "albany",
    "124": "catskills", "125": "catskills", "126": "catskills", "127": "catskills",
    "128": "plattsburgh", "129": "plattsburgh",
    "130": "syracuse", "131": "syracuse", "132": "syracuse",
    "133": "utica", "134": "utica", "135": "utica", "136": "utica",
    "137": "binghamton", "138": "binghamton", "139": "binghamton",
    "140": "buffalo", "141": "buffalo", "142": "buffalo", "143": "buffalo",
    "144": "rochester", "145": "rochester", "146": "rochester",
    "147": "ithaca", "148": "ithaca", "149": "elmira",
    # ── North Carolina ───────────────────────────────────────
    "270": "greensboro", "271": "greensboro", "272": "greensboro",
    "273": "greensboro", "274": "eastnc",
    "275": "raleigh", "276": "raleigh", "277": "raleigh",
    "278": "raleigh", "279": "raleigh",
    "280": "charlotte", "281": "charlotte", "282": "charlotte",
    "283": "charlotte", "284": "wilmington",
    "285": "onslow", "286": "asheville", "287": "asheville",
    "288": "winstonsalem", "289": "fayetteville",
    # ── North Dakota ─────────────────────────────────────────
    "580": "fargo", "581": "fargo", "582": "grandforks",
    "583": "grandforks", "584": "bismarck",
    "585": "bismarck", "586": "bismarck",
    "587": "nd", "588": "nd",
    # ── Ohio ─────────────────────────────────────────────────
    "430": "columbus", "431": "columbus", "432": "columbus",
    "433": "columbus", "434": "toledo",
    "435": "toledo", "436": "toledo", "437": "zanesville",
    "438": "zanesville", "439": "zanesville",
    "440": "cleveland", "441": "cleveland",
    "442": "akroncanton", "443": "akroncanton",
    "444": "youngstown", "445": "youngstown",
    "446": "youngstown", "447": "youngstown",
    "448": "mansfield", "449": "mansfield",
    "450": "cincinnati", "451": "cincinnati",
    "452": "cincinnati", "453": "cincinnati",
    "454": "dayton", "455": "dayton",
    "456": "dayton", "457": "dayton",
    "458": "limaohio", "459": "limaohio",
    # ── Oklahoma ─────────────────────────────────────────────
    "730": "oklahomacity", "731": "oklahomacity",
    "732": "oklahomacity", "733": "oklahomacity",
    "734": "oklahomacity", "735": "lawton",
    "736": "enid", "737": "enid", "738": "enid", "739": "enid",
    "740": "tulsa", "741": "tulsa", "742": "tulsa",
    "743": "tulsa", "744": "tulsa", "745": "tulsa",
    "746": "stillwater", "747": "stillwater",
    # ── Oregon ───────────────────────────────────────────────
    "970": "portland", "971": "portland", "972": "portland",
    "973": "portland", "974": "portland",
    "975": "salem", "976": "salem",
    "977": "eugene", "978": "eugene",
    "979": "corvallis",
    # ── Pennsylvania ─────────────────────────────────────────
    "150": "pittsburgh", "151": "pittsburgh", "152": "pittsburgh",
    "153": "pittsburgh", "154": "pittsburgh",
    "155": "pittsburgh", "156": "pittsburgh", "157": "pittsburgh",
    "158": "altoona", "159": "altoona",
    "160": "meadville", "161": "erie", "162": "erie", "163": "erie",
    "164": "meadville", "165": "meadville",
    "166": "altoona", "167": "williamsport",
    "168": "williamsport", "169": "williamsport",
    "170": "harrisburg", "171": "harrisburg", "172": "harrisburg",
    "173": "york", "174": "york", "175": "lancaster",
    "176": "york", "177": "pennstate",
    "178": "pennstate", "179": "pennstate",
    "180": "allentown", "181": "allentown", "182": "allentown",
    "183": "poconos", "184": "poconos", "185": "poconos",
    "186": "scranton", "187": "scranton",
    "188": "scranton", "189": "scranton",
    "190": "philadelphia", "191": "philadelphia",
    "192": "philadelphia", "193": "philadelphia",
    "194": "philadelphia", "195": "reading", "196": "reading",
    # ── Rhode Island ─────────────────────────────────────────
    "028": "providence", "029": "providence",
    # ── South Carolina ───────────────────────────────────────
    "290": "columbia", "291": "columbia", "292": "columbia",
    "293": "columbia", "294": "columbia",
    "295": "myrtlebeach", "296": "myrtlebeach",
    "297": "greenville", "298": "greenville",
    "299": "charleston",
    # ── South Dakota ─────────────────────────────────────────
    "570": "siouxfalls", "571": "siouxfalls",
    "572": "siouxfalls", "573": "siouxfalls",
    "574": "rapidcity", "575": "rapidcity",
    "576": "nesd", "577": "nesd",
    "578": "sd", "579": "sd",
    # ── Tennessee ────────────────────────────────────────────
    "370": "nashville", "371": "nashville", "372": "nashville",
    "373": "nashville", "374": "nashville", "375": "nashville",
    "376": "knoxville", "377": "knoxville", "378": "knoxville",
    "379": "knoxville",
    "380": "memphis", "381": "memphis", "382": "memphis", "383": "memphis",
    "384": "jacksontn", "385": "chattanooga", "386": "chattanooga",
    # ── Texas ────────────────────────────────────────────────
    "748": "wichitafalls",
    "750": "dallas", "751": "dallas", "752": "texoma",
    "753": "dallas", "754": "easttexas", "755": "dallas",
    "756": "nacogdoches", "757": "nacogdoches", "758": "nacogdoches",
    "759": "waco", "760": "waco", "761": "dallas",
    "762": "austin", "763": "austin", "764": "austin",
    "765": "killeen", "766": "killeen", "767": "killeen",
    "768": "waco", "769": "collegestation",
    "770": "houston", "771": "houston", "772": "houston",
    "773": "galveston", "774": "galveston", "775": "houston",
    "776": "houston", "777": "houston",
    "778": "beaumont", "779": "beaumont",
    "780": "sanantonio", "781": "sanantonio",
    "782": "sanantonio", "783": "sanantonio",
    "784": "corpuschristi", "785": "laredo",
    "786": "delrio", "787": "brownsville",
    "788": "mcallen", "789": "odessa",
    "790": "sanangelo", "791": "victoriatx",
    "792": "abilene", "793": "abilene", "794": "abilene",
    "795": "lubbock", "796": "lubbock", "797": "lubbock",
    "798": "elpaso", "799": "elpaso",
    # ── Utah ─────────────────────────────────────────────────
    "840": "saltlakecity", "841": "saltlakecity",
    "842": "saltlakecity", "843": "logan",
    "844": "ogden", "845": "ogden",
    "846": "provo", "847": "provo",
    "848": "stgeorge", "849": "stgeorge",
    # ── Vermont ──────────────────────────────────────────────
    "050": "vermont", "051": "vermont", "052": "vermont",
    "053": "vermont", "054": "vermont", "055": "vermont",
    "056": "vermont", "057": "vermont", "058": "vermont",
    "059": "vermont",
    # ── Virginia ─────────────────────────────────────────────
    "220": "washingtondc", "221": "washingtondc", "222": "washingtondc",
    "223": "fredericksburg", "224": "fredericksburg", "225": "fredericksburg",
    "226": "winchester", "227": "winchester",
    "228": "harrisonburg", "229": "harrisonburg",
    "230": "richmond", "231": "richmond", "232": "richmond",
    "233": "norfolk", "234": "norfolk", "235": "norfolk",
    "236": "norfolk", "237": "norfolk", "238": "richmond",
    "239": "norfolk",
    "240": "roanoke", "241": "roanoke", "242": "roanoke",
    "243": "roanoke", "244": "roanoke",
    "245": "lynchburg", "246": "danville",
    "247": "charlottesville",
    "248": "swva", "249": "swva",
    # ── Washington ───────────────────────────────────────────
    "980": "seattle", "981": "seattle", "982": "seattle",
    "983": "seattle", "984": "seattle", "985": "seattle",
    "986": "seattle",
    "988": "yakima", "989": "yakima",
    "990": "spokane", "991": "spokane", "992": "spokane",
    "993": "kpr", "994": "kpr",
    # ── West Virginia ────────────────────────────────────────
    "247": "huntington", "248": "huntington", "249": "huntington",
    "250": "charlestonwv", "251": "charlestonwv",
    "252": "charlestonwv", "253": "charlestonwv", "254": "charlestonwv",
    "255": "huntington", "256": "huntington",
    "257": "parkersburg", "258": "parkersburg", "259": "parkersburg",
    "260": "martinsburg", "261": "martinsburg", "262": "martinsburg",
    # ── Wisconsin ────────────────────────────────────────────
    "530": "milwaukee", "531": "milwaukee", "532": "milwaukee",
    "533": "milwaukee", "534": "milwaukee",
    "535": "racine", "536": "racine",
    "537": "madison", "538": "madison", "539": "madison",
    "540": "lacrosse", "541": "greenbay",
    "542": "greenbay", "543": "greenbay",
    "544": "appleton", "545": "rhinelander",
    "546": "lacrosse",
    "547": "eauclaire", "548": "eauclaire",
    "549": "janesville",
    # ── Wyoming ──────────────────────────────────────────────
    "820": "wyoming", "821": "wyoming", "822": "wyoming",
    "823": "wyoming", "824": "wyoming", "825": "wyoming",
    "826": "wyoming", "827": "wyoming", "828": "wyoming",
    "829": "wyoming", "830": "wyoming", "831": "wyoming",
}


class LocationResolutionError(Exception):
    pass


# ── Public entry point ────────────────────────────────────────

def resolve_location(location_type: str, location_value: str) -> dict:
    """
    Returns a dict with these keys:

        craigslist_cities     — list of CL region codes to scrape
        facebook_location_str — stored on every lead; includes ZIP when input
                                was a ZIP code, e.g. "Houston TX 77001"
        fb_search_location    — ZIP-free; used ONLY for FB group discovery
                                queries.  FB group search returns 0 results
                                when a ZIP is included, so we always strip it.
                                e.g. "Houston TX"
        display               — human-readable UI label
                                e.g. "Houston, TX (77001)"
        zip_code              — only present for ZIP searches
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
    stripped = value.strip()
    # Looks like a ZIP — delegate
    if stripped.isdigit() and len(stripped) in (5, 9):
        print(f"[LocationResolver] '{stripped}' looks like a ZIP — resolving as ZIP")
        return _resolve_by_zip(stripped[:5])

    # 2-letter abbreviation
    upper = value.upper()
    state_name = _STATE_ABBREV.get(upper)
    if state_name:
        return _build_state_result(state_name)

    # Full state name (case-insensitive)
    title = value.title()
    if title in _STATE_NAME_TO_CODES:
        return _build_state_result(title)

    for sname in _STATE_NAME_TO_CODES:
        if sname.lower() == value.lower():
            return _build_state_result(sname)

    # Maybe it's actually a city
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
    codes  = _STATE_NAME_TO_CODES[state_name]
    abbrev = _STATE_NAME_TO_ABBREV.get(state_name, "")
    fb_str = f"{state_name} {abbrev}".strip()
    return {
        "craigslist_cities":     codes,
        "facebook_location_str": fb_str,   # "Texas TX" — no ZIP for state
        "fb_search_location":    fb_str,   # same for state; no ZIP involved
        "display":               state_name,
    }


# ── Smart city resolver ───────────────────────────────────────

def _resolve_city_smart(value: str) -> dict:
    stripped = value.strip()
    # Looks like a ZIP — delegate
    if stripped.isdigit() and len(stripped) in (5, 9):
        print(f"[LocationResolver] '{stripped}' looks like a ZIP — resolving as ZIP")
        return _resolve_by_zip(stripped[:5])

    try:
        return _resolve_by_city(value)
    except LocationResolutionError:
        pass

    # Maybe a state abbreviation
    if len(value.strip()) == 2 and value.strip().upper() in _STATE_ABBREV:
        print(f"[LocationResolver] '{value}' looks like a state code — resolving as state")
        return _build_state_result(_STATE_ABBREV[value.strip().upper()])

    # Maybe a full state name
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
        matches = [
            (name, c)
            for name, c in _CITY_NAME_TO_CODE.items()
            if city_name.lower() in name
        ]
        if not matches:
            raise LocationResolutionError(
                f"City '{city_name}' not found in our coverage map."
            )
        matches.sort(key=lambda x: len(x[0]))
        _, code = matches[0]

    region     = _CODE_TO_REGION[code]
    state_name = region["state"]
    abbrev     = _STATE_NAME_TO_ABBREV.get(state_name, state_name)
    display    = f"{region['name']}, {abbrev}"

    return {
        "craigslist_cities":     [code],
        "facebook_location_str": display,   # "Houston, TX" — no ZIP for city
        "fb_search_location":    display,   # same for city; no ZIP involved
        "display":               display,
    }


# ── ZIP resolver ──────────────────────────────────────────────

def _resolve_by_zip(zip_code: str) -> dict:
    """
    Resolve a US ZIP code to a Craigslist region.

    Resolution priority (highest → lowest accuracy):
      1. Exact 5-digit match  (EXACT_ZIP_MAP)   — ~85% of US address volume
      2. 3-digit prefix match (ZIP_PREFIX_MAP)  — original behaviour
      3. 2-digit prefix scan                    — broad fallback
      4. First-digit → state                    — last resort

    Location strings returned:
      facebook_location_str  "Houston TX 77001"  stored on every lead
      fb_search_location     "Houston TX"         used only for FB group search
      display                "Houston, TX (77001)"
    """
    zip_code = zip_code.strip()

    if len(zip_code) < 3:
        raise LocationResolutionError("ZIP code too short.")

    zip5 = zip_code[:5].split("-")[0]
    if not zip5.isdigit():
        raise LocationResolutionError(
            f"'{zip_code}' doesn't look like a valid ZIP code."
        )

    prefix3 = zip5[:3]
    prefix2 = zip5[:2]

    # ── 1. Exact 5-digit match ────────────────────────────────
    code = EXACT_ZIP_MAP.get(zip5)
    if code:
        print(f"[LocationResolver] ZIP '{zip5}' exact match → '{code}'")

    # ── 2. 3-digit prefix ─────────────────────────────────────
    if not code:
        code = ZIP_PREFIX_MAP.get(prefix3)
        if code:
            print(
                f"[LocationResolver] ZIP '{zip5}' prefix-3 "
                f"('{prefix3}') → '{code}'"
            )

    # ── 3. 2-digit prefix scan ────────────────────────────────
    if not code:
        for p, c in ZIP_PREFIX_MAP.items():
            if p.startswith(prefix2):
                code = c
                print(
                    f"[LocationResolver] ZIP '{zip5}' prefix '{prefix3}' "
                    f"not mapped — using nearby prefix '{p}' → '{c}'"
                )
                break

    # ── 4. First-digit → state fallback ──────────────────────
    if not code:
        state_name = _ZIP_FIRST_DIGIT_TO_STATE.get(zip5[0])
        if state_name:
            print(
                f"[LocationResolver] ZIP '{zip5}' prefix not mapped — "
                f"falling back to state-wide scrape for {state_name}"
            )
            result = _build_state_result(state_name)
            abbrev = _STATE_NAME_TO_ABBREV.get(state_name, "")
            # ZIP included in fb_location_str for lead storage,
            # but stripped for fb_search_location (FB group search)
            result["facebook_location_str"] = (
                f"{state_name} {abbrev} {zip5}".strip()
            )
            result["fb_search_location"] = f"{state_name} {abbrev}".strip()
            result["display"]            = f"{state_name} ({zip5})"
            result["zip_code"]           = zip5
            return result

        raise LocationResolutionError(
            f"ZIP code '{zip_code}' could not be resolved. "
            f"Try searching by city or state instead."
        )

    region = _CODE_TO_REGION.get(code)
    if not region:
        raise LocationResolutionError(
            f"ZIP resolved to unknown CL region '{code}'."
        )

    state_name = region["state"]
    abbrev     = _STATE_NAME_TO_ABBREV.get(state_name, state_name)
    city_label = region["name"]

    # ── Location strings ──────────────────────────────────────
    # facebook_location_str  →  stored on every lead  →  includes ZIP
    #   e.g. "Houston TX 77001"
    # fb_search_location     →  FB group discovery only  →  no ZIP
    #   e.g. "Houston TX"
    # display                →  UI label
    #   e.g. "Houston, TX (77001)"
    facebook_location_str = f"{city_label} {abbrev} {zip5}"
    fb_search_location    = f"{city_label} {abbrev}"
    display               = f"{city_label}, {abbrev} ({zip5})"

    return {
        "craigslist_cities":     [code],
        "facebook_location_str": facebook_location_str,
        "fb_search_location":    fb_search_location,
        "display":               display,
        "zip_code":              zip5,
    }


# ── Utility ───────────────────────────────────────────────────

def get_all_city_codes() -> list[str]:
    return list(_CODE_TO_REGION.keys())