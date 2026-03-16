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

# ── Expanded ZIP Prefix Map ───────────────────────────────────
#
# Keys are 3-digit ZIP prefixes (first 3 digits of a 5-digit ZIP).
# Values are Craigslist city codes from US_CITY_STRUCTURE.
# Coverage: all 50 states, ~900 prefix entries.
#
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
    "349": "orlando", "350": "spacecoast", "351": "spacecoast",
    "352": "ocala", "353": "ocala", "354": "lakeland",
    "355": "lakeland", "356": "staugustine", "357": "staugustine",
    "358": "pensacola", "359": "pensacola",
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
    "836": "boise", "837": "boise", "838": "boise",
    "832": "twinfalls", "834": "twinfalls",
    "835": "eastidaho", "836": "eastidaho", "837": "eastidaho",
    "838": "lewiston",

    # ── Illinois ─────────────────────────────────────────────
    "600": "chicago", "601": "chicago", "602": "chicago",
    "603": "chicago", "604": "chicago", "605": "chicago",
    "606": "chicago", "607": "chicago", "608": "chicago",
    "609": "chicago", "610": "rockford", "611": "rockford",
    "612": "rockford", "613": "peoria", "614": "peoria",
    "615": "peoria", "616": "peoria", "617": "bloomington",  # IL not MA
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
    "206": "washingtondc",  # suburban MD near DC
    "207": "washingtondc", "208": "washingtondc",
    "209": "washingtondc",
    "210": "baltimore", "211": "baltimore",
    "212": "baltimore", "213": "baltimore", "214": "baltimore",
    "215": "baltimore", "216": "baltimore",
    "217": "annapolis", "218": "annapolis",
    "219": "easternshore",
    "206": "smd", "220": "fredericksburg",
    "221": "fredericksburg",

    # ── Massachusetts ────────────────────────────────────────
    "010": "worcester", "011": "worcester", "012": "worcester",
    "013": "worcester", "014": "worcester",
    "015": "worcester", "016": "worcester",
    "017": "worcester", "018": "boston",
    "019": "boston", "020": "boston", "021": "boston",
    "022": "boston", "023": "boston", "024": "boston",
    "025": "capecod", "026": "capecod", "027": "capecod",
    "028": "southcoast", "029": "southcoast",

    # ── Michigan ─────────────────────────────────────────────
    "480": "detroit", "481": "detroit", "482": "detroit",
    "483": "detroit", "484": "detroit", "485": "detroit",
    "486": "flint", "487": "flint", "488": "flint",
    "489": "flint", "490": "kalamazoo", "491": "kalamazoo",
    "492": "kalamazoo", "493": "grandrapids",
    "494": "grandrapids", "495": "grandrapids",
    "496": "grandrapids", "497": "lansing",
    "498": "lansing", "499": "saginaw",
    "488": "annarbor", "489": "annarbor",
    "490": "battlecreek",
    "491": "southbend",  # border region
    "493": "holland", "494": "muskegon",
    "495": "swmi", "496": "centralmich",
    "497": "nmi", "498": "up", "499": "up",

    # ── Minnesota ────────────────────────────────────────────
    "550": "minneapolis", "551": "minneapolis",
    "552": "minneapolis", "553": "minneapolis",
    "554": "minneapolis", "555": "minneapolis",
    "556": "duluth", "557": "duluth", "558": "duluth",
    "559": "rmn", "560": "mankato",
    "561": "mankato", "562": "stcloud",
    "563": "stcloud", "564": "brainerd",
    "565": "bemidji", "566": "bemidji",
    "567": "brainerd",

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
    "593": "bozeman", "594": "greatfalls",
    "595": "greatfalls", "596": "missoula",
    "597": "missoula", "598": "kalispell",
    "599": "kalispell", "596": "helena",
    "597": "butte",

    # ── Nebraska ─────────────────────────────────────────────
    "680": "omaha", "681": "omaha", "682": "omaha",
    "683": "lincoln", "684": "lincoln", "685": "lincoln",
    "686": "grandisland", "687": "grandisland",
    "688": "northplatte", "689": "scottsbluff",
    "690": "scottsbluff", "691": "scottsbluff",
    "692": "northplatte",

    # ── Nevada ───────────────────────────────────────────────
    "889": "lasvegas", "890": "lasvegas", "891": "lasvegas",
    "893": "lasvegas", "894": "reno", "895": "reno",
    "897": "reno", "898": "reno", "893": "elko",

    # ── New Hampshire ────────────────────────────────────────
    "030": "nh", "031": "nh", "032": "nh",
    "033": "nh", "034": "nh", "035": "nh",
    "036": "nh", "037": "nh", "038": "nh",

    # ── New Jersey ───────────────────────────────────────────
    "070": "newjersey", "071": "newjersey", "072": "newjersey",
    "073": "newjersey", "074": "newjersey", "075": "newjersey",
    "076": "newjersey", "077": "jerseyshore",
    "078": "newjersey", "079": "newjersey",
    "080": "southjersey", "081": "southjersey",
    "082": "southjersey", "083": "southjersey",
    "084": "southjersey", "085": "cnj",
    "086": "cnj", "087": "cnj", "088": "cnj",
    "089": "cnj",

    # ── New Mexico ───────────────────────────────────────────
    "870": "albuquerque", "871": "albuquerque",
    "872": "albuquerque", "873": "albuquerque",
    "874": "santafe", "875": "santafe",
    "876": "farmington", "877": "farmington",
    "878": "lascruces", "879": "lascruces",
    "880": "lascruces", "881": "roswell",
    "882": "roswell", "883": "clovis",

    # ── New York ─────────────────────────────────────────────
    "005": "longisland",  # 005xx — Holtsville/Long Island NY (e.g. IRS 00501)
    "100": "newyork", "101": "newyork", "102": "newyork",
    "103": "newyork", "104": "newyork",
    "105": "hudsonvalley", "106": "hudsonvalley",
    "107": "hudsonvalley", "108": "hudsonvalley",
    "109": "hudsonvalley",
    "110": "longisland", "111": "longisland",
    "112": "longisland", "113": "longisland",
    "114": "longisland", "115": "longisland",
    "116": "longisland", "117": "longisland",
    "118": "longisland", "119": "longisland",
    "120": "albany", "121": "albany", "122": "albany",
    "123": "albany", "124": "catskills",
    "125": "catskills", "126": "catskills",
    "127": "catskills",
    "128": "plattsburgh", "129": "plattsburgh",
    "130": "syracuse", "131": "syracuse", "132": "syracuse",
    "133": "utica", "134": "utica", "135": "utica",
    "136": "utica", "137": "binghamton",
    "138": "binghamton", "139": "binghamton",
    "140": "buffalo", "141": "buffalo", "142": "buffalo",
    "143": "buffalo", "144": "rochester",
    "145": "rochester", "146": "rochester",
    "147": "ithaca", "148": "ithaca",
    "149": "elmira", "140": "chautauqua",
    "147": "fingerlakes", "148": "glensfalls",
    "136": "oneonta", "137": "potsdam",
    "138": "watertown",

    # ── North Carolina ───────────────────────────────────────
    "270": "greensboro", "271": "greensboro", "272": "greensboro",
    "273": "greensboro", "274": "greensboro",
    "275": "raleigh", "276": "raleigh", "277": "raleigh",
    "278": "raleigh", "279": "raleigh",
    "280": "charlotte", "281": "charlotte", "282": "charlotte",
    "283": "charlotte", "284": "charlotte",
    "285": "onslow", "286": "hickory",
    "287": "winstonsalem", "288": "winstonsalem",
    "289": "winstonsalem",
    "274": "eastnc", "275": "eastnc",
    "285": "outerbanks",
    "276": "boone",
    "284": "wilmington",
    "286": "asheville", "287": "asheville", "288": "asheville",
    "289": "fayetteville",

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
    "458": "lima", "459": "limaohio",
    "440": "ashtabula", "441": "sandusky",
    "442": "chillicothe", "443": "athensohio",
    "444": "tuscarawas",

    # ── Oklahoma ─────────────────────────────────────────────
    "730": "oklahomacity", "731": "oklahomacity",
    "732": "oklahomacity", "733": "oklahomacity",
    "734": "oklahomacity", "735": "lawton",
    "736": "enid", "737": "enid",
    "738": "enid", "739": "enid",
    "740": "tulsa", "741": "tulsa",
    "742": "tulsa", "743": "tulsa",
    "744": "tulsa", "745": "tulsa",
    "746": "stillwater", "747": "stillwater",

    # ── Oregon ───────────────────────────────────────────────
    "970": "portland", "971": "portland", "972": "portland",
    "973": "portland", "974": "portland",
    "975": "salem", "976": "salem",
    "977": "eugene", "978": "eugene",
    "979": "corvallis", "980": "corvallis",
    "971": "bend", "972": "klamath",
    "973": "medford", "974": "roseburg",
    "975": "oregoncoast",
    "976": "eastoregon",

    # ── Pennsylvania ─────────────────────────────────────────
    "150": "pittsburgh", "151": "pittsburgh", "152": "pittsburgh",
    "153": "pittsburgh", "154": "pittsburgh",
    "155": "pittsburgh", "156": "pittsburgh",
    "157": "pittsburgh", "158": "altoona",
    "159": "altoona", "160": "meadville",
    "161": "erie", "162": "erie",
    "163": "erie", "164": "meadville",
    "165": "meadville",
    "166": "altoona", "167": "williamsport",
    "168": "williamsport", "169": "williamsport",
    "170": "harrisburg", "171": "harrisburg",
    "172": "harrisburg", "173": "york",
    "174": "york", "175": "york",
    "176": "york", "177": "pennstate",
    "178": "pennstate", "179": "pennstate",
    "180": "allentown", "181": "allentown",
    "182": "allentown", "183": "poconos",
    "184": "poconos", "185": "poconos",
    "186": "scranton", "187": "scranton",
    "188": "scranton", "189": "scranton",
    "190": "philadelphia", "191": "philadelphia",
    "192": "philadelphia", "193": "philadelphia",
    "194": "philadelphia", "195": "reading",
    "196": "reading", "174": "chambersburg",
    "175": "lancaster",

    # ── Rhode Island ─────────────────────────────────────────
    "028": "providence", "029": "providence",

    # ── South Carolina ───────────────────────────────────────
    "290": "columbia", "291": "columbia", "292": "columbia",
    "293": "columbia", "294": "columbia",
    "295": "florencesc", "296": "florencesc",
    "297": "greenville", "298": "greenville",
    "299": "charleston",
    "295": "myrtlebeach", "296": "myrtlebeach",
    "299": "hiltonhead",

    # ── South Dakota ─────────────────────────────────────────
    "570": "siouxfalls", "571": "siouxfalls",
    "572": "siouxfalls", "573": "siouxfalls",
    "574": "rapidcity", "575": "rapidcity",
    "576": "nesd", "577": "nesd",
    "578": "sd", "579": "sd",

    # ── Tennessee ────────────────────────────────────────────
    "370": "nashville", "371": "nashville", "372": "nashville",
    "373": "nashville", "374": "nashville",
    "375": "nashville",
    "376": "knoxville", "377": "knoxville", "378": "knoxville",
    "379": "knoxville",
    "380": "memphis", "381": "memphis", "382": "memphis",
    "383": "memphis",
    "384": "jacksontn",
    "385": "jacksontn",
    "370": "clarksville",
    "376": "tricities", "377": "tricities",
    "385": "chattanooga", "386": "chattanooga",
    "372": "cookeville",

    # ── Texas ────────────────────────────────────────────────
    "750": "dallas", "751": "dallas", "752": "dallas",
    "753": "dallas", "754": "dallas", "755": "dallas",
    "756": "nacogdoches", "757": "nacogdoches",
    "758": "nacogdoches",
    "759": "waco", "760": "waco", "761": "fortworth",
    "762": "austin",
    "763": "austin", "764": "austin",
    "765": "killeen", "766": "killeen", "767": "killeen",
    "768": "waco",
    "769": "collegestation",
    "770": "houston", "771": "houston", "772": "houston",
    "773": "houston", "774": "houston", "775": "houston",
    "776": "houston", "777": "houston",
    "778": "beaumont", "779": "beaumont",
    "780": "sanantonio", "781": "sanantonio",
    "782": "sanantonio", "783": "sanantonio",
    "784": "corpuschristi", "785": "corpuschristi",
    "786": "austin", "787": "austin",
    "788": "sanantonio",
    "789": "sanmarcos",
    "790": "amarillo", "791": "amarillo",
    "792": "abilene", "793": "abilene",
    "794": "abilene",
    "795": "lubbock", "796": "lubbock", "797": "lubbock",
    "798": "elpaso", "799": "elpaso",
    "748": "wichitafalls",
    "752": "texoma",
    "754": "easttexas",
    "756": "nacogdoches",
    "757": "nacogdoches",
    "773": "galveston",
    "774": "galveston",
    "785": "laredo",
    "786": "delrio",
    "787": "brownsville",
    "788": "mcallen",
    "789": "odessa",
    "790": "sanangelo",
    "791": "victoriatx",

    # ── Utah ─────────────────────────────────────────────────
    "840": "saltlakecity", "841": "saltlakecity",
    "842": "saltlakecity", "843": "saltlakecity",
    "844": "ogden", "845": "ogden",
    "846": "provo", "847": "provo",
    "848": "stgeorge", "849": "stgeorge",
    "843": "logan",

    # ── Vermont ──────────────────────────────────────────────
    "050": "vermont", "051": "vermont", "052": "vermont",
    "053": "vermont", "054": "vermont", "055": "vermont",
    "056": "vermont", "057": "vermont", "058": "vermont",
    "059": "vermont",

    # ── Virginia ─────────────────────────────────────────────
    "220": "washingtondc",  # NoVA
    "221": "washingtondc", "222": "washingtondc",
    "223": "fredericksburg", "224": "fredericksburg",
    "225": "fredericksburg",
    "226": "winchester", "227": "winchester",
    "228": "harrisonburg", "229": "harrisonburg",
    "230": "richmond", "231": "richmond", "232": "richmond",
    "233": "norfolk", "234": "norfolk",
    "235": "norfolk", "236": "norfolk",
    "237": "norfolk", "238": "richmond",
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
    "982": "bellingham", "983": "skagit",
    "984": "olympic", "985": "pullman",
    "988": "moseslake", "989": "wenatchee",

    # ── West Virginia ────────────────────────────────────────
    "247": "huntington", "248": "huntington",
    "249": "huntington",
    "250": "charlestonwv", "251": "charlestonwv",
    "252": "charlestonwv", "253": "charlestonwv",
    "254": "charlestonwv",
    "255": "huntington", "256": "huntington",
    "257": "parkersburg", "258": "parkersburg",
    "259": "parkersburg",
    "260": "martinsburg", "261": "martinsburg",
    "262": "martinsburg",

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
    "535": "sheboygan", "536": "wausau",

    # ── Wyoming ──────────────────────────────────────────────
    "820": "wyoming", "821": "wyoming", "822": "wyoming",
    "823": "wyoming", "824": "wyoming", "825": "wyoming",
    "826": "wyoming", "827": "wyoming", "828": "wyoming",
    "829": "wyoming", "830": "wyoming", "831": "wyoming",
}


class LocationResolutionError(Exception):
    pass


def resolve_location(location_type: str, location_value: str) -> dict:
    """
    Returns:
        {
            "craigslist_cities": ["houston", "galveston"],
            "facebook_location_str": "Houston TX 77001",  ← ZIP included if available
            "display": "Houston, TX (77001)"
        }

    Raises LocationResolutionError if nothing matches.
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
    # 0. Looks like a ZIP code?
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

    for sname in _STATE_NAME_TO_CODES:
        if sname.lower() == value.lower():
            return _build_state_result(sname)

    # 3. Might be a city typed into the state field
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
    # 0. Looks like a ZIP code?
    stripped = value.strip()
    if stripped.isdigit() and len(stripped) in (5, 9):
        print(f"[LocationResolver] '{stripped}' looks like a ZIP — resolving as ZIP")
        return _resolve_by_zip(stripped[:5])

    # 1. Exact or fuzzy city match
    try:
        return _resolve_by_city(value)
    except LocationResolutionError:
        pass

    # 2. Looks like a 2-letter state code?
    if len(value.strip()) == 2 and value.strip().upper() in _STATE_ABBREV:
        print(f"[LocationResolver] '{value}' looks like a state code — resolving as state")
        return _build_state_result(_STATE_ABBREV[value.strip().upper()])

    # 3. Full state name in the city box
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

    region = _CODE_TO_REGION[code]
    state_name = region["state"]
    abbrev = _STATE_NAME_TO_ABBREV.get(state_name, state_name)
    display = f"{region['name']}, {abbrev}"

    return {
        "craigslist_cities": [code],
        "facebook_location_str": display,
        "display": display,
    }


# ── ZIP first-digit → state fallback ─────────────────────────
#
# US ZIPs are geographically ordered. When a 3-digit prefix isn't
# in ZIP_PREFIX_MAP we fall back to a whole-state scrape based on
# the first digit, which always maps to a known geographic region.
#
_ZIP_FIRST_DIGIT_TO_STATE = {
    "0": "New York",          # 0xx — Northeast (NJ/NY/CT/MA/etc); NY is safest default
    "1": "New York",          # 1xx — NY, PA
    "2": "Virginia",          # 2xx — DC, MD, VA, WV, NC
    "3": "Florida",           # 3xx — FL, GA, AL, MS, TN, SC
    "4": "Indiana",           # 4xx — OH, IN, KY, MI
    "5": "Iowa",              # 5xx — IA, WI, MN
    "6": "Illinois",          # 6xx — IL, MO, KS, NE
    "7": "Texas",             # 7xx — TX, OK, LA, AR
    "8": "Colorado",          # 8xx — CO, UT, AZ, NM, NV, WY, ID, MT
    "9": "California",        # 9xx — CA, OR, WA, AK, HI
}


# ── ZIP resolver ──────────────────────────────────────────────

def _resolve_by_zip(zip_code: str) -> dict:
    zip_code = zip_code.strip()

    if len(zip_code) < 3:
        raise LocationResolutionError("ZIP code too short.")

    # Normalise: handle ZIP+4 (e.g. "77001-1234" → "77001")
    zip5 = zip_code[:5].split("-")[0]
    if not zip5.isdigit():
        raise LocationResolutionError(f"'{zip_code}' doesn't look like a valid ZIP code.")

    prefix3 = zip5[:3]
    prefix2 = zip5[:2]

    # ── 1. 3-digit prefix lookup ──────────────────────────────
    code = ZIP_PREFIX_MAP.get(prefix3)

    # ── 2. 2-digit prefix scan (fills gaps like 005xx) ────────
    if not code:
        for p, c in ZIP_PREFIX_MAP.items():
            if p.startswith(prefix2):
                code = c
                print(f"[LocationResolver] ZIP '{zip5}' prefix '{prefix3}' not mapped — "
                      f"using nearby prefix '{p}' → '{c}'")
                break

    # ── 3. First-digit state fallback ─────────────────────────
    if not code:
        state_name = _ZIP_FIRST_DIGIT_TO_STATE.get(zip5[0])
        if state_name:
            print(f"[LocationResolver] ZIP '{zip5}' prefix not mapped — "
                  f"falling back to state-wide scrape for {state_name}")
            result = _build_state_result(state_name)
            # Still include the ZIP in the FB search string and display
            abbrev = _STATE_NAME_TO_ABBREV.get(state_name, "")
            result["facebook_location_str"] = f"{state_name} {abbrev} {zip5}".strip()
            result["display"] = f"{state_name} ({zip5})"
            result["zip_code"] = zip5
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
    abbrev = _STATE_NAME_TO_ABBREV.get(state_name, state_name)
    display = f"{region['name']}, {abbrev} ({zip5})"
    fb_location_str = f"{region['name']} {abbrev} {zip5}"

    return {
        "craigslist_cities": [code],
        "facebook_location_str": fb_location_str,   # e.g. "Houston TX 77001"
        "display": display,                           # e.g. "Houston, TX (77001)"
        "zip_code": zip5,                             # pass-through for pipeline use
    }


# ── Utility ───────────────────────────────────────────────────

def get_all_city_codes() -> list[str]:
    return list(_CODE_TO_REGION.keys())