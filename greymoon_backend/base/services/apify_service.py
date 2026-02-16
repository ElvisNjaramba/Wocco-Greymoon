import requests
import time
from django.conf import settings

ACTOR_ID = "ivanvs~craigslist-scraper"

US_CITIES = [
    "auburn","bham","dothan","shoals","gadsden","huntsville","mobile","montgomery","tuscaloosa",
    "anchorage","fairbanks","kenai","juneau",
    "flagstaff","mohave","phoenix","prescott","showlow","sierravista","tucson","yuma",
    "fayar","fortsmith","jonesboro","littlerock","texarkana",
    "bakersfield","chico","fresno","goldcountry","hanford","humboldt","imperial","inlandempire",
    "losangeles","mendocino","merced","modesto","monterey","orangecounty","palmsprings",
    "redding","sacramento","sandiego","sfbay","slo","santabarbara","santamaria","stockton",
    "susanville","ventura","visalia","yubasutter",
    "boulder","coloradosprings","denver","eastco","fortcollins","rockies","pueblo","westslope",
    "newlondon","hartford","newhaven",
    "delaware",
    "daytona","fortlauderdale","fortmyers","gainesville","heartland","jacksonville","keys",
    "lakeland","miami","ocala","orlando","panamacity","pensacola","sarasota","spacecoast",
    "staugustine","tallahassee","tampa","treasurecoast",
    "albanyga","athensga","atlanta","augusta","brunswick","columbusga","macon","nwga",
    "savannah","statesboro","valdosta","warnerrobins",
    "honolulu",
    "boise","eastidaho","lewiston","twinfalls",
    "bn","chambana","chicago","decatur","lasalle","mattoon","peoria","rockford","carbondale","quincy",
    "bloomington","evansville","fortwayne","indianapolis","kokomo","muncie","richmondin",
    "southbend","terrehaute",
    "ames","cedarrapids","desmoines","dubuque","fortdodge","iowacity","masoncity",
    "quadcities","siouxcity","waterloo",
    "lawrence","ksu","nwks","salina","seks","swks","topeka","wichita",
    "bgky","eastky","lexington","louisville","owensboro","westky",
    "batonrouge","cenla","houma","lafayette","lakecharles","monroe","neworleans","shreveport",
    "maine",
    "annapolis","baltimore","easternshore","frederick","smd",
    "boston","capecod","southcoast","worcester",
    "annarbor","battlecreek","centralmich","detroit","flint","grandrapids","holland","jxn",
    "kalamazoo","lansing","monroemi","muskegon","nmi","porthuron","saginaw","swmi","thumb",
    "bemidji","brainerd","duluth","mankato","minneapolis","rmn","stcloud",
    "gulfport","hattiesburg","jackson","meridian","northmiss","southmiss",
    "columbiamo","joplin","kansascity","kirksville","loz","semo","springfield","stjoseph","stlouis",
    "billings","bozeman","butte","greatfalls","helena","kalispell","missoula",
    "grandisland","lincoln","northplatte","omaha","scottsbluff",
    "elko","lasvegas","reno",
    "nh",
    "cnj","jerseyshore","newjersey","southjersey",
    "albuquerque","clovis","farmington","lascruces","roswell","santafe",
    "albany","binghamton","buffalo","catskills","chautauqua","elmira","fingerlakes",
    "glensfalls","hudsonvalley","ithaca","longisland","newyork","oneonta","plattsburgh",
    "potsdam","rochester","syracuse","utica","watertown",
    "asheville","boone","charlotte","eastnc","fayetteville","greensboro","hickory",
    "onslow","outerbanks","raleigh","wilmington","winstonsalem",
    "bismarck","fargo","grandforks","nd",
    "akroncanton","ashtabula","athensohio","chillicothe","cincinnati","cleveland",
    "columbus","dayton","limaohio","mansfield","sandusky","toledo","tuscarawas",
    "youngstown","zanesville",
    "lawton","enid","oklahomacity","stillwater","tulsa",
    "bend","corvallis","eastoregon","eugene","klamath","medford","oregoncoast",
    "portland","roseburg","salem",
    "altoona","chambersburg","erie","harrisburg","lancaster","allentown","meadville",
    "philadelphia","pittsburgh","poconos","reading","scranton","pennstate",
    "williamsport","york",
    "providence",
    "charlestonwv","martinsburg","huntington","parkersburg",
    "charleston","columbia","florencesc","greenville","hiltonhead","myrtlebeach",
    "nesd","rapidcity","siouxfalls","sd",
    "chattanooga","clarksville","cookeville","jackson","knoxville",
    "memphis","nashville","tricities"
]


CATEGORIES = ["hss", "sks", "lbs", "fgs", "trd", "aos"]


# ---------- CONFIG ----------
MAX_CITIES_PER_RUN = 3   # VERY IMPORTANT (prevents mass blocking)
POLL_INTERVAL = 5        # seconds
COOLDOWN_BETWEEN_RUNS = 20  # seconds
# ----------------------------


def chunk_list(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


# def build_urls_payload(cities_batch=None):
#     urls = []

#     # If no batch provided → use full list
#     if cities_batch is None:
#         cities_batch = US_CITIES

#     for city in cities_batch:
#         for category in CATEGORIES:
#             urls.append({
#                 "url": f"https://{city}.craigslist.org/search/{category}"
#             })

#     payload = {
#         "urls": urls,
#         "maxAge": 15,
#         "maxConcurrency": 1,
#         "proxyConfiguration": {
#             "useApifyProxy": True,
#             "apifyProxyGroups": ["RESIDENTIAL"],
#             "apifyProxyCountry": "US"
#         }
#     }

#     return payload


def build_urls_payload(cities_batch):
    urls = []

    for city in cities_batch:
        for category in CATEGORIES:
            urls.append({
                "url": f"https://{city}.craigslist.org/search/{category}"
            })

    payload = {
        "urls": urls,
        "maxAge": 15,
        "maxConcurrency": 1,
        "proxyConfiguration": {
            "useApifyProxy": True,
            "apifyProxyGroups": ["RESIDENTIAL"],
            "apifyProxyCountry": "US"
        }
    }

    return payload

# def run_actor():
#     payload = build_urls_payload()

#     url = f"https://api.apify.com/v2/acts/{ACTOR_ID}/runs"

#     headers = {
#         "Authorization": f"Bearer {settings.APIFY_TOKEN}"
#     }

#     response = requests.post(url, json=payload, headers=headers)
#     response.raise_for_status()

#     data = response.json()["data"]
#     return data["id"], data["defaultDatasetId"]

def run_actor(selected_cities):
    payload = build_urls_payload(selected_cities)

    url = f"https://api.apify.com/v2/acts/{ACTOR_ID}/runs"

    headers = {
        "Authorization": f"Bearer {settings.APIFY_TOKEN}"
    }

    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()

    data = response.json()["data"]
    return data["id"], data["defaultDatasetId"]


def wait_for_finish(run_id):
    url = f"https://api.apify.com/v2/actor-runs/{run_id}"

    headers = {
        "Authorization": f"Bearer {settings.APIFY_TOKEN}"
    }

    while True:
        res = requests.get(url, headers=headers)
        res.raise_for_status()

        data = res.json()["data"]
        status = data["status"]

        print("Actor status:", status)

        if status == "SUCCEEDED":
            return

        elif status in ["FAILED", "ABORTED", "TIMED-OUT"]:
            raise Exception(f"Scraper failed with status {status}")

        time.sleep(POLL_INTERVAL)


def fetch_results(dataset_id):
    dataset_url = (
        f"https://api.apify.com/v2/datasets/"
        f"{dataset_id}/items?clean=true&limit=1000"
    )

    headers = {
        "Authorization": f"Bearer {settings.APIFY_TOKEN}"
    }

    response = requests.get(dataset_url, headers=headers)
    response.raise_for_status()

    data = response.json()
    print("Fetched dataset count:", len(data))

    return data


# def scrape_all():
#     all_results = []

#     for cities_batch in chunk_list(US_CITIES, MAX_CITIES_PER_RUN):
#         print("Running batch:", cities_batch)

#         payload = build_urls_payload(cities_batch)
#         run_id, dataset_id = run_actor(payload)

#         wait_for_finish(run_id)

#         results = fetch_results(dataset_id)
#         all_results.extend(results)

#         print("Cooling down...")
#         time.sleep(COOLDOWN_BETWEEN_RUNS)

#     return all_results


def scrape_all():
    all_results = []

    for cities_batch in chunk_list(US_CITIES, MAX_CITIES_PER_RUN):
        print("Running batch:", cities_batch)

        run_id, dataset_id = run_actor(cities_batch)

        wait_for_finish(run_id)

        results = fetch_results(dataset_id)
        all_results.extend(results)

        print("Cooling down...")
        time.sleep(COOLDOWN_BETWEEN_RUNS)

    return all_results
