import requests
import time
from django.conf import settings

ACTOR_ID = "ivanvs~craigslist-scraper"

US_CITIES = [
    "newyork",
    "losangeles",
    "chicago",
    "houston",
    "phoenix",
    "miami",
    "dallas",
    "atlanta"
]

CATEGORIES = ["hss", "lss"]

def build_urls_payload():
    urls = []
    for city in US_CITIES:
        for category in CATEGORIES:
            urls.append({"url": f"https://{city}.craigslist.org/search/{category}"})
    payload = {
        "urls": urls,
        "maxAge": 15,
        "maxConcurrency": 4,
        "proxyConfiguration": {"useApifyProxy": True}
    }
    return payload

def run_actor():
    url = f"https://api.apify.com/v2/acts/{ACTOR_ID}/runs"

    headers = {
        "Authorization": f"Bearer {settings.APIFY_TOKEN}"
    }

    payload = build_urls_payload()

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
        data = res.json()["data"]
        status = data["status"]

        print("Actor status:", status)

        if status == "SUCCEEDED":
            print("Actor succeeded")
            break

        elif status in ["FAILED", "ABORTED", "TIMED-OUT"]:
            print("Actor failed:", status)
            raise Exception(f"Scraper failed with status {status}")

        time.sleep(3)


def fetch_results(run_id):
    run_url = f"https://api.apify.com/v2/actor-runs/{run_id}"

    headers = {
        "Authorization": f"Bearer {settings.APIFY_TOKEN}"
    }

    run_response = requests.get(run_url, headers=headers)
    run_data = run_response.json()["data"]

    dataset_id = run_data.get("defaultDatasetId")

    if not dataset_id:
        raise Exception("No dataset found for this run")

    dataset_url = f"https://api.apify.com/v2/datasets/{dataset_id}/items?clean=true&limit=1000"

    response = requests.get(dataset_url, headers=headers)
    response.raise_for_status()

    data = response.json()
    print("Fetched dataset count:", len(data))

    return data
