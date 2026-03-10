import requests
import time
from django.conf import settings

ACTOR_ID = "ivanvs~craigslist-scraper"

POLL_INTERVAL = 5          # seconds between status checks
COOLDOWN_BETWEEN_RUNS = 20  # seconds between city batches
MAX_CITIES_PER_RUN = 3     # keeps us under Apify's radar


def chunk_list(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def _apify_headers():
    return {"Authorization": f"Bearer {settings.APIFY_TOKEN}"}


def build_craigslist_payload(cities: list[str], category_codes: list[str]) -> dict:
    """
    Build the actor input payload for a batch of cities + category codes.
    Each city × category = one URL to scrape.
    """
    urls = [
        {"url": f"https://{city}.craigslist.org/search/{code}"}
        for city in cities
        for code in category_codes
    ]
    return {
        "urls": urls,
        "maxAge": 15,
        "maxConcurrency": 1,
        "proxyConfiguration": {
            "useApifyProxy": True,
            "apifyProxyGroups": ["RESIDENTIAL"],
            "apifyProxyCountry": "US",
        },
    }


def start_craigslist_run(cities: list[str], category_codes: list[str]) -> tuple[str, str]:
    """
    Kick off one actor run for a batch of cities.
    Returns (run_id, dataset_id).
    """
    payload = build_craigslist_payload(cities, category_codes)
    url = f"https://api.apify.com/v2/acts/{ACTOR_ID}/runs"
    resp = requests.post(url, json=payload, headers=_apify_headers())
    resp.raise_for_status()
    data = resp.json()["data"]
    return data["id"], data["defaultDatasetId"]


def wait_for_run(run_id: str, source_label: str = "Craigslist"):
    """Poll until actor finishes. Raises on failure."""
    url = f"https://api.apify.com/v2/actor-runs/{run_id}"
    while True:
        res = requests.get(url, headers=_apify_headers())
        res.raise_for_status()
        status = res.json()["data"]["status"]
        print(f"[{source_label}] Actor status: {status}")
        if status == "SUCCEEDED":
            return
        if status in ["FAILED", "ABORTED", "TIMED-OUT"]:
            raise Exception(f"[{source_label}] Actor run {run_id} ended with: {status}")
        time.sleep(POLL_INTERVAL)


def fetch_dataset(dataset_id: str, limit: int = 1000) -> list[dict]:
    """Pull all results from a completed dataset."""
    url = (
        f"https://api.apify.com/v2/datasets/{dataset_id}"
        f"/items?clean=true&limit={limit}"
    )
    resp = requests.get(url, headers=_apify_headers())
    resp.raise_for_status()
    return resp.json()


# ── Progressive generator (used by pipeline.py) ───────────────────────────────

def scrape_craigslist_progressive(cities: list[str], category_codes: list[str]):
    """
    Generator version of scrape_craigslist.

    Yields one list of raw items per city batch so the pipeline can save
    results to the database immediately — before the next batch starts.
    This means data is never lost if the run is aborted mid-way.

    Usage:
        for batch in scrape_craigslist_progressive(cities, codes):
            save_to_db(batch)
    """
    for batch in chunk_list(cities, MAX_CITIES_PER_RUN):
        print(f"[Craigslist] Scraping batch: {batch} | categories: {category_codes}")
        run_id, dataset_id = start_craigslist_run(batch, category_codes)

        try:
            wait_for_run(run_id)
        except Exception as e:
            # Actor failed/aborted — fetch whatever it managed to collect
            # before dying and yield that partial data.
            print(f"[Craigslist] Run ended early ({e}), fetching partial dataset...")
            try:
                partial = fetch_dataset(dataset_id)
                if partial:
                    print(f"[Craigslist] Yielding {len(partial)} partial results")
                    yield partial
            except Exception as fetch_err:
                print(f"[Craigslist] Could not fetch partial dataset: {fetch_err}")
            continue  # move on to next batch

        results = fetch_dataset(dataset_id)
        print(f"[Craigslist] Got {len(results)} results from batch")
        yield results

        print(f"[Craigslist] Cooling down {COOLDOWN_BETWEEN_RUNS}s...")
        time.sleep(COOLDOWN_BETWEEN_RUNS)


# ── Legacy non-generator version (kept for backwards compat) ──────────────────

def scrape_craigslist(cities: list[str], category_codes: list[str]) -> list[dict]:
    """
    Full Craigslist scrape: batch cities, run actor, collect results.
    Returns raw items — normalization happens in the pipeline.
    """
    all_results = []
    for batch in scrape_craigslist_progressive(cities, category_codes):
        all_results.extend(batch)
    return all_results