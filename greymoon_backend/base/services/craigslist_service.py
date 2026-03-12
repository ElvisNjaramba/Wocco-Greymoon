import time
import requests
from django.conf import settings

ACTOR_ID = "ivanvs~craigslist-scraper"

POLL_INTERVAL = 5           # seconds between status checks
COOLDOWN_BETWEEN_RUNS = 20  # seconds between city batches
MAX_CITIES_PER_RUN = 3      # keeps us under Apify's radar


def chunk_list(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def _apify_headers():
    return {"Authorization": f"Bearer {settings.APIFY_TOKEN}"}


def build_craigslist_payload(cities: list[str], category_codes: list[str]) -> dict:
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
    """Kick off one actor run. Returns (apify_run_id, dataset_id)."""
    payload = build_craigslist_payload(cities, category_codes)
    url = f"https://api.apify.com/v2/acts/{ACTOR_ID}/runs"
    resp = requests.post(url, json=payload, headers=_apify_headers())
    resp.raise_for_status()
    data = resp.json()["data"]
    return data["id"], data["defaultDatasetId"]


def _register_apify_run(scrape_run_id: int | None, apify_run_id: str):
    """
    Store the Apify run ID on ScrapeRun so cancel_scrape can abort it.
    Uses an atomic append via F() to avoid race conditions.
    """
    if not scrape_run_id:
        return
    try:
        from base.models import ScrapeRun
        from django.db.models import F
        run = ScrapeRun.objects.filter(pk=scrape_run_id).first()
        if run:
            ids = run.apify_run_ids or []
            if apify_run_id not in ids:
                ids.append(apify_run_id)
            ScrapeRun.objects.filter(pk=scrape_run_id).update(apify_run_ids=ids)
    except Exception as e:
        print(f"[Craigslist] Could not register Apify run ID: {e}")


def _is_cancel_requested(scrape_run_id: int | None) -> bool:
    if not scrape_run_id:
        return False
    try:
        from base.models import ScrapeRun
        return ScrapeRun.objects.filter(pk=scrape_run_id, cancel_requested=True).exists()
    except Exception:
        return False


def wait_for_run(run_id: str, source_label: str = "Craigslist", scrape_run_id=None):
    """Poll until actor finishes. Raises on failure or cancellation."""
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
        # Check if user hit Stop between polls
        if _is_cancel_requested(scrape_run_id):
            raise Exception(f"[{source_label}] Cancelled by user")
        time.sleep(POLL_INTERVAL)


def fetch_dataset(dataset_id: str, limit: int = 1000) -> list[dict]:
    url = (
        f"https://api.apify.com/v2/datasets/{dataset_id}"
        f"/items?clean=true&limit={limit}"
    )
    resp = requests.get(url, headers=_apify_headers())
    resp.raise_for_status()
    return resp.json()


def scrape_craigslist_progressive(
    cities: list[str],
    category_codes: list[str],
    scrape_run_id=None,
):
    """
    Generator — yields one list of raw items per city batch.

    scrape_run_id: the ScrapeRun PK. When provided:
      - Each new Apify run ID is registered so Stop can abort it.
      - The cancel_requested flag is checked between batches.
    """
    for i, batch in enumerate(chunk_list(cities, MAX_CITIES_PER_RUN)):

        # Check cancel BEFORE launching a new Apify run
        if _is_cancel_requested(scrape_run_id):
            print(f"[Craigslist] Cancel requested — stopping before batch {i+1}")
            return

        print(f"[Craigslist] Scraping batch: {batch} | categories: {category_codes}")
        run_id, dataset_id = start_craigslist_run(batch, category_codes)

        # Register so the Stop button can abort this specific Apify run
        _register_apify_run(scrape_run_id, run_id)

        try:
            wait_for_run(run_id, scrape_run_id=scrape_run_id)
        except Exception as e:
            print(f"[Craigslist] Run ended early ({e}), fetching partial dataset...")
            try:
                partial = fetch_dataset(dataset_id)
                if partial:
                    print(f"[Craigslist] Yielding {len(partial)} partial results")
                    yield partial
            except Exception as fetch_err:
                print(f"[Craigslist] Could not fetch partial dataset: {fetch_err}")
            # If cancelled, stop iterating
            if _is_cancel_requested(scrape_run_id):
                return
            continue

        results = fetch_dataset(dataset_id)
        print(f"[Craigslist] Got {len(results)} results from batch")
        yield results

        if i < len(list(chunk_list(cities, MAX_CITIES_PER_RUN))) - 1:
            # Check cancel before cooldown
            if _is_cancel_requested(scrape_run_id):
                print("[Craigslist] Cancel requested — stopping after batch")
                return
            print(f"[Craigslist] Cooling down {COOLDOWN_BETWEEN_RUNS}s before next batch...")
            time.sleep(COOLDOWN_BETWEEN_RUNS)