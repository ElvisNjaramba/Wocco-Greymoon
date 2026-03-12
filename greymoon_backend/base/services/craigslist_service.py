import time
import requests
from django.conf import settings

ACTOR_ID                 = "ivanvs~craigslist-scraper"
POLL_INTERVAL            = 5   
COOLDOWN_BETWEEN_RUNS    = 20  
COOLDOWN_POLL_INTERVAL   = 1   
MAX_CITIES_PER_RUN       = 3   


def chunk_list(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def _apify_headers():
    return {"Authorization": f"Bearer {settings.APIFY_TOKEN}"}


def _abort_apify_run(run_id: str):
    """Best-effort abort of a single Apify actor run."""
    try:
        requests.post(
            f"https://api.apify.com/v2/actor-runs/{run_id}/abort",
            headers=_apify_headers(),
            timeout=10,
        )
    except Exception as e:
        print(f"[Craigslist] Could not abort run {run_id}: {e}")


def _register_apify_run(scrape_run_id: int | None, apify_run_id: str):

    if not scrape_run_id:
        return
    try:
        from base.models import ScrapeRun
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


def _launch_and_guard(
    cities: list[str],
    category_codes: list[str],
    scrape_run_id: int | None,
) -> tuple[str, str] | None:

    resp = requests.post(
        f"https://api.apify.com/v2/acts/{ACTOR_ID}/runs",
        json=build_craigslist_payload(cities, category_codes),
        headers=_apify_headers(),
    )
    resp.raise_for_status()
    data       = resp.json()["data"]
    run_id     = data["id"]
    dataset_id = data["defaultDatasetId"]

    # Register FIRST — before the post-launch cancel check — so that
    # cancel_scrape's own sweep also sees this ID.
    _register_apify_run(scrape_run_id, run_id)

    # Post-launch cancel guard
    if _is_cancel_requested(scrape_run_id):
        _abort_apify_run(run_id)
        return None

    return run_id, dataset_id


def wait_for_run(
    run_id: str,
    source_label: str = "Craigslist",
    scrape_run_id: int | None = None,
    log_fn=None,
):

    log = log_fn or print
    url = f"https://api.apify.com/v2/actor-runs/{run_id}"
    while True:
        res = requests.get(url, headers=_apify_headers())
        res.raise_for_status()
        status = res.json()["data"]["status"]
        log(f"[{source_label}] Actor status: {status}")

        if status == "SUCCEEDED":
            return
        if status in ["FAILED", "ABORTED", "TIMED-OUT"]:
            raise Exception(f"[{source_label}] Actor run {run_id} ended with: {status}")

        # Cancel check inside the poll loop — abort the live actor instantly
        if _is_cancel_requested(scrape_run_id):
            _abort_apify_run(run_id)
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


def _interruptible_cooldown(
    seconds: int,
    scrape_run_id: int | None,
    log_fn=None,
) -> bool:

    log     = log_fn or print
    elapsed = 0
    while elapsed < seconds:
        if _is_cancel_requested(scrape_run_id):
            log("[Craigslist] Cancel requested — cooldown interrupted")
            return False
        chunk   = min(COOLDOWN_POLL_INTERVAL, seconds - elapsed)
        time.sleep(chunk)
        elapsed += chunk
    return True


def scrape_craigslist_progressive(
    cities: list[str],
    category_codes: list[str],
    scrape_run_id: int | None = None,
    _log_fn=None,
):

    log     = _log_fn or print
    batches = list(chunk_list(cities, MAX_CITIES_PER_RUN))

    for i, batch in enumerate(batches):

        # 1. Pre-launch check
        if _is_cancel_requested(scrape_run_id):
            log(f"[Craigslist] Cancel requested — stopping before batch {i+1}")
            return

        log(
            f"[Craigslist] Starting batch {i+1}/{len(batches)}: "
            f"{batch} | categories: {category_codes}"
        )

        # 2. Launch with post-launch guard
        result = _launch_and_guard(batch, category_codes, scrape_run_id)
        if result is None:
            log("[Craigslist] Cancelled during actor launch — stopping.")
            return
        run_id, dataset_id = result
        log(f"[Craigslist] Actor started (run {run_id})")

        # 3. Poll — actor is aborted the moment cancel is detected
        try:
            wait_for_run(run_id, scrape_run_id=scrape_run_id, log_fn=log)
        except Exception as e:
            log(f"[Craigslist] Run ended: {e}")
            try:
                partial = fetch_dataset(dataset_id)
                if partial:
                    log(f"[Craigslist] Saving {len(partial)} partial result(s)")
                    yield partial
            except Exception as fetch_err:
                log(f"[Craigslist] Could not fetch partial dataset: {fetch_err}")
            return  # stop regardless — cancelled or hard failure

        results = fetch_dataset(dataset_id)
        log(f"[Craigslist] Got {len(results)} results from batch {i+1}")
        yield results

        # 4. Interruptible cooldown before the next batch
        if i < len(batches) - 1:
            log(
                f"[Craigslist] Cooling down {COOLDOWN_BETWEEN_RUNS}s "
                "before next batch..."
            )
            if not _interruptible_cooldown(
                COOLDOWN_BETWEEN_RUNS, scrape_run_id, log_fn=log
            ):
                return  # cancelled during cooldown