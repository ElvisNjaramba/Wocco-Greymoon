import requests
import time
from django.conf import settings

ACTOR_ID = "misceres~indeed-scraper"
POLL_INTERVAL  = 5
MAX_RESULTS    = 100


def _apify_headers():
    return {"Authorization": f"Bearer {settings.APIFY_TOKEN}"}


def _abort_apify_run(run_id: str):
    try:
        requests.post(
            f"https://api.apify.com/v2/actor-runs/{run_id}/abort",
            headers=_apify_headers(),
            timeout=10,
        )
    except Exception as e:
        print(f"[Indeed] Could not abort run {run_id}: {e}")


def _register_apify_run(scrape_run_id, apify_run_id: str):
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
        print(f"[Indeed] Could not register Apify run ID: {e}")


def _is_cancel_requested(scrape_run_id) -> bool:
    if not scrape_run_id:
        return False
    try:
        from base.models import ScrapeRun
        return ScrapeRun.objects.filter(pk=scrape_run_id, cancel_requested=True).exists()
    except Exception:
        return False


def build_indeed_payload(position: str, location: str, country: str = "US") -> dict:
    return {
        "position": position,
        "country": country,
        "location": location,
        "maxItems": MAX_RESULTS,
        "saveOnlyUniqueItems": True,
        "maxConcurrency": 5,
    }


def _launch_and_guard(payload: dict, scrape_run_id) -> tuple[str, str] | None:
    resp = requests.post(
        f"https://api.apify.com/v2/acts/{ACTOR_ID}/runs",
        json=payload,
        headers=_apify_headers(),
    )
    resp.raise_for_status()
    data       = resp.json()["data"]
    run_id     = data["id"]
    dataset_id = data["defaultDatasetId"]

    _register_apify_run(scrape_run_id, run_id)

    if _is_cancel_requested(scrape_run_id):
        _abort_apify_run(run_id)
        return None

    return run_id, dataset_id


def _wait_for_run(run_id: str, scrape_run_id=None, log_fn=None):
    log = log_fn or print
    url = f"https://api.apify.com/v2/actor-runs/{run_id}"
    while True:
        res = requests.get(url, headers=_apify_headers())
        res.raise_for_status()
        status = res.json()["data"]["status"]
        log(f"[Indeed] Actor status: {status}")
        if status == "SUCCEEDED":
            return
        if status in ["FAILED", "ABORTED", "TIMED-OUT"]:
            raise Exception(f"[Indeed] Run {run_id} ended with: {status}")
        if _is_cancel_requested(scrape_run_id):
            _abort_apify_run(run_id)
            raise Exception(f"[Indeed] Run {run_id} cancelled by user")
        time.sleep(POLL_INTERVAL)


def _fetch_dataset(dataset_id: str, limit: int = MAX_RESULTS) -> list[dict]:
    resp = requests.get(
        f"https://api.apify.com/v2/datasets/{dataset_id}/items?clean=true&limit={limit}",
        headers=_apify_headers(),
    )
    resp.raise_for_status()
    return resp.json()


def scrape_indeed_progressive(
    position: str,
    location: str,
    country: str = "US",
    scrape_run_id=None,
    _log_fn=None,
):
    """
    Generator that scrapes Indeed for the given position/location and yields
    a list of raw job records. Yields partial results on cancellation/error.
    """
    log = _log_fn or print

    if _is_cancel_requested(scrape_run_id):
        log("[Indeed] Cancel requested before start — skipping.")
        return

    log(f"[Indeed] Searching: '{position}' in '{location}' ({country})")

    payload = build_indeed_payload(position, location, country)

    try:
        result = _launch_and_guard(payload, scrape_run_id)
    except Exception as e:
        log(f"[Indeed] Failed to launch actor: {e}")
        return

    if result is None:
        log("[Indeed] Cancelled during actor launch — stopping.")
        return

    run_id, dataset_id = result
    log(f"[Indeed] Actor started (run {run_id})")

    try:
        _wait_for_run(run_id, scrape_run_id=scrape_run_id, log_fn=log)
    except Exception as e:
        log(f"[Indeed] Run ended: {e}")
        try:
            partial = _fetch_dataset(dataset_id)
            if partial:
                log(f"[Indeed] Saving {len(partial)} partial result(s)")
                yield partial
        except Exception as fetch_err:
            log(f"[Indeed] Could not fetch partial dataset: {fetch_err}")
        return

    results = _fetch_dataset(dataset_id)
    log(f"[Indeed] Got {len(results)} results")
    yield results