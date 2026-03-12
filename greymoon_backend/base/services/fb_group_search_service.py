import requests
import time
from django.conf import settings

ACTOR_ID         = "easyapi~facebook-groups-search-scraper"
POLL_INTERVAL    = 5
DEFAULT_MAX_GROUPS = 20


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
        print(f"[FB Group Search] Could not abort run {run_id}: {e}")


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
        print(f"[FB Group Search] Could not register Apify run ID: {e}")


def _is_cancel_requested(scrape_run_id) -> bool:
    if not scrape_run_id:
        return False
    try:
        from base.models import ScrapeRun
        return ScrapeRun.objects.filter(pk=scrape_run_id, cancel_requested=True).exists()
    except Exception:
        return False


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

    # Register before post-launch cancel check
    _register_apify_run(scrape_run_id, run_id)

    if _is_cancel_requested(scrape_run_id):
        _abort_apify_run(run_id)
        return None

    return run_id, dataset_id


def find_facebook_groups(
    keywords: list[str],
    location=None,
    max_groups: int = DEFAULT_MAX_GROUPS,
    scrape_run_id=None,
    _log_fn=None,
) -> list[str]:

    log = _log_fn or print

    if not keywords:
        return []

    n_queries         = len(keywords)
    per_keyword_limit = max(1, -(-max_groups // n_queries))  # ceil
    scope             = location if location else "national (no location filter)"

    group_urls: list[str] = []
    seen: set[str]        = set()

    for keyword in keywords:
        if len(group_urls) >= max_groups:
            break

        # Pre-launch cancel check
        if _is_cancel_requested(scrape_run_id):
            log("[FB Group Search] Cancel requested — stopping")
            break

        query = f"{keyword} {location}".strip() if location else keyword
        log(f"[FB Group Search] Searching: '{query}' (limit {per_keyword_limit}, scope: {scope})")

        payload = {
            "searchQuery": query,
            "maxResults": per_keyword_limit,
            "proxyConfiguration": {
                "useApifyProxy": True,
                "apifyProxyGroups": ["RESIDENTIAL"],
                "apifyProxyCountry": "US",
            },
        }

        # Launch with post-launch guard
        try:
            result = _launch_and_guard(payload, scrape_run_id)
        except Exception as e:
            log(f"[FB Group Search] Failed to start actor for '{keyword}': {e}")
            continue

        if result is None:
            log("[FB Group Search] Cancelled during actor launch — stopping.")
            break

        run_id, dataset_id = result
        log(f"[FB Group Search] Actor started (run {run_id}) for keyword: '{keyword}'")

        # Poll — abort live actor if cancel fires between ticks
        try:
            _wait_for_run(run_id, scrape_run_id=scrape_run_id, log_fn=log)
        except Exception as e:
            log(f"[FB Group Search] Run ended for '{keyword}': {e}")
            # If cancelled, stop the whole keyword loop
            if _is_cancel_requested(scrape_run_id):
                break
            continue

        # Fetch results
        try:
            items = _fetch_dataset(dataset_id)
            log(f"[FB Group Search] Got {len(items)} result(s) for '{keyword}'")
        except Exception as e:
            log(f"[FB Group Search] Could not fetch results for '{keyword}': {e}")
            continue

        for item in items:
            url = item.get("url") or item.get("groupUrl") or item.get("link")
            if url and url not in seen:
                seen.add(url)
                group_urls.append(url)

                # Persist the real group name returned by the actor
                group_name = (
                    item.get("name")
                    or item.get("title")
                    or item.get("groupName")
                    or item.get("groupTitle")
                    or ""
                )
                if group_name:
                    try:
                        from base.models import ScrapedFbGroup
                        ScrapedFbGroup.objects.update_or_create(
                            group_url=url,
                            defaults={"group_name": group_name},
                        )
                    except Exception as e:
                        log(f"[FB Group Search] Could not store group name: {e}")

                if len(group_urls) >= max_groups:
                    break

    log(
        f"[FB Group Search] Discovered {len(group_urls)} unique group(s) "
        f"(requested max: {max_groups}, scope: {scope})"
    )
    return group_urls


def _wait_for_run(run_id: str, scrape_run_id=None, log_fn=None):
    """Poll until done. Aborts the actor immediately if cancel is detected."""
    log = log_fn or print
    url = f"https://api.apify.com/v2/actor-runs/{run_id}"
    while True:
        res = requests.get(url, headers=_apify_headers())
        res.raise_for_status()
        status = res.json()["data"]["status"]
        log(f"[FB Group Search] Actor status: {status}")

        if status == "SUCCEEDED":
            return
        if status in ["FAILED", "ABORTED", "TIMED-OUT"]:
            raise Exception(f"Run {run_id} ended with status: {status}")

        if _is_cancel_requested(scrape_run_id):
            _abort_apify_run(run_id)
            raise Exception(f"Run {run_id} cancelled by user")

        time.sleep(POLL_INTERVAL)


def _fetch_dataset(dataset_id: str) -> list[dict]:
    resp = requests.get(
        f"https://api.apify.com/v2/datasets/{dataset_id}/items?clean=true&limit=200",
        headers=_apify_headers(),
    )
    resp.raise_for_status()
    return resp.json()