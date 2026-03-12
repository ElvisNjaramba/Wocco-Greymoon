import requests
import time
from django.conf import settings

ACTOR_ID = "easyapi~facebook-groups-search-scraper"   # search actor (not scraper)

POLL_INTERVAL = 5
DEFAULT_MAX_GROUPS = 20   # fallback if not specified by user


def _register_apify_run(scrape_run_id, apify_run_id: str):
    """Store Apify run ID on ScrapeRun so Stop can abort it."""
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



def _apify_headers():
    return {"Authorization": f"Bearer {settings.APIFY_TOKEN}"}


def find_facebook_groups(
    keywords: list[str],
    location=None,
    max_groups: int = DEFAULT_MAX_GROUPS,
    scrape_run_id=None,
) -> list[str]:
    """
    Run the Facebook Groups Search actor for each keyword and collect
    up to `max_groups` unique group URLs in total.

    location:
        City/state searches pass a string like "Houston TX" or "Texas TX"
        which is appended to each keyword for more relevant local results.

        ZIP searches pass None — Facebook groups are national so searching
        by ZIP artificially restricts results. Keywords-only queries return
        far more relevant US-wide groups (e.g. "house cleaning services",
        "junk removal contractors") regardless of where the user is based.

    `max_groups` is the user-controlled cap.
    """
    if not keywords:
        return []

    n_queries = len(keywords)
    per_keyword_limit = max(1, -(-max_groups // n_queries))  # ceil division
    scope = location if location else "national (no location filter)"

    group_urls = []
    seen: set[str] = set()

    for keyword in keywords:
        if len(group_urls) >= max_groups:
            break

        # Append location for city/state; omit entirely for ZIP/national scope
        query = f"{keyword} {location}".strip() if location else keyword

        if _is_cancel_requested(scrape_run_id):
            print("[FB Group Search] Cancel requested — stopping")
            break

        print(f"[FB Group Search] Searching: '{query}' (limit {per_keyword_limit}, scope: {scope})")

        payload = {
            "searchQuery": query,
            "maxResults": per_keyword_limit,
            "proxyConfiguration": {
                "useApifyProxy": True,
                "apifyProxyGroups": ["RESIDENTIAL"],
                "apifyProxyCountry": "US",
            },
        }

        try:
            resp = requests.post(
                f"https://api.apify.com/v2/acts/{ACTOR_ID}/runs",
                json=payload,
                headers=_apify_headers(),
            )
            resp.raise_for_status()
            data = resp.json()["data"]
            run_id = data["id"]
            dataset_id = data["defaultDatasetId"]
            _register_apify_run(scrape_run_id, run_id)
        except Exception as e:
            print(f"[FB Group Search] Failed to start actor for '{keyword}': {e}")
            continue

        # Poll until done
        try:
            _wait_for_run(run_id)
        except Exception as e:
            print(f"[FB Group Search] Run failed for '{keyword}': {e}")
            continue

        # Fetch results
        try:
            items = _fetch_dataset(dataset_id)
        except Exception as e:
            print(f"[FB Group Search] Could not fetch results for '{keyword}': {e}")
            continue

        for item in items:
            url = item.get("url") or item.get("groupUrl") or item.get("link")
            if url and url not in seen:
                seen.add(url)
                group_urls.append(url)
                if len(group_urls) >= max_groups:
                    break

    print(
        f"[FB Group Search] Discovered {len(group_urls)} unique group(s) "
        f"(requested max: {max_groups}, scope: {scope})"
    )

    return group_urls


def _wait_for_run(run_id: str):
    url = f"https://api.apify.com/v2/actor-runs/{run_id}"
    while True:
        res = requests.get(url, headers=_apify_headers())
        res.raise_for_status()
        status = res.json()["data"]["status"]
        print(f"[FB Group Search] Status: {status}")
        if status == "SUCCEEDED":
            return
        if status in ["FAILED", "ABORTED", "TIMED-OUT"]:
            raise Exception(f"Run {run_id} ended with status: {status}")
        time.sleep(POLL_INTERVAL)


def _fetch_dataset(dataset_id: str) -> list[dict]:
    url = (
        f"https://api.apify.com/v2/datasets/{dataset_id}"
        f"/items?clean=true&limit=200"
    )
    resp = requests.get(url, headers=_apify_headers())
    resp.raise_for_status()
    return resp.json()