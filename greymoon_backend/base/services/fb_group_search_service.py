import requests
import time
from django.conf import settings

ACTOR_ID = "easyapi~facebook-groups-search-scraper"   # search actor (not scraper)
POLL_INTERVAL = 5
DEFAULT_MAX_GROUPS = 20   # fallback if not specified by user


def _apify_headers():
    return {"Authorization": f"Bearer {settings.APIFY_TOKEN}"}


def find_facebook_groups(
    keywords: list[str],
    location: str,
    max_groups: int = DEFAULT_MAX_GROUPS,
) -> list[str]:
    """
    Run the Facebook Groups Search actor for each keyword and collect
    up to `max_groups` unique group URLs in total.

    `max_groups` is the user-controlled cap — the search actor is told
    to return at most ceil(max_groups / len(keywords)) results per keyword
    so that the combined total stays near the requested limit.
    """
    if not keywords:
        return []

    per_keyword_limit = max(1, -(-max_groups // len(keywords)))  # ceil division
    group_urls: list[str] = []
    seen: set[str] = set()

    for keyword in keywords:
        if len(group_urls) >= max_groups:
            break

        query = f"{keyword} {location}".strip()
        print(f"[FB Group Search] Searching: '{query}' (limit {per_keyword_limit})")

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

    print(f"[FB Group Search] Discovered {len(group_urls)} unique group(s) "
          f"(requested max: {max_groups})")
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