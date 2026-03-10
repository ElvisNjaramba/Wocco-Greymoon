import requests
import time
from django.conf import settings

ACTOR_ID = "apify~facebook-groups-scraper"

POLL_INTERVAL = 5
MAX_POSTS_PER_GROUP = 50   # tune based on your Apify plan
CHUNK_SIZE = 5             # scrape this many groups per actor run


def _apify_headers():
    return {"Authorization": f"Bearer {settings.APIFY_TOKEN}"}


def build_scraper_payload(group_urls: list[str]) -> dict:
    """
    Build the scraper input from a list of group URLs.
    Each URL becomes a startUrl entry.
    """
    start_urls = [{"url": url} for url in group_urls]
    return {
        "startUrls": start_urls,
        "maxPostsPerGroup": MAX_POSTS_PER_GROUP,
        "proxyConfiguration": {
            "useApifyProxy": True,
            "apifyProxyGroups": ["RESIDENTIAL"],
            "apifyProxyCountry": "US",
        },
    }


def start_groups_scraper(group_urls: list[str]) -> tuple[str, str]:
    """Launch the FB groups scraper actor. Returns (run_id, dataset_id)."""
    if not group_urls:
        raise ValueError("[FB Groups Scraper] No group URLs provided.")

    payload = build_scraper_payload(group_urls)
    url = f"https://api.apify.com/v2/acts/{ACTOR_ID}/runs"
    resp = requests.post(url, json=payload, headers=_apify_headers())
    resp.raise_for_status()
    data = resp.json()["data"]
    print(f"[FB Groups Scraper] Started run: {data['id']} for {len(group_urls)} groups")
    return data["id"], data["defaultDatasetId"]


def wait_for_run(run_id: str):
    url = f"https://api.apify.com/v2/actor-runs/{run_id}"
    while True:
        res = requests.get(url, headers=_apify_headers())
        res.raise_for_status()
        status = res.json()["data"]["status"]
        print(f"[FB Groups Scraper] Status: {status}")
        if status == "SUCCEEDED":
            return
        if status in ["FAILED", "ABORTED", "TIMED-OUT"]:
            raise Exception(f"[FB Groups Scraper] Run {run_id} failed: {status}")
        time.sleep(POLL_INTERVAL)


def fetch_posts(dataset_id: str) -> list[dict]:
    """Fetch all scraped posts from the dataset."""
    url = (
        f"https://api.apify.com/v2/datasets/{dataset_id}"
        f"/items?clean=true&limit=1000"
    )
    resp = requests.get(url, headers=_apify_headers())
    resp.raise_for_status()
    posts = resp.json()
    print(f"[FB Groups Scraper] Retrieved {len(posts)} posts")
    return posts


# ── Progressive generator (used by pipeline.py) ───────────────────────────────

def scrape_facebook_groups_progressive(group_urls: list[str]):
    """
    Generator version of scrape_facebook_groups.

    Scrapes groups in small chunks (CHUNK_SIZE at a time) and yields
    the results of each chunk immediately so the pipeline can persist
    them to the database before starting the next chunk.

    This guarantees that data collected before any abort/failure is
    already saved — no data is held in memory waiting for completion.

    Usage:
        for batch in scrape_facebook_groups_progressive(urls):
            save_to_db(batch)
    """
    if not group_urls:
        print("[FB Groups Scraper] No groups to scrape.")
        return

    for i in range(0, len(group_urls), CHUNK_SIZE):
        chunk = group_urls[i:i + CHUNK_SIZE]
        print(f"[FB Groups Scraper] Scraping chunk {i // CHUNK_SIZE + 1}: {len(chunk)} groups")

        try:
            run_id, dataset_id = start_groups_scraper(chunk)
        except Exception as e:
            print(f"[FB Groups Scraper] Failed to start actor for chunk: {e}")
            continue

        try:
            wait_for_run(run_id)
        except Exception as e:
            # Actor failed/aborted — try fetching partial results
            print(f"[FB Groups Scraper] Run ended early ({e}), fetching partial dataset...")
            try:
                partial = fetch_posts(dataset_id)
                if partial:
                    print(f"[FB Groups Scraper] Yielding {len(partial)} partial posts")
                    yield partial
            except Exception as fetch_err:
                print(f"[FB Groups Scraper] Could not fetch partial dataset: {fetch_err}")
            continue

        posts = fetch_posts(dataset_id)
        if posts:
            yield posts


# ── Legacy non-generator version (kept for backwards compat) ──────────────────

def scrape_facebook_groups(group_urls: list[str]) -> list[dict]:
    """
    Full Stage 2 flow: scrape posts from given group URLs.
    Returns raw FB post items — normalization in pipeline.
    """
    if not group_urls:
        print("[FB Groups Scraper] No groups to scrape.")
        return []

    all_posts = []
    for batch in scrape_facebook_groups_progressive(group_urls):
        all_posts.extend(batch)
    return all_posts