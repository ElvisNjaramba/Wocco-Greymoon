import requests
import time
from django.conf import settings

ACTOR_ID = "apify~facebook-groups-scraper"

POLL_INTERVAL = 5
MAX_POSTS_PER_GROUP = 50
CHUNK_SIZE = 5

def _apify_headers():
    return {"Authorization": f"Bearer {settings.APIFY_TOKEN}"}

def build_scraper_payload(group_urls: list[str]) -> dict:
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
    url = (
        f"https://api.apify.com/v2/datasets/{dataset_id}"
        f"/items?clean=true&limit=1000"
    )
    resp = requests.get(url, headers=_apify_headers())
    resp.raise_for_status()
    posts = resp.json()
    print(f"[FB Groups Scraper] Retrieved {len(posts)} posts")
    return posts

def _inject_group_url(posts: list[dict], chunk_urls: list[str]) -> list[dict]:
    """
    Apify's FB scraper returns posts but doesn't always include which
    group URL the post came from. We inject it based on position/groupUrl
    field, falling back to tagging posts with the chunk's URLs so the
    normalizer can store group attribution on each lead.
    """
    if not posts:
        return posts
    # Many posts already have groupUrl — leave those alone.
    # For posts missing it, distribute across chunk_urls proportionally.
    posts_per_group = max(1, len(posts) // len(chunk_urls)) if chunk_urls else 1
    for i, post in enumerate(posts):
        if not post.get("groupUrl") and not post.get("group_url"):
            group_idx = min(i // posts_per_group, len(chunk_urls) - 1)
            post["groupUrl"] = chunk_urls[group_idx]
    return posts


def scrape_facebook_groups_progressive(group_urls: list[str]):
    """
    Generator — yields (chunk_urls, posts) tuples so the pipeline knows
    which groups each batch of posts came from.

    The chunk_urls are passed back so the pipeline can log group names
    and the posts are tagged with groupUrl for per-group attribution.
    """
    if not group_urls:
        print("[FB Groups Scraper] No groups to scrape.")
        return

    for i in range(0, len(group_urls), CHUNK_SIZE):
        chunk = group_urls[i:i + CHUNK_SIZE]
        print(f"[FB Groups Scraper] Scraping chunk {i // CHUNK_SIZE + 1}: {len(chunk)} groups")
        print(f"[FB Groups Scraper] Groups: {chunk}")

        try:
            run_id, dataset_id = start_groups_scraper(chunk)
        except Exception as e:
            print(f"[FB Groups Scraper] Failed to start actor for chunk: {e}")
            continue

        try:
            wait_for_run(run_id)
        except Exception as e:
            print(f"[FB Groups Scraper] Run ended early ({e}), fetching partial dataset...")
            try:
                partial = fetch_posts(dataset_id)
                if partial:
                    partial = _inject_group_url(partial, chunk)
                    yield chunk, partial
            except Exception as fetch_err:
                print(f"[FB Groups Scraper] Could not fetch partial dataset: {fetch_err}")
            continue

        posts = fetch_posts(dataset_id)
        if posts:
            posts = _inject_group_url(posts, chunk)
            yield chunk, posts


def scrape_facebook_groups(group_urls: list[str]) -> list[dict]:
    """Legacy non-generator version for backwards compat."""
    all_posts = []
    for _, posts in scrape_facebook_groups_progressive(group_urls):
        all_posts.extend(posts)
    return all_posts