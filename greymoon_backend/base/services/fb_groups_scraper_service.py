import requests
import time
from django.conf import settings

ACTOR_ID                 = "apify~facebook-groups-scraper"
POLL_INTERVAL            = 5
DEFAULT_MAX_POSTS_PER_GROUP = 50
CHUNK_SIZE               = 5


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
        print(f"[FB Groups Scraper] Could not abort run {run_id}: {e}")


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
        print(f"[FB Groups Scraper] Could not register Apify run ID: {e}")


def _is_cancel_requested(scrape_run_id) -> bool:
    if not scrape_run_id:
        return False
    try:
        from base.models import ScrapeRun
        return ScrapeRun.objects.filter(pk=scrape_run_id, cancel_requested=True).exists()
    except Exception:
        return False


def build_scraper_payload(group_urls, max_posts_per_group=DEFAULT_MAX_POSTS_PER_GROUP):
    return {
        "startUrls": [{"url": url} for url in group_urls],
        "maxPostsPerGroup": max_posts_per_group,
        "proxyConfiguration": {
            "useApifyProxy": True,
            "apifyProxyGroups": ["RESIDENTIAL"],
            "apifyProxyCountry": "US",
        },
    }


def _launch_and_guard(
    group_urls: list[str],
    max_posts_per_group: int,
    scrape_run_id,
) -> tuple[str, str] | None:

    if not group_urls:
        raise ValueError("[FB Groups Scraper] No group URLs provided.")

    payload = build_scraper_payload(group_urls, max_posts_per_group)
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


def wait_for_run(run_id: str, scrape_run_id=None, log_fn=None):
    """Poll until done. Aborts the actor immediately if cancel is detected."""
    log = log_fn or print
    url = f"https://api.apify.com/v2/actor-runs/{run_id}"
    while True:
        res = requests.get(url, headers=_apify_headers())
        res.raise_for_status()
        status = res.json()["data"]["status"]
        log(f"[FB Groups Scraper] Actor status: {status}")

        if status == "SUCCEEDED":
            return
        if status in ["FAILED", "ABORTED", "TIMED-OUT"]:
            raise Exception(f"Run {run_id} ended with status: {status}")

        if _is_cancel_requested(scrape_run_id):
            _abort_apify_run(run_id)
            raise Exception(f"Run {run_id} cancelled by user")

        time.sleep(POLL_INTERVAL)


def fetch_posts(dataset_id: str, log_fn=None) -> list[dict]:
    log = log_fn or print
    resp = requests.get(
        f"https://api.apify.com/v2/datasets/{dataset_id}/items?clean=true&limit=5000",
        headers=_apify_headers(),
    )
    resp.raise_for_status()
    posts = resp.json()
    log(f"[FB Groups Scraper] Retrieved {len(posts)} posts from dataset")
    return posts


def _inject_group_url(posts: list[dict], chunk_urls: list[str]) -> list[dict]:
    """Tag posts with groupUrl if the actor didn't include it."""
    if not posts or not chunk_urls:
        return posts
    posts_per_group = max(1, len(posts) // len(chunk_urls))
    for i, post in enumerate(posts):
        if not post.get("groupUrl") and not post.get("group_url"):
            group_idx = min(i // posts_per_group, len(chunk_urls) - 1)
            post["groupUrl"] = chunk_urls[group_idx]
    return posts


def scrape_facebook_groups_progressive(
    group_urls: list[str],
    max_posts_per_group: int = DEFAULT_MAX_POSTS_PER_GROUP,
    scrape_run_id=None,
    _log_fn=None,
):

    log = _log_fn or print

    if not group_urls:
        return

    chunks = [
        group_urls[i:i + CHUNK_SIZE]
        for i in range(0, len(group_urls), CHUNK_SIZE)
    ]

    for chunk_num, chunk in enumerate(chunks, 1):

        # 1. Pre-launch cancel check
        if _is_cancel_requested(scrape_run_id):
            log(f"[FB Groups Scraper] Cancel requested — stopping before chunk {chunk_num}")
            return

        log(
            f"[FB Groups Scraper] Starting chunk {chunk_num}/{len(chunks)} "
            f"— {len(chunk)} group(s)"
        )

        # 2. Launch with post-launch guard
        try:
            result = _launch_and_guard(chunk, max_posts_per_group, scrape_run_id)
        except Exception as e:
            log(f"[FB Groups Scraper] Failed to start chunk {chunk_num}: {e}")
            continue

        if result is None:
            log("[FB Groups Scraper] Cancelled during actor launch — stopping.")
            return

        run_id, dataset_id = result
        log(f"[FB Groups Scraper] Actor started (run {run_id}) for chunk {chunk_num}")

        # 3. Poll — actor aborted immediately on cancel
        try:
            wait_for_run(run_id, scrape_run_id=scrape_run_id, log_fn=log)
        except Exception as e:
            log(f"[FB Groups Scraper] Run ended for chunk {chunk_num}: {e}")
            try:
                partial = fetch_posts(dataset_id, log_fn=log)
                if partial:
                    log(f"[FB Groups Scraper] Yielding {len(partial)} partial result(s)")
                    yield chunk, _inject_group_url(partial, chunk)
            except Exception:
                pass
            return  # stop regardless — cancelled or hard failure

        posts = fetch_posts(dataset_id, log_fn=log)
        if posts:
            yield chunk, _inject_group_url(posts, chunk)


def scrape_facebook_groups(
    group_urls: list[str],
    max_posts_per_group: int = DEFAULT_MAX_POSTS_PER_GROUP,
) -> list[dict]:
    """Legacy non-generator wrapper."""
    all_posts = []
    for _, posts in scrape_facebook_groups_progressive(group_urls, max_posts_per_group):
        all_posts.extend(posts)
    return all_posts