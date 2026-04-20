import time
import requests
from django.conf import settings

FB_POSTS_ACTOR_ID = "apify~facebook-groups-scraper"

POLL_INTERVAL            = 5
GROUPS_PER_POST_BATCH    = 5
MAX_POSTS_PER_GROUP      = 1000
COOLDOWN_BETWEEN_BATCHES = 5


def _apify_headers() -> dict:
    return {"Authorization": f"Bearer {settings.APIFY_TOKEN}"}


def _abort_apify_run(run_id: str):
    try:
        requests.post(
            f"https://api.apify.com/v2/actor-runs/{run_id}/abort",
            headers=_apify_headers(),
            timeout=10,
        )
    except Exception as e:
        print(f"[Facebook] Could not abort run {run_id}: {e}")


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
        print(f"[Facebook] Could not register Apify run ID: {e}")


def _is_cancel_requested(scrape_run_id) -> bool:
    if not scrape_run_id:
        return False
    try:
        from base.models import ScrapeRun
        return ScrapeRun.objects.filter(
            pk=scrape_run_id, cancel_requested=True
        ).exists()
    except Exception:
        return False


def _fetch_dataset_count(dataset_id: str) -> int:
    try:
        resp = requests.get(
            f"https://api.apify.com/v2/datasets/{dataset_id}",
            headers=_apify_headers(),
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json().get("data", {}).get("itemCount", 0)
    except Exception:
        return 0


def _launch_actor(
    actor_id: str,
    payload: dict,
    scrape_run_id,
) -> tuple[str, str] | None:
    resp = requests.post(
        f"https://api.apify.com/v2/acts/{actor_id}/runs",
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


def _fetch_dataset(dataset_id: str, limit: int = 1000) -> list[dict]:
    resp = requests.get(
        f"https://api.apify.com/v2/datasets/{dataset_id}"
        f"/items?limit={limit}",
        headers=_apify_headers(),
        timeout=60,
    )
    resp.raise_for_status()
    items = resp.json()
    return [i for i in items if isinstance(i, dict)]


def upsert_fb_groups(group_urls: list[str], log=None) -> None:
    log = log or print
    try:
        from base.models import ScrapedFbGroup
        for url in group_urls:
            url = url.strip().rstrip("/") + "/"
            if not url or url == "/":
                continue
            ScrapedFbGroup.objects.get_or_create(
                group_url=url,
                defaults={"group_name": "", "post_count": 0},
            )
    except Exception as e:
        log(f"[Facebook] Could not upsert group registry: {e}")


def build_fb_posts_payload(
    group_urls: list[str],
    max_posts: int = MAX_POSTS_PER_GROUP,
) -> dict:
    return {
        "startUrls":       [{"url": u} for u in group_urls],
        "maxPosts":        max_posts,
        "maxPostsPerPage": max_posts,
        "sort":            "RECENT_ACTIVITY",
        "proxyConfiguration": {
            "useApifyProxy":     True,
            "apifyProxyGroups":  ["RESIDENTIAL"],
            "apifyProxyCountry": "US",
        },
    }


def scrape_fb_groups_progressive(
    group_urls: list[str],
    max_posts_per_group: int = MAX_POSTS_PER_GROUP,
    scrape_run_id=None,
    log=None,
    progress_callback=None,
):
    log = log or print

    if not group_urls:
        log("[Facebook Posts] No group URLs to scrape")
        return

    upsert_fb_groups(group_urls, log=log)

    batches = [
        group_urls[i:i + GROUPS_PER_POST_BATCH]
        for i in range(0, len(group_urls), GROUPS_PER_POST_BATCH)
    ]
    total_batches = len(batches)

    for b_idx, batch in enumerate(batches):
        if _is_cancel_requested(scrape_run_id):
            log(
                f"[Facebook Posts] Cancel requested — "
                f"stopping before batch {b_idx + 1}/{total_batches}"
            )
            return

        batch_labels = ", ".join(
            u.rstrip("/").split("/")[-1] or u
            for u in batch[:3]
        )
        if len(batch) > 3:
            batch_labels += f" (+{len(batch) - 3})"

        log(
            f"[Facebook Posts] Batch {b_idx + 1}/{total_batches} — "
            f"scraping {len(batch)} group(s): {batch_labels}"
        )

        if _is_cancel_requested(scrape_run_id):
            log(f"[Facebook Posts] Cancel requested — stopping before batch {b_idx + 1}")
            return

        payload = build_fb_posts_payload(batch, max_posts_per_group)
        try:
            result = _launch_actor(FB_POSTS_ACTOR_ID, payload, scrape_run_id)
        except Exception as e:
            log(f"[Facebook Posts] Failed to launch batch {b_idx + 1}: {e}")
            continue

        if result is None:
            log(f"[Facebook Posts] Cancelled during launch — stopping")
            return

        run_id, dataset_id = result
        log(
            f"[Facebook Posts] Batch {b_idx + 1} actor launched "
            f"(run {run_id}) — waiting for posts…"
        )

        # ── Stream results as they arrive ─────────────────────────
        last_saved_count = 0
        final_status     = None
        poll_count       = 0
        poll_error_count = 0

        while True:
            # Cancel check at top of every loop
            if _is_cancel_requested(scrape_run_id):
                _abort_apify_run(run_id)
                log(f"[Facebook Posts] Cancelled by user --- run {run_id} aborted")
                final_status = "ABORTED"
                break

            # Poll actor status
            try:
                res = requests.get(
                    f"https://api.apify.com/v2/actor-runs/{run_id}",
                    headers=_apify_headers(),
                    timeout=15,
                )
                res.raise_for_status()
                status = res.json()["data"]["status"]
                poll_error_count = 0
            except Exception as e:
                poll_error_count += 1
                log(f"[Facebook Posts] Status check error ({poll_error_count}/5): {e}")
                if poll_error_count >= 5:
                    final_status = "FAILED"
                    break
                for _ in range(POLL_INTERVAL):
                    if _is_cancel_requested(scrape_run_id):
                        break
                    time.sleep(1)
                continue

            poll_count += 1

            # ── Fetch and yield NEW items every poll ──────────────
            current_count = _fetch_dataset_count(dataset_id)
            if progress_callback and current_count > 0:
                progress_callback(current_count)

            if current_count > last_saved_count:
                try:
                    all_items = _fetch_dataset(
                        dataset_id,
                        limit=len(batch) * max_posts_per_group * 2,
                    )
                    new_items = all_items[last_saved_count:]
                    if new_items:
                        log(
                            f"[Facebook Posts] Batch {b_idx + 1} — "
                            f"yielding {len(new_items)} new post(s) "
                            f"({current_count} total so far)"
                        )
                        _update_group_metadata(batch, new_items, log)
                        yield new_items
                        last_saved_count = len(all_items)
                except Exception as e:
                    log(f"[Facebook Posts] Mid-run fetch error: {e}")

            # Re-check cancel after yield — limit may have just been hit
            if _is_cancel_requested(scrape_run_id):
                _abort_apify_run(run_id)
                log(f"[Facebook Posts] Cancelled after yield --- run {run_id} aborted")
                final_status = "ABORTED"
                break

            if poll_count % 4 == 0:
                log(
                    f"[Facebook Posts] Still running "
                    f"({poll_count * POLL_INTERVAL}s) | "
                    f"~{current_count} post(s) found so far..."
                )

            if status == "SUCCEEDED":
                final_status = "SUCCEEDED"
                break

            if status in ("FAILED", "ABORTED", "TIMED-OUT"):
                log(f"[Facebook Posts] Actor run {run_id} ended with: {status}")
                final_status = status
                break

            # Interruptible sleep
            for _ in range(POLL_INTERVAL):
                if _is_cancel_requested(scrape_run_id):
                    _abort_apify_run(run_id)
                    final_status = "ABORTED"
                    break
                time.sleep(1)

            if final_status == "ABORTED":
                break

        # ── Final fetch to catch any remaining items ──────────────
        try:
            all_items = _fetch_dataset(
                dataset_id,
                limit=len(batch) * max_posts_per_group * 2,
            )
            remaining = all_items[last_saved_count:]
            if remaining:
                log(
                    f"[Facebook Posts] Batch {b_idx + 1} final — "
                    f"{len(remaining)} remaining post(s) saved"
                )
                _update_group_metadata(batch, remaining, log)
                yield remaining
        except Exception as e:
            log(f"[Facebook Posts] Final fetch error for batch {b_idx + 1}: {e}")

        if final_status == "ABORTED":
            return

        if b_idx < total_batches - 1:
            log(
                f"[Facebook Posts] Cooling down {COOLDOWN_BETWEEN_BATCHES}s "
                "before next batch…"
            )
            for _ in range(COOLDOWN_BETWEEN_BATCHES):
                if _is_cancel_requested(scrape_run_id):
                    log("[Facebook Posts] Cancel during cooldown — stopping")
                    return
                time.sleep(1)


def _update_group_metadata(batch_urls: list[str], items: list[dict], log):
    try:
        from base.models import ScrapedFbGroup
        from django.db.models import F

        url_counts: dict[str, int] = {}
        url_names:  dict[str, str] = {}

        for item in items:
            g_url = (
                item.get("inputUrl")
                or item.get("facebookUrl")
                or ""
            ).rstrip("/") + "/"
            if not g_url or g_url == "/":
                continue
            url_counts[g_url] = url_counts.get(g_url, 0) + 1

            g_name = (
                item.get("groupName")
                or item.get("group")
                or item.get("groupTitle")
                or ""
            )
            if g_name and g_url not in url_names:
                url_names[g_url] = g_name

        for raw_url in batch_urls:
            norm  = raw_url.rstrip("/") + "/"
            count = url_counts.get(norm, 0)
            name  = url_names.get(norm, "")

            update_kwargs = {}
            if count:
                update_kwargs["post_count"] = F("post_count") + count
            if name:
                grp = ScrapedFbGroup.objects.filter(
                    group_url__in=[raw_url, norm]
                ).first()
                if grp and not grp.group_name:
                    update_kwargs["group_name"] = name

            if update_kwargs:
                ScrapedFbGroup.objects.filter(
                    group_url__in=[raw_url, norm]
                ).update(**update_kwargs)

    except Exception as e:
        log(f"[Facebook Posts] Could not update group metadata: {e}")