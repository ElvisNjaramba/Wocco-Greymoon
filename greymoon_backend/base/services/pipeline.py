# ============================================================
#  pipeline.py  —  verbose, progressive-save version
#
#  Every meaningful step writes a human-readable log entry
#  to ScrapeRun.activity_log (JSON list) AND updates
#  ScrapeRun.current_stage / stage_detail so the frontend
#  status poll can show exactly what is happening right now.
# ============================================================

from datetime import datetime, timezone
from .location_resolver import resolve_location, LocationResolutionError
from .category_map import get_craigslist_codes, get_facebook_keywords
from .craigslist_service import scrape_craigslist_progressive
from .fb_group_search_service import find_facebook_groups
from .fb_groups_scraper_service import scrape_facebook_groups_progressive
from .normalizer import normalize_craigslist, normalize_facebook
from .lead_scorer import calculate_lead_score


# ── Logging helper ────────────────────────────────────────────

def _log(scrape_run_id, stage: str, detail: str, level: str = "info"):
    """
    Append one entry to ScrapeRun.activity_log and update
    current_stage + stage_detail so polls see it immediately.

    level: "info" | "success" | "warning" | "error"
    """
    ts = datetime.now(timezone.utc).isoformat()
    entry = {"ts": ts, "stage": stage, "detail": detail, "level": level}
    print(f"[Pipeline][{level.upper()}] {stage}: {detail}")

    if not scrape_run_id:
        return

    try:
        from base.models import ScrapeRun

        run = ScrapeRun.objects.filter(pk=scrape_run_id).first()
        if not run:
            return

        log = run.activity_log or []
        log.append(entry)

        ScrapeRun.objects.filter(pk=scrape_run_id).update(
            current_stage=stage,
            stage_detail=detail,
            activity_log=log,
        )
    except Exception as e:
        print(f"[Pipeline] Could not write log entry: {e}")


# ── Batch save helper ─────────────────────────────────────────

def _save_lead_batch(normalized_items, stats, scrape_run_id=None):
    """
    Persist a batch of normalized leads immediately.
    Updates ScrapeRun.leads_collected after each save so the
    frontend counter is always live.
    """
    from base.models import ServiceLead, ScrapeRun

    for lead_data in normalized_items:
        try:
            content_hash = lead_data.get("content_hash")
            if content_hash and ServiceLead.objects.filter(content_hash=content_hash).exists():
                stats["leads_skipped"] += 1
                continue

            post_id = lead_data.get("post_id")
            if post_id and ServiceLead.objects.filter(post_id=post_id).exists():
                stats["leads_skipped"] += 1
                continue

            score, score_reason = calculate_lead_score(lead_data)

            lead_dt = None
            raw_dt = lead_data.get("datetime")
            if raw_dt:
                try:
                    lead_dt = datetime.fromisoformat(str(raw_dt).replace("Z", "+00:00"))
                except Exception:
                    pass

            ServiceLead.objects.create(
                post_id=post_id or content_hash or "",
                url=lead_data.get("url") or "",
                title=(lead_data.get("title") or "")[:500],
                datetime=lead_dt,
                location=lead_data.get("location") or "",
                category=lead_data.get("category") or "",
                service_category=lead_data.get("service_category") or "",
                state=lead_data.get("state") or "",
                latitude=lead_data.get("latitude") or "",
                longitude=lead_data.get("longitude") or "",
                map_accuracy=lead_data.get("map_accuracy") or "",
                content_hash=content_hash or "",
                post=lead_data.get("post") or "",
                phone=lead_data.get("phone") or "",
                email=lead_data.get("email") or "",
                source=lead_data.get("source", "CRAIGSLIST"),
                score=score,
                score_reason=score_reason,
                raw_json=lead_data.get("raw_json"),
            )
            stats["leads_saved"] += 1

            if scrape_run_id:
                try:
                    ScrapeRun.objects.filter(pk=scrape_run_id).update(
                        leads_collected=stats["leads_saved"],
                        leads_skipped=stats["leads_skipped"],
                    )
                except Exception:
                    pass

        except Exception as e:
            print(f"[Pipeline] Failed to save lead: {e}")
            stats["errors"].append(f"Save error: {str(e)[:100]}")


# ── Main pipeline ─────────────────────────────────────────────

def run_pipeline(
    location_type: str,
    location_value: str,
    categories: list[str],
    sources: list[str],
    scrape_run_id: int | None = None,
    max_groups: int = 20,
) -> dict:
    from base.models import ScrapeRun

    stats = {"leads_saved": 0, "leads_skipped": 0, "errors": []}

    # ── Step 1: Resolve location ──────────────────────────────
    _log(scrape_run_id, "Resolving location",
         f"Looking up '{location_value}' ({location_type})…")
    try:
        location_data = resolve_location(location_type, location_value)
    except LocationResolutionError as e:
        _log(scrape_run_id, "Location error", str(e), level="error")
        stats["errors"].append(str(e))
        _mark_run_failed(scrape_run_id, str(e))
        return stats

    cl_cities = location_data["craigslist_cities"]
    fb_location = location_data["facebook_location_str"]

    _log(scrape_run_id, "Location resolved",
         f"{location_data['display']} — {len(cl_cities)} Craigslist region(s) found",
         level="success")

    # ── Step 2: Resolve categories ────────────────────────────
    cl_codes = get_craigslist_codes(categories)
    fb_keywords = get_facebook_keywords(categories)
    cat_labels = ", ".join(categories)

    _log(scrape_run_id, "Categories mapped",
         f"{cat_labels} → {len(cl_codes)} CL code(s), {len(fb_keywords)} FB keyword(s)")

    # ── Step 3: Craigslist ────────────────────────────────────
    if "craigslist" in sources and cl_cities and cl_codes:
        total_batches = -(-len(cl_cities) // 3)  # ceil(cities / MAX_CITIES_PER_RUN)
        _log(scrape_run_id, "Craigslist — starting",
             f"Scraping {len(cl_cities)} region(s) across {len(cl_codes)} "
             f"categor{'y' if len(cl_codes) == 1 else 'ies'} in ~{total_batches} batch(es).")

        batch_num = 0
        try:
            for batch_items in scrape_craigslist_progressive(cl_cities, cl_codes):
                batch_num += 1
                _log(scrape_run_id,
                     f"Craigslist — batch {batch_num}/{total_batches}",
                     f"Actor finished — received {len(batch_items)} listing(s). Saving to database…")

                normalized_batch = [
                    normalize_craigslist(item, categories[0] if categories else "general")
                    for item in batch_items
                ]
                _save_lead_batch(normalized_batch, stats, scrape_run_id)

                _log(scrape_run_id,
                     f"Craigslist — batch {batch_num} saved",
                     f"{len(normalized_batch)} listing(s) processed — "
                     f"{stats['leads_saved']} saved total, {stats['leads_skipped']} duplicate(s) skipped.",
                     level="success")

            _log(scrape_run_id, "Craigslist — complete",
                 f"All {batch_num} batch(es) finished. "
                 f"{stats['leads_saved']} Craigslist lead(s) saved.",
                 level="success")

        except Exception as e:
            _log(scrape_run_id, "Craigslist — error", str(e), level="error")
            stats["errors"].append(f"Craigslist: {str(e)}")

    elif "craigslist" in sources:
        _log(scrape_run_id, "Craigslist — skipped",
             "No matching regions or category codes for this location.", level="warning")

    # ── Step 4: Facebook group discovery ─────────────────────
    if "facebook" in sources and fb_keywords:
        _log(scrape_run_id, "Facebook — searching for groups",
             f"Querying {len(fb_keywords)} keyword(s) near '{fb_location}' "
             f"to discover relevant Facebook groups…")
        try:
            group_urls = find_facebook_groups(fb_keywords, fb_location, max_groups=max_groups)

            if not group_urls:
                _log(scrape_run_id, "Facebook — no groups found",
                     f"No Facebook groups matched '{fb_location}' for the selected categories. "
                     f"Try a broader location or different categories.",
                     level="warning")
                stats["errors"].append("Facebook: No groups found for this location/category.")

            else:
                _log(scrape_run_id, "Facebook — groups discovered",
                     f"Found {len(group_urls)} unique group(s). Starting post scrape now…",
                     level="success")

                # ── Step 5: Scrape posts from each group chunk ────
                chunk_size = 5
                total_chunks = -(-len(group_urls) // chunk_size)
                chunk_num = 0

                for fb_batch in scrape_facebook_groups_progressive(group_urls):
                    chunk_num += 1
                    _log(scrape_run_id,
                         f"Facebook — scraping chunk {chunk_num}/{total_chunks}",
                         f"Actor returned {len(fb_batch)} post(s) from "
                         f"groups {(chunk_num-1)*chunk_size+1}–{min(chunk_num*chunk_size, len(group_urls))} "
                         f"of {len(group_urls)}. Saving…")

                    normalized_batch = [
                        normalize_facebook(
                            item,
                            categories[0] if categories else "general",
                            location_data["display"],
                        )
                        for item in fb_batch
                    ]
                    _save_lead_batch(normalized_batch, stats, scrape_run_id)

                    _log(scrape_run_id,
                         f"Facebook — chunk {chunk_num} saved",
                         f"{len(normalized_batch)} post(s) processed — "
                         f"{stats['leads_saved']} saved total, {stats['leads_skipped']} duplicate(s) skipped.",
                         level="success")

                _log(scrape_run_id, "Facebook — complete",
                     f"All {chunk_num} chunk(s) scraped across {len(group_urls)} group(s). "
                     f"{stats['leads_saved']} total lead(s) saved.",
                     level="success")

        except Exception as e:
            _log(scrape_run_id, "Facebook — error", str(e), level="error")
            stats["errors"].append(f"Facebook: {str(e)}")

    elif "facebook" in sources:
        _log(scrape_run_id, "Facebook — skipped",
             "No Facebook keywords configured for the selected categories.", level="warning")

    # ── Step 6: Wrap up ───────────────────────────────────────
    if scrape_run_id:
        try:
            run = ScrapeRun.objects.get(pk=scrape_run_id)
            run.status = "SUCCEEDED" if not stats["errors"] else "PARTIAL"
            run.leads_collected = stats["leads_saved"]
            run.leads_skipped = stats["leads_skipped"]
            run.finished_at = datetime.now(timezone.utc)
            run.save()
        except Exception as e:
            print(f"[Pipeline] Could not update ScrapeRun: {e}")

    summary = (
        f"Run complete — {stats['leads_saved']} lead(s) saved, "
        f"{stats['leads_skipped']} duplicate(s) skipped"
        + (f", {len(stats['errors'])} error(s)" if stats["errors"] else "")
    )
    _log(scrape_run_id, "Finished", summary,
         level="success" if not stats["errors"] else "warning")

    return stats


def _mark_run_failed(scrape_run_id, reason: str):
    if not scrape_run_id:
        return
    try:
        from base.models import ScrapeRun
        ScrapeRun.objects.filter(pk=scrape_run_id).update(
            status="FAILED",
            current_stage="Failed",
            stage_detail=reason,
            finished_at=datetime.now(timezone.utc),
        )
    except Exception:
        pass