# ============================================================
#  pipeline.py  ---  verbose, progressive-save version
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
                zip_code=lead_data.get("zip_code") or "",
                source=lead_data.get("source", "CRAIGSLIST"),
                fb_group_name=lead_data.get("fb_group_name") or "",
                fb_group_url=lead_data.get("fb_group_url") or "",
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
    scrape_run_id=None,
    max_groups: int = 20,
    fb_custom_keywords=None,
) -> dict:

    from base.models import ScrapeRun

    stats = {"leads_saved": 0, "leads_skipped": 0, "errors": []}

    # ── Step 1: Resolve location ──────────────────────────────
    _log(scrape_run_id, "Resolving location",
         f"Looking up '{location_value}' ({location_type})...")

    try:
        location_data = resolve_location(location_type, location_value)
    except LocationResolutionError as e:
        _log(scrape_run_id, "Location error", str(e), level="error")
        stats["errors"].append(str(e))
        _mark_run_failed(scrape_run_id, str(e))
        return stats

    cl_cities  = location_data["craigslist_cities"]
    # For Facebook: ZIP searches go national (no location filter).
    # Facebook groups are US-wide; appending a ZIP narrows results
    # artificially. City and state searches still pass location context.
    zip_code = location_data.get("zip_code")
    if zip_code:
        fb_location = None   # signals FB search to run without location filter
    else:
        fb_location = location_data["facebook_location_str"]

    zip_note = f" (ZIP: {zip_code} — Facebook will search nationally)" if zip_code else ""
    _log(scrape_run_id, "Location resolved",
         f"{location_data['display']}{zip_note} — {len(cl_cities)} Craigslist region(s) found",
         level="success")

    # Early-exit if cancelled before pipeline even starts
    def _cancelled():
        if not scrape_run_id:
            return False
        try:
            from base.models import ScrapeRun
            return ScrapeRun.objects.filter(pk=scrape_run_id, cancel_requested=True).exists()
        except Exception:
            return False

    # ── Step 2: Resolve categories ────────────────────────────
    cl_codes    = get_craigslist_codes(categories)
    fb_keywords = get_facebook_keywords(categories)
    cat_labels  = ", ".join(categories)

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
            for batch_items in scrape_craigslist_progressive(cl_cities, cl_codes, scrape_run_id=scrape_run_id):
                batch_num += 1
                _log(scrape_run_id,
                     f"Craigslist — batch {batch_num}/{total_batches}",
                     f"Actor finished — received {len(batch_items)} listing(s). Saving to database...")

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
    if "facebook" in sources and (fb_keywords or fb_custom_keywords):

        # Merge category keywords with any user-supplied custom terms
        all_fb_keywords = list(fb_keywords)
        for kw in (fb_custom_keywords or []):
            if kw and kw not in all_fb_keywords:
                all_fb_keywords.append(kw)
        fb_keywords = all_fb_keywords

        custom_note = f" + {len(fb_custom_keywords)} custom term(s)" if fb_custom_keywords else ""
        _log(scrape_run_id, "Facebook — searching for groups",
             f"Querying {len(fb_keywords)} keyword(s){custom_note} "
             + (f"near '{fb_location}' " if fb_location else "nationally (ZIP — no location filter) ")
             + f"to discover relevant Facebook groups...")

        try:
            group_urls = find_facebook_groups(
                fb_keywords,
                fb_location,       # None when ZIP was entered → national search
                max_groups=max_groups,
                scrape_run_id=scrape_run_id,
            )

            if not group_urls:
                location_hint = f"near '{fb_location}'" if fb_location else "nationally"
                _log(scrape_run_id, "Facebook — no groups found",
                     f"No Facebook groups found {location_hint} for the selected categories. "
                     f"Try different categories.",
                     level="warning")
                stats["errors"].append("Facebook: No groups found for this search.")

            else:
                # group_urls is a list of URL strings from the search actor.
                # Build a name list for logging from the URL slugs.
                group_name_map = {
                    url: url.rstrip("/").split("/")[-1].replace("-", " ").title()
                    for url in group_urls
                }
                group_list_str = ", ".join(list(group_name_map.values())[:8])
                if len(group_urls) > 8:
                    group_list_str += f" (+{len(group_urls)-8} more)"

                _log(scrape_run_id, "Facebook — groups discovered",
                     f"Found {len(group_urls)} group(s): {group_list_str}. Starting post scrape now...",
                     level="success")

                # ── Step 5: Scrape posts from each group chunk ────
                chunk_size    = 5
                total_chunks  = -(-len(group_urls) // chunk_size)
                chunk_num     = 0

                for chunk_urls, fb_batch in scrape_facebook_groups_progressive(group_urls):
                    chunk_num += 1
                    chunk_names = ", ".join(group_name_map.get(u, u) for u in chunk_urls[:3])
                    if len(chunk_urls) > 3:
                        chunk_names += f" (+{len(chunk_urls)-3})"
                    _log(scrape_run_id,
                         f"Facebook — scraping chunk {chunk_num}/{total_chunks}",
                         f"Scraping: {chunk_names} — {len(fb_batch)} post(s) returned. Saving...")

                    normalized_batch = [
                        normalize_facebook(
                            item,
                            categories[0] if categories else "general",
                            location_data["display"],
                            zip_code=zip_code,
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
            run.leads_skipped   = stats["leads_skipped"]
            run.finished_at     = datetime.now(timezone.utc)
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