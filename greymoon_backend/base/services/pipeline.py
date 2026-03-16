# ============================================================
#  pipeline.py
# ============================================================

from datetime import datetime, timezone

from .location_resolver import resolve_location, LocationResolutionError
from .category_map import get_craigslist_codes, get_facebook_keywords
from .craigslist_service import scrape_craigslist_progressive
from .fb_group_search_service import find_facebook_groups
from .fb_groups_scraper_service import scrape_facebook_groups_progressive
from .indeed_scraper_service import scrape_indeed_progressive
from .google_search_service import scrape_google_search_progressive
from .normalizer import normalize_craigslist, normalize_facebook
from .indeed_normalizer import normalize_indeed
from .google_normalizer import normalize_google_serp_page
from .lead_scorer import calculate_lead_score


# ── Source → search term maps ─────────────────────────────────

INDEED_CATEGORY_TERMS = {
    "cleaning":        "cleaning service contractor",
    "maintenance":     "home maintenance handyman contractor",
    "waste_management": "junk removal waste management contractor",
}

GOOGLE_CATEGORY_QUERIES = {
    "cleaning":        "house cleaning service contractor",
    "maintenance":     "home maintenance handyman contractor",
    "waste_management": "junk removal waste management contractor",
}


# ── Logging helper ────────────────────────────────────────────

def _log(scrape_run_id, stage: str, detail: str, level: str = "info"):
    ts    = datetime.now(timezone.utc).isoformat()
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


# ── Service logger ────────────────────────────────────────────

class _ServiceLogger:
    def __init__(self, scrape_run_id, default_stage="Background"):
        self._run_id  = scrape_run_id
        self._default = default_stage

    def __call__(self, *args, **kwargs):
        import re
        msg    = " ".join(str(a) for a in args)
        m      = re.match(r"^\[([^\]]+)\]\s*(.*)", msg)
        stage  = m.group(1) if m else self._default
        detail = m.group(2) if m else msg
        low    = detail.lower()
        if any(w in low for w in ("error", "failed", "exception")):
            level = "error"
        elif any(w in low for w in ("warn", "skipping", "no groups", "partial", "cancel")):
            level = "warning"
        elif any(w in low for w in ("succeeded", "saved", "complete", "discovered", "got ", "retrieved")):
            level = "success"
        else:
            level = "info"
        _log(self._run_id, stage, detail, level=level)


# ── Cancel helper ─────────────────────────────────────────────

def _cancelled(scrape_run_id) -> bool:
    if not scrape_run_id:
        return False
    try:
        from base.models import ScrapeRun
        return ScrapeRun.objects.filter(
            pk=scrape_run_id, cancel_requested=True
        ).exists()
    except Exception:
        return False


# ── Batch save helper ─────────────────────────────────────────

def _save_lead_batch(normalized_items, stats, scrape_run_id=None):
    from base.models import ServiceLead, ScrapeRun

    for lead_data in normalized_items:
        try:
            content_hash = lead_data.get("content_hash")
            if content_hash and ServiceLead.objects.filter(
                content_hash=content_hash
            ).exists():
                stats["leads_skipped"] += 1
                continue

            post_id = lead_data.get("post_id")
            if post_id and ServiceLead.objects.filter(
                post_id=post_id
            ).exists():
                stats["leads_skipped"] += 1
                continue

            score, score_reason = calculate_lead_score(lead_data)

            lead_dt = None
            raw_dt  = lead_data.get("datetime")
            if raw_dt:
                try:
                    lead_dt = datetime.fromisoformat(
                        str(raw_dt).replace("Z", "+00:00")
                    )
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


# ── FB batch processor ────────────────────────────────────────

def _process_and_save_fb_batch(
    chunk_urls, fb_batch, categories, location_data,
    zip_code, stats, scrape_run_id, group_name_map,
    chunk_num, total_chunks, svc_log,
):
    from base.models import ScrapedFbGroup, ScrapedFbPost

    chunk_names = ", ".join(group_name_map.get(u, u) for u in chunk_urls[:3])
    if len(chunk_urls) > 3:
        chunk_names += f" (+{len(chunk_urls)-3})"

    _log(scrape_run_id,
         f"Facebook — chunk {chunk_num}/{total_chunks}",
         f"Groups: {chunk_names} — {len(fb_batch)} post(s). Deduping + saving...")

    post_urls_in_batch = [
        item.get("url") or item.get("postUrl") or item.get("link") or ""
        for item in fb_batch
    ]
    already_seen = set(
        ScrapedFbPost.objects.filter(
            post_url__in=[u for u in post_urls_in_batch if u]
        ).values_list("post_url", flat=True)
    )
    fresh_items = [
        item for item, url in zip(fb_batch, post_urls_in_batch)
        if not url or url not in already_seen
    ]
    dupe_count = len(fb_batch) - len(fresh_items)
    if dupe_count:
        _log(scrape_run_id,
             f"Facebook — post dedup (chunk {chunk_num})",
             f"{dupe_count} already saved — skipped. "
             f"{len(fresh_items)} new post(s) to process.",
             level="info")

    normalized = [
        normalize_facebook(
            item,
            categories[0] if categories else "general",
            location_data["display"] if location_data else "",
            zip_code=zip_code,
        )
        for item in fresh_items
    ]
    _save_lead_batch(normalized, stats, scrape_run_id)

    new_post_records = []
    for item, url in zip(fb_batch, post_urls_in_batch):
        if url and url not in already_seen:
            group_url_for_post = (
                item.get("groupUrl") or item.get("group_url")
                or (chunk_urls[0] if chunk_urls else "")
            )
            new_post_records.append(
                ScrapedFbPost(post_url=url, group_url=group_url_for_post)
            )
    if new_post_records:
        ScrapedFbPost.objects.bulk_create(
            new_post_records, ignore_conflicts=True
        )

    _log(scrape_run_id,
         f"Facebook — chunk {chunk_num} saved",
         f"{len(normalized)} new post(s) saved — "
         f"{stats['leads_saved']} total, {stats['leads_skipped']} duplicate(s).",
         level="success")


# ── Main pipeline ─────────────────────────────────────────────

def run_pipeline(
    location_type,
    location_value,
    categories,
    sources,
    scrape_run_id=None,
    max_groups=20,
    max_posts_per_group=50,
    fb_custom_keywords=None,
    fb_group_urls=None,
    # Google-specific settings
    google_max_pages=3,         # pages per query (~10 results each)
    google_deep_scrape=True,    # crawl contractor websites for contacts
):
    from base.models import ScrapeRun

    stats   = {"leads_saved": 0, "leads_skipped": 0, "errors": []}
    svc_log = _ServiceLogger(scrape_run_id)

    # ── Step 1: Resolve location ──────────────────────────────
    if location_value:
        _log(scrape_run_id, "Resolving location",
             f"Looking up '{location_value}' ({location_type})...")
        try:
            location_data = resolve_location(location_type, location_value)
        except LocationResolutionError as e:
            _log(scrape_run_id, "Location error", str(e), level="error")
            stats["errors"].append(str(e))
            _mark_run_failed(scrape_run_id, str(e))
            return stats

        cl_cities   = location_data["craigslist_cities"]
        zip_code    = location_data.get("zip_code")
        fb_location = None if zip_code else location_data["facebook_location_str"]
        zip_note    = (
            f" (ZIP: {zip_code} — Facebook will search nationally)"
            if zip_code else ""
        )
        _log(scrape_run_id, "Location resolved",
             f"{location_data['display']}{zip_note} — "
             f"{len(cl_cities)} Craigslist region(s)",
             level="success")
    else:
        location_data = None
        cl_cities     = []
        zip_code      = None
        fb_location   = None
        _log(scrape_run_id, "Location",
             "No location set — Facebook-only run.", level="info")

    # ── Step 2: Resolve categories ────────────────────────────
    cl_codes    = get_craigslist_codes(categories)
    fb_keywords = get_facebook_keywords(categories)
    _log(scrape_run_id, "Categories mapped",
         f"{', '.join(categories)} → {len(cl_codes)} CL code(s), "
         f"{len(fb_keywords)} FB keyword(s)")

    # ── Step 3: Craigslist ────────────────────────────────────
    if "craigslist" in sources and cl_cities and cl_codes:
        total_batches = -(-len(cl_cities) // 3)
        _log(scrape_run_id, "Craigslist — starting",
             f"Scraping {len(cl_cities)} region(s) across {len(cl_codes)} "
             f"categor{'y' if len(cl_codes) == 1 else 'ies'} "
             f"in ~{total_batches} batch(es).")
        batch_num = 0
        try:
            for batch_items in scrape_craigslist_progressive(
                cl_cities, cl_codes,
                scrape_run_id=scrape_run_id,
                _log_fn=svc_log,
            ):
                batch_num += 1
                _log(scrape_run_id,
                     f"Craigslist — batch {batch_num}/{total_batches}",
                     f"Received {len(batch_items)} listing(s). Saving...")
                normalized = [
                    normalize_craigslist(
                        item, categories[0] if categories else "general"
                    )
                    for item in batch_items
                ]
                _save_lead_batch(normalized, stats, scrape_run_id)
                _log(scrape_run_id,
                     f"Craigslist — batch {batch_num} saved",
                     f"{len(normalized)} processed — "
                     f"{stats['leads_saved']} saved, "
                     f"{stats['leads_skipped']} duplicate(s).",
                     level="success")
            _log(scrape_run_id, "Craigslist — complete",
                 f"All {batch_num} batch(es) done. "
                 f"{stats['leads_saved']} lead(s) saved.",
                 level="success")
        except Exception as e:
            _log(scrape_run_id, "Craigslist — error", str(e), level="error")
            stats["errors"].append(f"Craigslist: {str(e)}")

    elif "craigslist" in sources:
        _log(scrape_run_id, "Craigslist — skipped",
             "No matching regions or category codes for this location.",
             level="warning")

    # ── Step 4: Facebook ──────────────────────────────────────
    manual_group_urls = [u.strip() for u in (fb_group_urls or []) if u.strip()]
    has_fb_keywords   = bool(fb_keywords or fb_custom_keywords)

    if "facebook" in sources and (has_fb_keywords or manual_group_urls):
        from base.models import ScrapedFbGroup, ScrapedFbPost

        all_fb_keywords = list(fb_keywords)
        for kw in (fb_custom_keywords or []):
            if kw and kw not in all_fb_keywords:
                all_fb_keywords.append(kw)
        fb_keywords = all_fb_keywords

        try:
            if manual_group_urls:
                discovered_urls = manual_group_urls
                _log(scrape_run_id, "Facebook — manual groups",
                     f"Using {len(manual_group_urls)} manually specified group URL(s).",
                     level="success")
            else:
                if _cancelled(scrape_run_id):
                    raise Exception("Cancelled by user")
                custom_note = (
                    f" + {len(fb_custom_keywords)} custom term(s)"
                    if fb_custom_keywords else ""
                )
                _log(scrape_run_id, "Facebook — searching for groups",
                     f"Querying {len(fb_keywords)} keyword(s){custom_note} "
                     + (f"near '{fb_location}'" if fb_location else "nationally")
                     + " to discover groups...")
                discovered_urls = find_facebook_groups(
                    fb_keywords, fb_location,
                    max_groups=max_groups,
                    scrape_run_id=scrape_run_id,
                    _log_fn=svc_log,
                )

            if not discovered_urls:
                hint = f"near '{fb_location}'" if fb_location else "nationally"
                _log(scrape_run_id, "Facebook — no groups found",
                     f"No groups found {hint}. Try different categories "
                     f"or add group URLs manually.",
                     level="warning")
                stats["errors"].append("Facebook: No groups found.")
            else:
                def _slug(u):
                    import re as _re
                    m = _re.search(r"/groups/([^/?#]+)", u or "")
                    slug = m.group(1) if m else (u.rstrip("/").split("/")[-1])
                    slug = _re.sub(r"([a-z])([A-Z])", r"\1 \2", slug)
                    slug = _re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1 \2", slug)
                    slug = _re.sub(r"[-_]", " ", slug)
                    return _re.sub(r"\s+", " ", slug).strip().title() or u

                stored_names = {
                    obj["group_url"]: obj["group_name"]
                    for obj in ScrapedFbGroup.objects.filter(
                        group_url__in=discovered_urls
                    ).values("group_url", "group_name")
                    if obj["group_name"]
                }
                group_name_map = {
                    url: stored_names.get(url) or _slug(url)
                    for url in discovered_urls
                }
                previously = set(
                    ScrapedFbGroup.objects.filter(
                        group_url__in=discovered_urls
                    ).values_list("group_url", flat=True)
                )
                new_count  = sum(1 for u in discovered_urls if u not in previously)
                seen_count = len(previously)
                if seen_count:
                    _log(scrape_run_id, "Facebook — group info",
                         f"{seen_count} previously scraped group(s) will be "
                         f"re-scraped for new posts. "
                         f"{new_count} brand-new group(s) also queued.",
                         level="info")

                names_preview = ", ".join(list(group_name_map.values())[:8])
                if len(discovered_urls) > 8:
                    names_preview += f" (+{len(discovered_urls)-8} more)"
                _log(scrape_run_id, "Facebook — groups to scrape",
                     f"Scraping {len(discovered_urls)} group(s): {names_preview}",
                     level="success")

                chunk_size   = 5
                total_chunks = -(-len(discovered_urls) // chunk_size)
                chunk_num    = 0

                for chunk_urls, fb_batch in scrape_facebook_groups_progressive(
                    discovered_urls,
                    max_posts_per_group=max_posts_per_group,
                    scrape_run_id=scrape_run_id,
                    _log_fn=svc_log,
                ):
                    chunk_num += 1
                    _process_and_save_fb_batch(
                        chunk_urls=chunk_urls, fb_batch=fb_batch,
                        categories=categories, location_data=location_data,
                        zip_code=zip_code, stats=stats,
                        scrape_run_id=scrape_run_id,
                        group_name_map=group_name_map,
                        chunk_num=chunk_num, total_chunks=total_chunks,
                        svc_log=svc_log,
                    )
                    if _cancelled(scrape_run_id):
                        _log(scrape_run_id, "Facebook — cancelled",
                             f"Cancelled after saving chunk {chunk_num}. "
                             f"{stats['leads_saved']} lead(s) saved so far.",
                             level="warning")
                        break

                for url in discovered_urls:
                    ScrapedFbGroup.objects.update_or_create(
                        group_url=url,
                        defaults={
                            "group_name": group_name_map.get(url, ""),
                            "post_count": ScrapedFbPost.objects.filter(
                                group_url=url
                            ).count(),
                        },
                    )
                _log(scrape_run_id, "Facebook — complete",
                     f"All {chunk_num} chunk(s) done across "
                     f"{len(discovered_urls)} group(s). "
                     f"{stats['leads_saved']} total lead(s) saved.",
                     level="success")

        except Exception as e:
            _log(scrape_run_id, "Facebook — error", str(e), level="error")
            stats["errors"].append(f"Facebook: {str(e)}")

    elif "facebook" in sources:
        _log(scrape_run_id, "Facebook — skipped",
             "No Facebook keywords configured for the selected categories.",
             level="warning")

    # ── Step 5: Indeed ────────────────────────────────────────
    if "indeed" in sources:
        if not location_data:
            _log(scrape_run_id, "Indeed — skipped",
                 "No location set — Indeed requires a location.",
                 level="warning")
        else:
            indeed_position = " ".join(
                INDEED_CATEGORY_TERMS.get(c, c) for c in categories
            ).strip() or "contractor"
            indeed_location = (
                location_data.get("facebook_location_str")
                or location_data.get("display", "")
            )
            _log(scrape_run_id, "Indeed — starting",
                 f"Searching Indeed for '{indeed_position}' "
                 f"near '{indeed_location}'...")

            if _cancelled(scrape_run_id):
                _log(scrape_run_id, "Indeed — skipped",
                     "Cancelled before Indeed could start.", level="warning")
            else:
                batch_num = 0
                try:
                    for batch_items in scrape_indeed_progressive(
                        position=indeed_position,
                        location=indeed_location,
                        country="US",
                        scrape_run_id=scrape_run_id,
                        _log_fn=svc_log,
                    ):
                        batch_num += 1
                        _log(scrape_run_id,
                             f"Indeed — batch {batch_num}",
                             f"Received {len(batch_items)} listing(s). Saving...")
                        normalized = [
                            normalize_indeed(
                                item,
                                categories[0] if categories else "general",
                                indeed_location,
                            )
                            for item in batch_items
                        ]
                        _save_lead_batch(normalized, stats, scrape_run_id)
                        _log(scrape_run_id,
                             f"Indeed — batch {batch_num} saved",
                             f"{len(normalized)} processed — "
                             f"{stats['leads_saved']} total saved, "
                             f"{stats['leads_skipped']} duplicate(s).",
                             level="success")
                        if _cancelled(scrape_run_id):
                            _log(scrape_run_id, "Indeed — cancelled",
                                 f"Cancelled after saving batch {batch_num}. "
                                 f"{stats['leads_saved']} lead(s) saved so far.",
                                 level="warning")
                            break

                    if batch_num > 0:
                        _log(scrape_run_id, "Indeed — complete",
                             f"All {batch_num} batch(es) done. "
                             f"{stats['leads_saved']} total lead(s) saved.",
                             level="success")
                    else:
                        _log(scrape_run_id, "Indeed — no results",
                             "Actor returned 0 results for this search.",
                             level="warning")
                except Exception as e:
                    _log(scrape_run_id, "Indeed — error", str(e), level="error")
                    stats["errors"].append(f"Indeed: {str(e)}")

    # ── Step 6: Google Search ─────────────────────────────────
    if "google" in sources:
        if not location_data:
            _log(scrape_run_id, "Google Search — skipped",
                 "No location set — Google Search requires a location.",
                 level="warning")
        else:
            # One deduplicated query per selected category
            seen_q         = set()
            google_queries = []
            for c in categories:
                q = GOOGLE_CATEGORY_QUERIES.get(c, f"{c} contractor")
                if q not in seen_q:
                    seen_q.add(q)
                    google_queries.append(q)

            google_location = (
                location_data.get("facebook_location_str")
                or location_data.get("display", "")
            )

            approx_results = google_max_pages * 10 * len(google_queries)
            _log(scrape_run_id, "Google Search — starting",
                 f"{len(google_queries)} "
                 f"quer{'y' if len(google_queries) == 1 else 'ies'} × "
                 f"{google_max_pages} page(s) ≈ {approx_results} results near "
                 f"'{google_location}'"
                 + (" + website deep-scrape" if google_deep_scrape else "")
                 + " + all AI modes + leads enrichment...")

            if _cancelled(scrape_run_id):
                _log(scrape_run_id, "Google Search — skipped",
                     "Cancelled before Google Search could start.",
                     level="warning")
            else:
                query_num = 0
                try:
                    for result_bundle in scrape_google_search_progressive(
                        queries=google_queries,
                        location=google_location,
                        max_pages=google_max_pages,
                        enrich_leads=True,
                        deep_scrape_sites=google_deep_scrape,
                        scrape_run_id=scrape_run_id,
                        _log_fn=svc_log,
                    ):
                        query_num += 1
                        serp_pages   = result_bundle.get("serp_pages", [])
                        contacts_map = result_bundle.get("contacts_map", {})

                        all_leads = []
                        for page in serp_pages:
                            page_leads = normalize_google_serp_page(
                                page,
                                categories[0] if categories else "general",
                                google_location,
                                contacts_map=contacts_map,
                            )
                            all_leads.extend(page_leads)

                        if not all_leads:
                            _log(scrape_run_id,
                                 f"Google Search — query {query_num} empty",
                                 "No usable leads extracted from SERP pages.",
                                 level="warning")
                            continue

                        _log(scrape_run_id,
                             f"Google Search — query {query_num} saving",
                             f"{len(all_leads)} lead(s) from "
                             f"{len(serp_pages)} SERP page(s)"
                             + (f" + {len(contacts_map)} site(s) crawled"
                                if contacts_map else "")
                             + ". Saving...")

                        _save_lead_batch(all_leads, stats, scrape_run_id)

                        _log(scrape_run_id,
                             f"Google Search — query {query_num} saved",
                             f"{len(all_leads)} processed — "
                             f"{stats['leads_saved']} total saved, "
                             f"{stats['leads_skipped']} duplicate(s).",
                             level="success")

                        if _cancelled(scrape_run_id):
                            _log(scrape_run_id, "Google Search — cancelled",
                                 f"Cancelled after query {query_num}. "
                                 f"{stats['leads_saved']} lead(s) saved so far.",
                                 level="warning")
                            break

                    if query_num > 0:
                        _log(scrape_run_id, "Google Search — complete",
                             f"All {query_num} "
                             f"quer{'y' if query_num == 1 else 'ies'} done. "
                             f"{stats['leads_saved']} total lead(s) saved.",
                             level="success")
                    else:
                        _log(scrape_run_id, "Google Search — no results",
                             "Actor returned 0 results for all queries.",
                             level="warning")

                except Exception as e:
                    _log(scrape_run_id, "Google Search — error",
                         str(e), level="error")
                    stats["errors"].append(f"Google Search: {str(e)}")

    # ── Step 7: Wrap up ───────────────────────────────────────
    if scrape_run_id:
        try:
            run = ScrapeRun.objects.get(pk=scrape_run_id)
            if run.status == "RUNNING":
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