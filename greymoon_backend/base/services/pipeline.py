import threading
from datetime import datetime, timezone
from .location_resolver import resolve_location, LocationResolutionError
from .category_map import get_craigslist_codes, get_facebook_keywords
from .craigslist_service import scrape_craigslist_progressive
from .google_search_service import scrape_google_search_progressive
from .fb_service import scrape_fb_groups_progressive, upsert_fb_groups
from .normalizer import normalize_craigslist, normalize_facebook
from .google_normalizer import normalize_google_serp_page
from .lead_scorer import calculate_lead_score
import requests
from django.conf import settings

GOOGLE_CATEGORY_QUERIES = {
    "cleaning":         "house cleaning service contractor",
    "maintenance":      "home maintenance handyman contractor",
    "waste_management": "junk removal waste management contractor",
}


# ── Sentinel — raised when max_leads is hit ────────────────────
class LimitReached(Exception):
    pass


def _log(scrape_run_id, stage: str, detail: str, level: str = "info", extra: dict = None):
    ts    = datetime.now(timezone.utc).isoformat()
    entry = {"ts": ts, "stage": stage, "detail": detail, "level": level}
    if extra:
        entry.update(extra)
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


def _emit_source_stats(scrape_run_id, source: str, saved: int, skipped: int, batch_saved: int = 0):
    if not scrape_run_id:
        return
    try:
        from base.models import ScrapeRun
        ts = datetime.now(timezone.utc).isoformat()
        entry = {
            "ts":          ts,
            "stage":       f"{source.title()} — live count",
            "detail":      f"{saved} lead(s) saved ({skipped} duplicate(s) skipped)",
            "level":       "success" if saved > 0 else "info",
            "type":        "source_stats",
            "source":      source,
            "saved":       saved,
            "skipped":     skipped,
            "batch_saved": batch_saved,
        }

        run = ScrapeRun.objects.filter(pk=scrape_run_id).first()
        if not run:
            return

        log = run.activity_log or []
        log = [e for e in log if not (e.get("type") == "source_stats" and e.get("source") == source)]
        log.append(entry)

        source_stats = run.source_stats or {}
        source_stats[source] = {"saved": saved, "skipped": skipped}

        ScrapeRun.objects.filter(pk=scrape_run_id).update(
            activity_log=log,
            source_stats=source_stats,
        )
    except Exception as e:
        print(f"[Pipeline] Could not emit source stats: {e}")


def _make_progress_callback(scrape_run_id, source_label: str, stats: dict, max_leads: int = 0):
    def callback(apify_count: int):
        if not scrape_run_id:
            return
        try:
            from base.models import ScrapeRun
 
            already_saved = stats.get("leads_saved", 0)
            detail = (
                f"~{apify_count} result(s) found by Apify "
                f"| {already_saved} lead(s) saved to DB so far"
            )
            ScrapeRun.objects.filter(pk=scrape_run_id).update(stage_detail=detail)
 
            if not max_leads:
                return  # unlimited — nothing to do
 
            # ── FIX: abort as soon as the actor has found enough ──
            remaining = max(0, max_leads - already_saved)
 
            limit_already_hit  = already_saved >= max_leads          
            actor_has_enough = apify_count >= max(remaining, 1)

            if limit_already_hit or actor_has_enough:
                if scrape_run_id:
                    try:
                        from base.models import ScrapeRun
                        ScrapeRun.objects.filter(pk=scrape_run_id).update(cancel_requested=True)
                    except Exception:
                        pass
                _abort_all_actors(scrape_run_id)
 
        except Exception:
            pass
 
    return callback


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
        elif any(w in low for w in ("warn", "skipping", "no groups",
                                    "partial", "cancel", "stopping")):
            level = "warning"
        elif any(w in low for w in ("succeeded", "saved", "complete",
                                    "discovered", "got ", "retrieved",
                                    "registered", "found")):
            level = "success"
        else:
            level = "info"
        _log(self._run_id, stage, detail, level=level)


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

def _abort_all_actors(scrape_run_id):
    if not scrape_run_id:
        return

    try:
        from base.models import ScrapeRun
        import time

        run = ScrapeRun.objects.filter(pk=scrape_run_id).first()
        if not run:
            return

        ScrapeRun.objects.filter(pk=scrape_run_id).update(cancel_requested=True)

        headers = {"Authorization": f"Bearer {settings.APIFY_TOKEN}"}
        aborted_set = set()

        def _abort_ids(ids):
            for apify_id in ids:
                try:
                    requests.post(
                        f"https://api.apify.com/v2/actor-runs/{apify_id}/abort",
                        headers=headers,
                        timeout=10,
                    )
                    aborted_set.add(apify_id)
                except Exception as e:
                    print(f"[Pipeline] Could not abort actor {apify_id} on limit: {e}")

        def _sweep():
            newly = []
            for status_filter in ("RUNNING", "READY"):
                try:
                    resp = requests.get(
                        "https://api.apify.com/v2/actor-runs",
                        headers=headers,
                        params={"status": status_filter, "limit": 100},
                        timeout=15,
                    )
                    if resp.status_code != 200:
                        continue
                    for actor_run in resp.json().get("data", {}).get("items", []):
                        aid = actor_run.get("id")
                        if aid and aid not in aborted_set:
                            try:
                                requests.post(
                                    f"https://api.apify.com/v2/actor-runs/{aid}/abort",
                                    headers=headers,
                                    timeout=10,
                                )
                                aborted_set.add(aid)
                                newly.append(aid)
                            except Exception:
                                pass
                except Exception:
                    pass
            return newly

        # Abort all currently registered actors
        _abort_ids(run.apify_run_ids or [])

        # Sweep immediately, then again after short delays to catch late-registered actors
        _sweep()
        time.sleep(3)

        # Re-read from DB in case new actors were registered after the first read
        run.refresh_from_db()
        _abort_ids([aid for aid in (run.apify_run_ids or []) if aid not in aborted_set])
        _sweep()

        time.sleep(3)
        run.refresh_from_db()
        _abort_ids([aid for aid in (run.apify_run_ids or []) if aid not in aborted_set])
        _sweep()

    except Exception as e:
        print(f"[Pipeline] _abort_all_actors error: {e}")

def _finalise_run(scrape_run_id, stats: dict):
    if not scrape_run_id:
        return
    try:
        from base.models import ScrapeRun
        ScrapeRun.objects.filter(pk=scrape_run_id).update(
            status="SUCCEEDED",
            limit_stop=True,
            leads_collected=stats["leads_saved"],
            leads_skipped=stats["leads_skipped"],
            finished_at=datetime.now(timezone.utc),
            )
    except Exception as e:
        print(f"[Pipeline] Could not finalise run: {e}")


def _inc_skipped(stats, source_key):
    if source_key:
        stats.setdefault("source_skipped", {})
        stats["source_skipped"][source_key] = (
            stats["source_skipped"].get(source_key, 0) + 1
        )


def _save_lead_batch(
    normalized_items,
    stats,
    scrape_run_id=None,
    source_key: str = None,
    max_leads: int = 0,
):
    from base.models import ServiceLead, ScrapeRun
    import re
 
    def _norm_title(t: str) -> str:
        if not t:
            return ""
        t = t.lower().strip()
        t = re.sub(r'[\W_]+', ' ', t)
        return re.sub(r'\s+', ' ', t).strip()
 
    if not normalized_items:
        return
 
    # ── Guard: quota already filled before we even start ──────
    if max_leads and stats["leads_saved"] >= max_leads:
        raise LimitReached()           # caller's except block will abort
 
    incoming_hashes = {i["content_hash"] for i in normalized_items if i.get("content_hash")}
    incoming_ids    = {i["post_id"]       for i in normalized_items if i.get("post_id")}
    incoming_titles = {_norm_title(i["title"]) for i in normalized_items if i.get("title")}
 
    existing_hashes = set(
        ServiceLead.objects.filter(content_hash__in=incoming_hashes)
        .values_list("content_hash", flat=True)
    ) if incoming_hashes else set()
 
    existing_ids = set(
        ServiceLead.objects.filter(post_id__in=incoming_ids)
        .values_list("post_id", flat=True)
    ) if incoming_ids else set()
 
    existing_titles: set[str] = set()
    if incoming_titles:
        existing_titles = {
            _norm_title(t)
            for t in ServiceLead.objects.values_list("title", flat=True)
            if _norm_title(t) in incoming_titles
        }
 
    batch_saved_count = 0
 
    for lead_data in normalized_items:
        if max_leads and stats["leads_saved"] >= max_leads:
            if scrape_run_id:
                try:
                    from base.models import ScrapeRun
                    ScrapeRun.objects.filter(pk=scrape_run_id).update(cancel_requested=True)
                except Exception:
                    pass
            raise LimitReached()     
 
        try:
            content_hash = lead_data.get("content_hash")
            if content_hash and content_hash in existing_hashes:
                stats["leads_skipped"] += 1
                _inc_skipped(stats, source_key)
                continue
 
            post_id = lead_data.get("post_id")
            if post_id and post_id in existing_ids:
                stats["leads_skipped"] += 1
                _inc_skipped(stats, source_key)
                continue
 
            norm_t = _norm_title(lead_data.get("title", ""))
            if norm_t and norm_t in existing_titles:
                stats["leads_skipped"] += 1
                _inc_skipped(stats, source_key)
                continue
 
            score, score_reason = calculate_lead_score(lead_data)
 
            lead_dt = None
            raw_dt  = lead_data.get("datetime")
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
            batch_saved_count += 1
 
            if source_key:
                stats.setdefault("source_saved", {})
                stats["source_saved"][source_key] = (
                    stats["source_saved"].get(source_key, 0) + 1
                )
 
            if norm_t:
                existing_titles.add(norm_t)
            if content_hash:
                existing_hashes.add(content_hash)
            if post_id:
                existing_ids.add(post_id)
 
            if scrape_run_id:
                try:
                    ScrapeRun.objects.filter(pk=scrape_run_id).update(
                        leads_collected=stats["leads_saved"],
                        leads_skipped=stats["leads_skipped"],
                    )
                except Exception:
                    pass
 
        except LimitReached:
            raise
 
        except Exception as e:
            if "unique_content_hash" in str(e) or "UNIQUE constraint" in str(e):
                stats["leads_skipped"] += 1
                _inc_skipped(stats, source_key)
                continue
 
            err_msg = (
                f"Save error for post_id={lead_data.get('post_id','?')[:20]}: "
                f"{str(e)[:150]}"
            )
            print(f"[Pipeline] {err_msg}")
            _log(scrape_run_id, "Pipeline --- save error", err_msg, level="error")
            stats["errors"].append(err_msg)
 
    if source_key and scrape_run_id and batch_saved_count > 0:
        src_saved   = stats.get("source_saved",   {}).get(source_key, 0)
        src_skipped = stats.get("source_skipped", {}).get(source_key, 0)
        _emit_source_stats(
            scrape_run_id, source_key, src_saved, src_skipped, batch_saved_count
        )


def _enrich_saved_google_leads(contacts_map: dict, scrape_run_id, log):
    if not contacts_map:
        return
    try:
        from base.models import ServiceLead
        from django.db.models import Q

        updated = 0

        for norm_key, contacts in contacts_map.items():
            phones = contacts.get("phones", [])
            emails = contacts.get("emails", [])
            if not phones and not emails:
                continue

            www_key = norm_key.replace("://", "://www.", 1)

            url_filter = (
                Q(url=norm_key) | Q(url__startswith=norm_key + "/") |
                Q(url__startswith=norm_key + "?") |
                Q(url=www_key)  | Q(url__startswith=www_key  + "/") |
                Q(url__startswith=www_key  + "?")
            )

            needs_enriching = (
                Q(phone__isnull=True) | Q(phone="") |
                Q(email__isnull=True) | Q(email="")
            )

            leads_to_update = ServiceLead.objects.filter(
                url_filter, source="GOOGLE",
            ).filter(needs_enriching)

            for lead in leads_to_update:
                update_fields = {}
                if phones and not lead.phone:
                    update_fields["phone"] = phones[0]
                if emails and not lead.email:
                    update_fields["email"] = emails[0]
                if update_fields:
                    ServiceLead.objects.filter(pk=lead.pk).update(**update_fields)
                    updated += 1

        if updated:
            log(
                f"[Google Website Crawl] Enriched {updated} saved lead(s) "
                f"with contact data from crawled sites"
            )
    except Exception as e:
        log(f"[Google Website Crawl] Enrichment update error: {e}")


def _process_and_save_fb_batch(
    chunk_urls,
    fb_batch,
    stats,
    scrape_run_id,
    group_name_map,
    chunk_num,
    total_chunks,
    svc_log,
    max_leads: int = 0,
):
    from base.models import ScrapedFbPost

    chunk_names = ", ".join(group_name_map.get(u, u) for u in chunk_urls[:3])
    if len(chunk_urls) > 3:
        chunk_names += f" (+{len(chunk_urls) - 3})"

    _log(
        scrape_run_id,
        f"Facebook — chunk {chunk_num}/{total_chunks}",
        f"Groups: {chunk_names} — {len(fb_batch)} post(s). Deduping + saving…",
    )

    def _item_url(item: dict) -> str:
        return (
            item.get("url")
            or item.get("postUrl")
            or item.get("permalink")
            or item.get("link")
            or item.get("postLink")
            or ""
        )

    all_urls = [u for u in (_item_url(i) for i in fb_batch) if u]

    already_seen: set[str] = set(
        ScrapedFbPost.objects.filter(post_url__in=all_urls)
        .values_list("post_url", flat=True)
    ) if all_urls else set()

    fresh_items = []
    for item in fb_batch:
        url = _item_url(item)
        if url and url in already_seen:
            continue
        fresh_items.append(item)

    dupe_count = len(fb_batch) - len(fresh_items)
    if dupe_count:
        _log(
            scrape_run_id,
            f"Facebook — post dedup (chunk {chunk_num})",
            f"{dupe_count} already saved — skipped. "
            f"{len(fresh_items)} new post(s) to process.",
        )

    normalized = [
        normalize_facebook(item, service_category="", location_str="")
        for item in fresh_items
    ]

    # LimitReached bubbles straight up to _run_facebook_pipeline
    _save_lead_batch(
        normalized, stats, scrape_run_id,
        source_key="facebook", max_leads=max_leads,
    )

    new_post_records = []
    for item in fresh_items:
        url = _item_url(item)
        if not url:
            continue
        group_url_for_post = (
            item.get("groupUrl")
            or item.get("group_url")
            or item.get("inputUrl")
            or item.get("input_url")
            or (chunk_urls[0] if len(chunk_urls) == 1 else "")
        )
        new_post_records.append(
            ScrapedFbPost(post_url=url, group_url=group_url_for_post)
        )

    if new_post_records:
        ScrapedFbPost.objects.bulk_create(new_post_records, ignore_conflicts=True)

    fb_saved   = stats.get("source_saved",   {}).get("facebook", 0)
    fb_skipped = stats.get("source_skipped", {}).get("facebook", 0)

    _log(
        scrape_run_id,
        f"Facebook — chunk {chunk_num} saved",
        f"{len(fb_batch)} retrieved → {len(fresh_items)} fresh → "
        f"{fb_saved} FB leads saved, {fb_skipped} duplicate(s).",
        level="success",
    )


def run_pipeline(
    location_type,
    location_value,
    categories,
    sources,
    scrape_run_id=None,
    max_posts_per_group=50,
    fb_group_urls=None,
    google_max_pages=3,
    google_deep_scrape=True,
    max_leads: int = 0,          # 0 = unlimited
):
    from base.models import ScrapeRun

    stats = {
        "leads_saved":    0,
        "leads_skipped":  0,
        "errors":         [],
        "source_saved":   {},
        "source_skipped": {},
        "limit_stop":     False,
    }
    svc_log = _ServiceLogger(scrape_run_id)

    if location_value:
        _log(
            scrape_run_id, "Resolving location",
            f"Looking up '{location_value}' ({location_type})…",
        )
        try:
            location_data = resolve_location(location_type, location_value)
        except LocationResolutionError as e:
            _log(scrape_run_id, "Location error", str(e), level="error")
            stats["errors"].append(str(e))
            _mark_run_failed(scrape_run_id, str(e))
            return stats

        cl_cities = location_data["craigslist_cities"]
        zip_code  = location_data.get("zip_code")

        _log(
            scrape_run_id, "Location resolved",
            f"{location_data['display']} — {len(cl_cities)} Craigslist region(s)",
            level="success",
        )
    else:
        location_data = None
        cl_cities     = []
        zip_code      = None
        _log(
            scrape_run_id, "Location",
            "No location set — Facebook-only run.", level="info",
        )

    if max_leads:
        _log(
            scrape_run_id, "Lead limit set",
            f"Run will stop automatically after {max_leads} lead(s) are saved.",
            level="info",
        )

    cl_codes = get_craigslist_codes(categories) if categories else []
    if categories:
        _log(
            scrape_run_id, "Categories mapped",
            f"{', '.join(categories)} → {len(cl_codes)} CL code(s)",
        )

    # ── Craigslist ─────────────────────────────────────────────
    if "craigslist" in sources and cl_cities and cl_codes:
        total_batches = -(-len(cl_cities) // 3)
        _log(
            scrape_run_id, "Craigslist — starting",
            f"Scraping {len(cl_cities)} region(s) across {len(cl_codes)} "
            f"categor{'y' if len(cl_codes) == 1 else 'ies'} "
            f"in ~{total_batches} batch(es).",
        )

        try:
            from base.models import ServiceLead
            existing_cl_ids = set(
                ServiceLead.objects.filter(source="CRAIGSLIST")
                .values_list("post_id", flat=True)
            )
        except Exception:
            existing_cl_ids = set()

        batch_num = 0
        try:
            for batch_items in scrape_craigslist_progressive(
                cl_cities, cl_codes,
                scrape_run_id=scrape_run_id,
                _log_fn=svc_log,
                progress_callback=_make_progress_callback(
                    scrape_run_id, "Craigslist", stats, max_leads=max_leads
                ),
            ):
                batch_num += 1
                fresh = [
                    item for item in batch_items
                    if str(item.get("id") or "") not in existing_cl_ids
                ]
                skipped_known = len(batch_items) - len(fresh)
                if skipped_known:
                    stats["leads_skipped"] += skipped_known
                    stats.setdefault("source_skipped", {})
                    stats["source_skipped"]["craigslist"] = (
                        stats["source_skipped"].get("craigslist", 0) + skipped_known
                    )

                _log(
                    scrape_run_id,
                    f"Craigslist — batch {batch_num}/{total_batches}",
                    f"Received {len(batch_items)} listing(s) "
                    f"({len(fresh)} new). Saving…",
                )

                normalized = [
                    normalize_craigslist(
                        item, categories[0] if categories else "general"
                    )
                    for item in fresh
                ]
                _save_lead_batch(
                    normalized, stats, scrape_run_id,
                    source_key="craigslist", max_leads=max_leads,
                )

                for item in fresh:
                    pid = str(item.get("id") or "")
                    if pid:
                        existing_cl_ids.add(pid)

                cl_saved   = stats.get("source_saved",   {}).get("craigslist", 0)
                cl_skipped = stats.get("source_skipped", {}).get("craigslist", 0)

                _log(
                    scrape_run_id,
                    f"Craigslist — batch {batch_num} saved",
                    f"{len(normalized)} processed — "
                    f"{cl_saved} CL leads saved, "
                    f"{cl_skipped} duplicate(s).",
                    level="success",
                )

            _log(
                scrape_run_id, "Craigslist — complete",
                f"All {batch_num} batch(es) done. "
                f"{stats.get('source_saved', {}).get('craigslist', 0)} CL lead(s) saved.",
                level="success",
            )

        except LimitReached:
            _log(
                scrape_run_id, "Craigslist — limit reached",
                f"max_leads={max_leads} reached after "
                f"{stats['leads_saved']} lead(s) — stopping.",
                level="success",
            )
            stats["limit_stop"] = True
            _abort_all_actors(scrape_run_id) 
            _finalise_run(scrape_run_id, stats)
            return stats

        except Exception as e:
            _log(scrape_run_id, "Craigslist — error", str(e), level="error")
            stats["errors"].append(f"Craigslist: {str(e)}")

    elif "craigslist" in sources:
        _log(
            scrape_run_id, "Craigslist — skipped",
            "No matching regions or category codes for this location.",
            level="warning",
        )

    # ── Facebook ───────────────────────────────────────────────
    if "facebook" in sources:
        manual_group_urls = [u.strip() for u in (fb_group_urls or []) if u.strip()]

        if not manual_group_urls:
            _log(
                scrape_run_id, "Facebook — skipped",
                "No group URLs provided. Add group URLs to scrape Facebook.",
                level="warning",
            )
        elif _cancelled(scrape_run_id):
            _log(
                scrape_run_id, "Facebook — skipped",
                "Cancelled before Facebook could start.", level="warning",
            )
        else:
            try:
                _run_facebook_pipeline(
                    manual_group_urls=manual_group_urls,
                    max_posts_per_group=max_posts_per_group,
                    stats=stats,
                    scrape_run_id=scrape_run_id,
                    svc_log=svc_log,
                    max_leads=max_leads,
                )
            except LimitReached:
                _log(
                    scrape_run_id, "Facebook — limit reached",
                    f"max_leads={max_leads} reached after "
                    f"{stats['leads_saved']} lead(s) — stopping.",
                    level="success",
                )
                stats["limit_stop"] = True
                _abort_all_actors(scrape_run_id) 
                _finalise_run(scrape_run_id, stats)
                return stats

    # ── Google ─────────────────────────────────────────────────
    if "google" in sources:
        if not location_data:
            _log(
                scrape_run_id, "Google Search — skipped",
                "No location set — Google Search requires a location.",
                level="warning",
            )
        elif _cancelled(scrape_run_id):
            _log(
                scrape_run_id, "Google Search — skipped",
                "Cancelled before Google Search could start.", level="warning",
            )
        else:
            try:
                _run_google_pipeline(
                    categories=categories,
                    location_data=location_data,
                    google_max_pages=google_max_pages,
                    google_deep_scrape=google_deep_scrape,
                    stats=stats,
                    scrape_run_id=scrape_run_id,
                    svc_log=svc_log,
                    max_leads=max_leads,
                )
            except LimitReached:
                _log(
                    scrape_run_id, "Google Search — limit reached",
                    f"max_leads={max_leads} reached after "
                    f"{stats['leads_saved']} lead(s) — stopping.",
                    level="success",
                )
                stats["limit_stop"] = True
                _abort_all_actors(scrape_run_id) 
                _finalise_run(scrape_run_id, stats)
                return stats

    # ── Finalise ───────────────────────────────────────────────
    if scrape_run_id:
        try:
            run = ScrapeRun.objects.get(pk=scrape_run_id)
            if run.status == "RUNNING":
                run.status = "SUCCEEDED" if not stats["errors"] else "PARTIAL"
            run.leads_collected = stats["leads_saved"]
            run.leads_skipped   = stats["leads_skipped"]
            run.finished_at     = datetime.now(timezone.utc)
            run.source_stats = {
                k: {
                    "saved":   v,
                    "skipped": stats.get("source_skipped", {}).get(k, 0),
                }
                for k, v in stats.get("source_saved", {}).items()
            }
            run.save()
        except Exception as e:
            print(f"[Pipeline] Could not update ScrapeRun: {e}")

    summary = (
        f"Run complete — {stats['leads_saved']} lead(s) saved, "
        f"{stats['leads_skipped']} duplicate(s) skipped"
        + (f", {len(stats['errors'])} error(s)" if stats["errors"] else "")
    )
    _log(
        scrape_run_id, "Finished", summary,
        level="success" if not stats["errors"] else "warning",
    )
    return stats


def _run_facebook_pipeline(
    *,
    manual_group_urls,
    max_posts_per_group,
    stats,
    scrape_run_id,
    svc_log,
    max_leads: int = 0,
):
    all_group_urls: list[str] = []
    seen: set[str] = set()
    for url in manual_group_urls:
        url = url.strip().rstrip("/") + "/"
        if url and url not in seen:
            seen.add(url)
            all_group_urls.append(url)

    if not all_group_urls:
        _log(
            scrape_run_id, "Facebook — no groups to scrape",
            "No valid group URLs after normalisation.",
            level="warning",
        )
        return

    _log(
        scrape_run_id, "Facebook — posts starting",
        f"Scraping posts from {len(all_group_urls)} group(s) "
        f"(up to {max_posts_per_group} posts/group, "
        f"already-scraped posts will be skipped)…",
    )

    group_name_map: dict[str, str] = {}
    try:
        from base.models import ScrapedFbGroup
        for grp in ScrapedFbGroup.objects.filter(
            group_url__in=all_group_urls
        ).values("group_url", "group_name"):
            group_name_map[grp["group_url"]] = grp["group_name"] or grp["group_url"]
    except Exception:
        pass

    total_chunks = -(-len(all_group_urls) // 5)
    chunk_num    = 0

    for batch_posts in scrape_fb_groups_progressive(
        group_urls=all_group_urls,
        max_posts_per_group=max_posts_per_group,
        scrape_run_id=scrape_run_id,
        log=svc_log,
        progress_callback=_make_progress_callback(
            scrape_run_id, "Facebook", stats, max_leads=max_leads
        ),
    ):
        chunk_num += 1

        if not batch_posts:
            _log(
                scrape_run_id,
                f"Facebook — chunk {chunk_num} empty",
                "Actor returned no posts for this batch.",
                level="warning",
            )
            continue

        chunk_urls_set: set[str] = set()
        for post in batch_posts:
            g_url = (
                post.get("inputUrl")
                or post.get("facebookUrl")
                or ""
            ).rstrip("/") + "/"
            if g_url and g_url != "/":
                chunk_urls_set.add(g_url)
        chunk_urls = list(chunk_urls_set) if chunk_urls_set else all_group_urls

        # LimitReached bubbles up to run_pipeline
        _process_and_save_fb_batch(
            chunk_urls=chunk_urls,
            fb_batch=batch_posts,
            stats=stats,
            scrape_run_id=scrape_run_id,
            group_name_map=group_name_map,
            chunk_num=chunk_num,
            total_chunks=total_chunks,
            svc_log=svc_log,
            max_leads=max_leads,
        )

        if _cancelled(scrape_run_id):
            fb_saved = stats.get("source_saved", {}).get("facebook", 0)
            _log(
                scrape_run_id, "Facebook — cancelled",
                f"Cancelled after chunk {chunk_num}. "
                f"{fb_saved} FB lead(s) saved.",
                level="warning",
            )
            return

    fb_saved = stats.get("source_saved", {}).get("facebook", 0)
    _log(
        scrape_run_id, "Facebook — complete",
        f"All {chunk_num} chunk(s) processed. "
        f"{fb_saved} FB lead(s) saved.",
        level="success",
    )


def _run_google_pipeline(
    *,
    categories,
    location_data,
    google_max_pages,
    google_deep_scrape,
    stats,
    scrape_run_id,
    svc_log,
    max_leads: int = 0,
):
    try:
        from base.models import ServiceLead
        existing_google_urls = set(
            ServiceLead.objects.filter(source="GOOGLE")
            .values_list("url", flat=True)
        )
    except Exception:
        existing_google_urls = set()

    seen_q: set[str] = set()
    google_queries: list[str] = []
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
    _log(
        scrape_run_id, "Google Search — starting",
        f"{len(google_queries)} "
        f"quer{'y' if len(google_queries) == 1 else 'ies'} × "
        f"{google_max_pages} page(s) ≈ {approx_results} results near "
        f"'{google_location}'"
        + (" + website deep-scrape" if google_deep_scrape else "")
        + " + AI modes + leads enrichment…",
    )

    query_num = 0
    for result_bundle in scrape_google_search_progressive(
        queries=google_queries,
        location=google_location,
        max_pages=google_max_pages,
        enrich_leads=True,
        deep_scrape_sites=google_deep_scrape,
        scrape_run_id=scrape_run_id,
        _log_fn=svc_log,
        enrich_callback=lambda cm: _enrich_saved_google_leads(
            cm, scrape_run_id, svc_log
        ),
        progress_callback=_make_progress_callback(
            scrape_run_id, "Google", stats, max_leads=max_leads
        ),
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
            for lead in page_leads:
                url = lead.get("url", "")
                if url and url in existing_google_urls:
                    stats["leads_skipped"] += 1
                    stats.setdefault("source_skipped", {})
                    stats["source_skipped"]["google"] = (
                        stats["source_skipped"].get("google", 0) + 1
                    )
                    continue
                all_leads.append(lead)
                if url:
                    existing_google_urls.add(url)

        if not all_leads:
            _log(
                scrape_run_id,
                f"Google Search — query {query_num} empty",
                "No new leads extracted from SERP pages.",
                level="warning",
            )
            continue

        _log(
            scrape_run_id,
            f"Google Search — query {query_num} saving",
            f"{len(all_leads)} new lead(s) from "
            f"{len(serp_pages)} SERP page(s). Saving now…",
        )

        # LimitReached bubbles up to run_pipeline
        _save_lead_batch(
            all_leads, stats, scrape_run_id,
            source_key="google", max_leads=max_leads,
        )

        gg_saved   = stats.get("source_saved",   {}).get("google", 0)
        gg_skipped = stats.get("source_skipped", {}).get("google", 0)

        _log(
            scrape_run_id,
            f"Google Search — query {query_num} saved",
            f"{len(all_leads)} processed — "
            f"{gg_saved} GG leads saved, "
            f"{gg_skipped} duplicate(s).",
            level="success",
        )

        if contacts_map:
            _enrich_saved_google_leads(contacts_map, scrape_run_id, svc_log)

        if _cancelled(scrape_run_id):
            _log(
                scrape_run_id, "Google Search — cancelled",
                f"Cancelled after query {query_num}. "
                f"{gg_saved} GG lead(s) saved so far.",
                level="warning",
            )
            return

    if query_num > 0:
        gg_saved = stats.get("source_saved", {}).get("google", 0)
        _log(
            scrape_run_id, "Google Search — complete",
            f"All {query_num} "
            f"quer{'y' if query_num == 1 else 'ies'} done. "
            f"{gg_saved} GG lead(s) saved.",
            level="success",
        )
    else:
        _log(
            scrape_run_id, "Google Search — no results",
            "Actor returned 0 results for all queries.",
            level="warning",
        )


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