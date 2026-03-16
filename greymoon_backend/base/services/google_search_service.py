"""
google_search_service.py

Bugs fixed in this version
──────────────────────────
1. enrich_callback was never passed into scrape_google_search_progressive
   from pipeline.py — contacts extracted by the crawler were written into
   contacts_map but the callback to update already-saved leads was never
   called.  Fixed: enrich_callback is now wired all the way through.

2. Phone regex missed dot-separated numbers (555.123.4567) which are
   extremely common on contractor websites.  Also missed bare 10-digit
   numbers in href="tel:5551234567" links (the most reliable contact
   signal on modern sites).  Fixed: three-pattern extraction approach.

3. maxCrawlPagesPerCrawl was hard-coded to 5 but each batch sends up to
   15 URLs (root + /contact + /contact-us per site × 5 sites).  Only
   the first 5 pages were ever fetched — /contact pages were silently
   skipped.  Fixed: maxCrawlPagesPerCrawl = len(urls) * 3.

4. contacts_map keys are base-domain strings like "https://www.example.com"
   but _enrich_saved_google_leads matched via url__startswith.  If a saved
   lead had www. and the crawled page key didn't (or vice versa) the match
   failed silently.  Fixed: normalise both sides by stripping www. before
   storing/comparing.
"""

import threading
import time
import re
import requests

from django.conf import settings

# ── Actor IDs ─────────────────────────────────────────────────
SERP_ACTOR_ID       = "apify~google-search-scraper"
CRAWL_ACTOR_ID      = "apify~website-content-crawler"

POLL_INTERVAL       = 5
DATASET_FETCH_LIMIT = 500
DEFAULT_MAX_PAGES   = 3
CRAWL_TIMEOUT       = 480   # seconds to wait for crawler per query
CRAWL_BATCH_SIZE    = 5     # sites per crawler actor run

SKIP_DOMAINS = {
    "yelp.com", "angi.com", "thumbtack.com", "homeadvisor.com",
    "homedepot.com", "lowes.com", "amazon.com", "indeed.com",
    "linkedin.com", "facebook.com", "instagram.com", "youtube.com",
    "bbb.org", "angieslist.com", "taskrabbit.com", "craigslist.org",
    "nextdoor.com", "google.com", "twitter.com", "x.com",
}

# ── Apify helpers ─────────────────────────────────────────────

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
        print(f"[Google] Could not abort run {run_id}: {e}")


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
        print(f"[Google] Could not register Apify run ID: {e}")


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


def _launch_actor(actor_id: str, payload: dict, scrape_run_id) -> tuple[str, str] | None:
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


def _fetch_dataset(dataset_id: str) -> list[dict]:
    resp = requests.get(
        f"https://api.apify.com/v2/datasets/{dataset_id}"
        f"/items?clean=true&limit={DATASET_FETCH_LIMIT}",
        headers=_apify_headers(),
    )
    resp.raise_for_status()
    return resp.json()


def _poll_run_status(run_id: str) -> str:
    res = requests.get(
        f"https://api.apify.com/v2/actor-runs/{run_id}",
        headers=_apify_headers(),
    )
    res.raise_for_status()
    return res.json()["data"]["status"]


# ── Contact extraction ────────────────────────────────────────
#
# Three-pattern approach to maximise coverage on contractor websites:
#
#   Pattern 1 — formatted numbers with any separator (-, space, dot):
#     Covers (555) 123-4567  555-123-4567  555.123.4567  555 123 4567
#     +1-800-555-1234  1.800.555.1234  (555)123-4567
#
#   Pattern 2 — href="tel:" with formatted number:
#     Covers <a href="tel:(555)123-4567"> style links.
#
#   Pattern 3 — href="tel:" with raw 10-digit number:
#     Covers <a href="tel:5551234567"> which is the most common way
#     modern websites embed click-to-call links.  The 10 digits are
#     reformatted to (NXX) NXX-XXXX for storage consistency.
#
# Why not one regex?
#   A single regex broad enough to catch dots AND bare digits would also
#   match IP addresses (192.168.1.1), version numbers (3.14.159), and
#   dates (2024.01.15).  Splitting by context avoids all false positives.

_PHONE_FORMATTED = re.compile(
    r'(\+?1?[\s\.\-]?[\(\-]?\d{3}[\)\.\-\s][\.\-\s]?\d{3}[\.\-\s]\d{4})'
)
_PHONE_TEL_HREF_FORMATTED = re.compile(
    r'href=["\']tel:[\+]?1?[\-\s\.]?(\(?\d{3}\)?[\-\s\.]?\d{3}[\-\s\.]\d{4})'
)
_PHONE_TEL_HREF_RAW = re.compile(
    r'href=["\']tel:[\+]?1?(\d{10})["\']'
)
_EMAIL_RE = re.compile(
    r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+'
)
_EMAIL_SKIP = {
    "noreply", "no-reply", ".png", ".jpg", ".gif", ".svg",
    ".css", ".js", "example.com", "sentry.io",
    "wixpress.com", "squarespace.com", "wordpress.com",
}


def _extract_contacts(text: str) -> dict:
    """
    Extract phone numbers and email addresses from crawled page text.
    Returns {"phones": [...], "emails": [...]}.
    """
    if not text:
        return {"phones": [], "emails": []}

    # Pattern 1: formatted numbers — dashes, spaces, or dots as separators
    phones_formatted = [m.strip() for m in _PHONE_FORMATTED.findall(text)]

    # Pattern 2: tel: href with formatted number (most reliable, check first)
    phones_tel_fmt = _PHONE_TEL_HREF_FORMATTED.findall(text)

    # Pattern 3: tel: href with raw 10 digits — reformat to (NXX) NXX-XXXX
    phones_tel_raw = [
        f"({d[:3]}) {d[3:6]}-{d[6:]}"
        for d in _PHONE_TEL_HREF_RAW.findall(text)
    ]

    # Merge — tel: href results first (most reliable source), then formatted
    all_phones = list(dict.fromkeys(
        phones_tel_fmt + phones_tel_raw + phones_formatted
    ))

    emails = list(dict.fromkeys(
        m.lower() for m in _EMAIL_RE.findall(text)
        if not any(skip in m.lower() for skip in _EMAIL_SKIP)
    ))

    return {"phones": all_phones[:5], "emails": emails[:5]}


def _normalise_domain(base_url: str) -> str:
    """
    Strip www. so https://www.example.com and https://example.com
    both map to the same contacts_map key.
    """
    return base_url.replace("://www.", "://")


def _base_domain(url: str) -> str:
    """https://www.example.com/about  →  https://www.example.com"""
    try:
        parts = url.split("/")
        return f"{parts[0]}//{parts[2]}"
    except Exception:
        return url


# ── SERP payload ──────────────────────────────────────────────

def build_google_payload(
    query: str,
    location: str,
    max_pages: int = DEFAULT_MAX_PAGES,
    enrich_leads: bool = True,
) -> dict:
    return {
        "queries":                  f"{query} {location}".strip(),
        "maxPagesPerQuery":         max_pages,
        "countryCode":              "us",
        "languageCode":             "en",
        "resultsPerPage":           10,
        "mobileResults":            False,
        "includeUnfilteredResults": True,
        "maxConcurrency":           5,
        "scrapeAiMode":             True,
        "scrapeAiOverview":         True,
        "includePaidResults":       True,
        **({"includeLeadsEnrichment": True} if enrich_leads else {}),
    }


# ── Crawler payload ───────────────────────────────────────────

def build_crawl_payload(urls: list[str]) -> dict:
    """
    Build crawler input for a batch of contractor URLs.

    Queues root + /contact + /contact-us for every site.

    Bug fix: maxCrawlPagesPerCrawl is now len(urls) * 3 instead of the
    previous hard-coded 5.  With 5 sites per batch = 15 queued URLs,
    the old limit of 5 meant only root pages were ever fetched and
    /contact pages were silently dropped.
    """
    start_urls = []
    for u in urls:
        base = u.rstrip("/")
        start_urls.append({"url": u})
        start_urls.append({"url": f"{base}/contact"})
        start_urls.append({"url": f"{base}/contact-us"})

    return {
        "startUrls":             start_urls,
        "maxCrawlPagesPerCrawl": len(urls) * 3,   # ← was hard-coded 5
        "maxCrawlDepth":         1,
        "crawlerType":           "playwright:chrome",
        "proxyConfiguration": {
            "useApifyProxy":     True,
            "apifyProxyGroups":  ["RESIDENTIAL"],
            "apifyProxyCountry": "US",
        },
    }


# ── Parallel crawl thread ─────────────────────────────────────

def _run_crawl_parallel(
    urls_to_crawl: list[str],
    contacts_map: dict,
    contacts_lock: threading.Lock,
    crawl_done: threading.Event,
    enrich_callback,        # callable(contacts_map) called after each batch
    scrape_run_id,
    log,
):
    """
    Runs in a background thread alongside the SERP actor.
    Crawls sites in batches and writes contacts into contacts_map
    immediately after each batch.  Calls enrich_callback after every
    batch so pipeline.py can update already-saved leads in real time.
    """
    total = len(urls_to_crawl)
    log(
        f"[Google Website Crawl] Starting crawler on {total} contractor site(s) "
        f"(Playwright + JS rendering, /contact + /contact-us pages included)"
    )

    batches = [
        urls_to_crawl[i:i + CRAWL_BATCH_SIZE]
        for i in range(0, total, CRAWL_BATCH_SIZE)
    ]

    for b_idx, batch in enumerate(batches):
        if _is_cancel_requested(scrape_run_id):
            log(
                f"[Google Website Crawl] Cancel requested — stopping after batch {b_idx} "
                f"({b_idx * CRAWL_BATCH_SIZE} of {total} sites crawled)"
            )
            break

        batch_domains = ", ".join(
            u.split("/")[2] for u in batch if len(u.split("/")) > 2
        )
        log(
            f"[Google Website Crawl] Batch {b_idx+1}/{len(batches)} — "
            f"crawling: {batch_domains}"
        )

        payload = build_crawl_payload(batch)
        try:
            result = _launch_actor(CRAWL_ACTOR_ID, payload, scrape_run_id)
        except Exception as e:
            log(f"[Google Website Crawl] Failed to launch crawler batch {b_idx+1}: {e}")
            continue

        if result is None:
            log(f"[Google Website Crawl] Cancelled during launch of batch {b_idx+1}")
            break

        run_id, dataset_id = result
        expected_pages = len(batch) * 3
        log(
            f"[Google Website Crawl] Batch {b_idx+1} actor running (run {run_id}) — "
            f"waiting for Playwright to render up to {expected_pages} page(s)..."
        )

        poll_count = 0
        while True:
            if _is_cancel_requested(scrape_run_id):
                log(
                    f"[Google Website Crawl] Cancel detected mid-crawl — "
                    f"aborting batch {b_idx+1} and saving partial contacts"
                )
                _abort_apify_run(run_id)
                break

            try:
                status = _poll_run_status(run_id)
            except Exception as e:
                log(f"[Google Website Crawl] Status check error for batch {b_idx+1}: {e}")
                break

            poll_count += 1
            if status == "SUCCEEDED":
                log(
                    f"[Google Website Crawl] Batch {b_idx+1} finished — "
                    f"extracting contacts from crawled pages..."
                )
                break
            elif status in ("FAILED", "ABORTED", "TIMED-OUT"):
                log(
                    f"[Google Website Crawl] Batch {b_idx+1} ended with status: {status} — "
                    f"saving any partial results"
                )
                break
            else:
                if poll_count % 3 == 0:
                    elapsed = poll_count * POLL_INTERVAL
                    log(
                        f"[Google Website Crawl] Batch {b_idx+1} still running "
                        f"({elapsed}s elapsed, status: {status})..."
                    )
                time.sleep(POLL_INTERVAL)

        # Always fetch whatever was crawled — even on failure/cancel
        try:
            pages = _fetch_dataset(dataset_id)
        except Exception as e:
            log(f"[Google Website Crawl] Could not fetch dataset for batch {b_idx+1}: {e}")
            continue

        batch_found = 0
        for page in pages:
            page_url  = page.get("url") or page.get("loadedUrl") or ""
            page_text = (
                page.get("text") or
                page.get("markdown") or
                page.get("html") or
                page.get("content") or
                ""
            )
            if not page_url or not page_text:
                continue

            contacts = _extract_contacts(page_text)
            if not contacts["phones"] and not contacts["emails"]:
                continue

            # Store under normalised key (no www.) so it matches saved
            # lead URLs regardless of whether they include www.
            base     = _base_domain(page_url)
            norm_key = _normalise_domain(base)

            with contacts_lock:
                if norm_key not in contacts_map:
                    contacts_map[norm_key] = {"phones": [], "emails": []}
                contacts_map[norm_key]["phones"] = list(dict.fromkeys(
                    contacts_map[norm_key]["phones"] + contacts["phones"]
                ))[:5]
                contacts_map[norm_key]["emails"] = list(dict.fromkeys(
                    contacts_map[norm_key]["emails"] + contacts["emails"]
                ))[:5]
            batch_found += 1

        phones_found = sum(len(v["phones"]) for v in contacts_map.values())
        emails_found = sum(len(v["emails"]) for v in contacts_map.values())
        log(
            f"[Google Website Crawl] Batch {b_idx+1} complete — "
            f"{batch_found}/{len(batch)} site(s) yielded contacts "
            f"(running totals: {phones_found} phone(s), {emails_found} email(s) "
            f"across {len(contacts_map)} site(s))"
        )

        # Call enrich_callback after every batch so pipeline.py updates
        # already-saved leads with freshly extracted contact data
        if enrich_callback and contacts_map:
            try:
                with contacts_lock:
                    snapshot = dict(contacts_map)
                enrich_callback(snapshot)
            except Exception as e:
                log(f"[Google Website Crawl] Enrich callback error after batch {b_idx+1}: {e}")

    total_sites  = len(contacts_map)
    total_phones = sum(len(v["phones"]) for v in contacts_map.values())
    total_emails = sum(len(v["emails"]) for v in contacts_map.values())
    log(
        f"[Google Website Crawl] All batches complete — "
        f"{total_sites} site(s) with contact data, "
        f"{total_phones} phone number(s), {total_emails} email address(es) extracted"
    )
    crawl_done.set()


# ── Main SERP generator ───────────────────────────────────────

def scrape_google_search_progressive(
    queries: list[str],
    location: str,
    max_pages: int = DEFAULT_MAX_PAGES,
    enrich_leads: bool = True,
    deep_scrape_sites: bool = True,
    scrape_run_id=None,
    _log_fn=None,
    enrich_callback=None,
):
    """
    Generator — yields one bundle per query as soon as the SERP finishes:

        { "serp_pages": [...], "contacts_map": {} }

    The website crawler runs in a background thread and populates
    contacts_map incrementally.  enrich_callback(contacts_map) is called
    after each crawl batch so pipeline.py can update already-saved leads
    with contact data as it arrives.
    """
    log = _log_fn or print

    for i, query in enumerate(queries):
        if _is_cancel_requested(scrape_run_id):
            log(f"[Google Search] Cancel requested — stopping before query {i+1}/{len(queries)}")
            return

        log(f"[Google Search] ── Query {i+1}/{len(queries)} ──────────────────────────")
        log(
            f"[Google Search] Searching: '{query} {location}' "
            f"({max_pages} page(s) ≈ {max_pages * 10} results, "
            f"AI Mode on, paid results on, "
            f"leads enrichment {'on' if enrich_leads else 'off'})"
        )

        # ── Launch SERP actor ──────────────────────────────────
        payload = build_google_payload(
            query, location,
            max_pages=max_pages,
            enrich_leads=enrich_leads,
        )
        try:
            serp_result = _launch_actor(SERP_ACTOR_ID, payload, scrape_run_id)
        except Exception as e:
            log(f"[Google Search] Failed to launch SERP actor for query {i+1}: {e}")
            continue

        if serp_result is None:
            log("[Google Search] Cancelled while launching SERP actor — stopping")
            return

        serp_run_id, serp_dataset_id = serp_result
        log(f"[Google Search] SERP actor launched (run {serp_run_id}) — waiting for results...")

        # ── Poll SERP to completion ────────────────────────────
        serp_pages = []
        serp_poll  = 0
        serp_ok    = False

        while True:
            if _is_cancel_requested(scrape_run_id):
                log("[Google Search] Cancel detected — aborting SERP run")
                _abort_apify_run(serp_run_id)
                try:
                    serp_pages = _fetch_dataset(serp_dataset_id)
                except Exception:
                    pass
                if serp_pages:
                    yield {"serp_pages": serp_pages, "contacts_map": {}}
                return

            try:
                status = _poll_run_status(serp_run_id)
            except Exception as e:
                log(f"[Google Search] SERP status check error: {e} — retrying...")
                time.sleep(POLL_INTERVAL)
                continue

            serp_poll += 1
            if status == "SUCCEEDED":
                serp_ok = True
                break
            elif status in ("FAILED", "ABORTED", "TIMED-OUT"):
                log(f"[Google Search] SERP actor ended with: {status}")
                break
            else:
                if serp_poll % 3 == 0:
                    log(
                        f"[Google Search] SERP running ({serp_poll * POLL_INTERVAL}s) — "
                        f"Google is processing the query..."
                    )
                time.sleep(POLL_INTERVAL)

        try:
            serp_pages = _fetch_dataset(serp_dataset_id)
        except Exception as e:
            log(f"[Google Search] Could not fetch SERP dataset: {e}")
            serp_pages = []

        if not serp_ok:
            if serp_pages:
                log(f"[Google Search] SERP ended early — recovered {len(serp_pages)} page(s)")
                yield {"serp_pages": serp_pages, "contacts_map": {}}
            else:
                log(f"[Google Search] SERP returned no results for query {i+1} — skipping")
            continue

        total_organic = sum(len(p.get("organicResults") or []) for p in serp_pages)
        total_paid    = sum(len(p.get("paidResults")    or []) for p in serp_pages)
        total_leads   = sum(len(p.get("businessLeads")  or []) for p in serp_pages)
        has_ai        = any(p.get("aiModeResult") or p.get("aiOverview") for p in serp_pages)

        log(
            f"[Google Search] SERP complete — {len(serp_pages)} page(s): "
            f"{total_organic} organic, {total_paid} paid, {total_leads} enriched"
            + (", AI answer included" if has_ai else "")
        )

        # ── Launch website crawler IN BACKGROUND ──────────────
        contacts_map  = {}
        crawl_thread  = None
        crawl_done    = threading.Event()
        contacts_lock = threading.Lock()

        if deep_scrape_sites and not _is_cancel_requested(scrape_run_id):
            urls_to_crawl = []
            seen_domains  = set()

            for page in serp_pages:
                for result in (page.get("organicResults") or []):
                    url = result.get("url") or ""
                    if not url or any(d in url for d in SKIP_DOMAINS):
                        continue
                    try:
                        domain = url.split("/")[2]
                    except IndexError:
                        continue
                    # Normalise domain for dedup (strip www.)
                    norm_domain = domain.replace("www.", "", 1)
                    if norm_domain not in seen_domains:
                        seen_domains.add(norm_domain)
                        urls_to_crawl.append(url)

            if urls_to_crawl:
                log(
                    f"[Google Search] Launching website crawler in background — "
                    f"{len(urls_to_crawl)} contractor site(s) to crawl"
                )
                crawl_thread = threading.Thread(
                    target=_run_crawl_parallel,
                    args=(
                        urls_to_crawl,
                        contacts_map,
                        contacts_lock,
                        crawl_done,
                        enrich_callback,    # ← wired through correctly
                        scrape_run_id,
                        log,
                    ),
                    daemon=True,
                )
                crawl_thread.start()
            else:
                log(f"[Google Search] No contractor URLs to crawl for query {i+1}")
                crawl_done.set()
        else:
            crawl_done.set()

        # ── Yield SERP results IMMEDIATELY ────────────────────
        # contacts_map is likely empty here — that is intentional.
        # pipeline.py saves the leads now; enrich_callback fills in
        # contacts as each crawl batch finishes.
        with contacts_lock:
            snapshot = dict(contacts_map)

        log(
            f"[Google Search] Query {i+1} — yielding {len(serp_pages)} SERP page(s) now"
            + (
                f" ({len(snapshot)} site(s) already crawled)"
                if snapshot else " (crawler running in background)"
            )
        )
        yield {"serp_pages": serp_pages, "contacts_map": snapshot}

        # ── Wait for crawler to finish (capped) ───────────────
        if crawl_thread and not crawl_done.wait(timeout=CRAWL_TIMEOUT):
            log(
                f"[Google Search] Website crawl timeout ({CRAWL_TIMEOUT}s) — "
                f"moving to next query; crawler thread continues enriching in background"
            )

        # Final enrich pass once crawler fully finishes — catches any
        # batches that completed after the initial yield
        with contacts_lock:
            final_snapshot = dict(contacts_map)

        if final_snapshot and enrich_callback:
            try:
                enrich_callback(final_snapshot)
            except Exception as e:
                log(f"[Google Search] Final enrich callback error: {e}")