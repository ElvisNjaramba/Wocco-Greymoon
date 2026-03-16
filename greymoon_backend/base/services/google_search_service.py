"""
google_search_service.py
Apify actor: apify~google-search-scraper   (SERP)
Apify actor: apify~website-content-crawler (contact extraction)

How it works
────────────
For each search query the pipeline runs TWO actors in parallel:

  ┌─────────────────────────┐     ┌──────────────────────────────┐
  │  SERP actor             │     │  Website crawler             │
  │  apify~google-search-   │     │  apify~website-content-      │
  │  scraper                │     │  crawler                     │
  │                         │     │                              │
  │  • Organic results      │     │  • Visits every contractor   │
  │  • AI Mode answers      │     │    URL found in the SERP     │
  │  • AI Overviews         │     │  • Also crawls /contact and  │
  │  • Paid / ad results    │     │    /contact-us variants      │
  │  • Leads enrichment     │     │  • Renders full JS via       │
  │    (email/phone/LinkedIn│     │    Playwright so dynamically │
  │     from enrichment DB) │     │    loaded phone numbers are  │
  └─────────────────────────┘     │    visible                   │
           both launched          │  • Saves contacts per-site   │
           at the same time       │    immediately — no data lost│
                                  │    if the run is cancelled   │
                                  └──────────────────────────────┘

The generator yields one bundle per query:
  { "serp_pages": [...], "contacts_map": { "https://domain.com": { phones, emails } } }

The pipeline normaliser merges SERP results with contacts_map so every
lead gets the best available phone/email regardless of which source
provided it.
"""

import threading
import time
import re
import requests
from django.conf import settings

# ── Actor IDs ─────────────────────────────────────────────────
SERP_ACTOR_ID       = "apify~google-search-scraper"
CRAWL_ACTOR_ID      = "apify~website-content-crawler"

POLL_INTERVAL       = 5     # seconds between status checks
DATASET_FETCH_LIMIT = 500   # max items fetched from any single dataset
DEFAULT_MAX_PAGES   = 3     # SERP pages per query (~10 results each)

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
    """
    Fire off an Apify actor run.
    Registers the run ID immediately so cancel can abort it.
    Returns (run_id, dataset_id) or None if cancelled.
    """
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
    """Single status check — returns Apify status string."""
    res = requests.get(
        f"https://api.apify.com/v2/actor-runs/{run_id}",
        headers=_apify_headers(),
    )
    res.raise_for_status()
    return res.json()["data"]["status"]


# ── Contact extraction from crawled page text ─────────────────

def _extract_contacts(text: str) -> dict:
    """Return { phones: [...], emails: [...] } found in page text."""
    if not text:
        return {"phones": [], "emails": []}
    phones = list(dict.fromkeys(
        m.strip() for m in re.findall(
            r"(\+?1?\s?[\(\-]?\d{3}[\)\-\s]?\s?\d{3}[\-\s]?\d{4})", text
        )
    ))
    emails = list(dict.fromkeys(
        m.lower() for m in re.findall(
            r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+", text
        )
        if not any(x in m.lower() for x in [
            "noreply", "no-reply", ".png", ".jpg", ".gif",
            ".svg", ".css", ".js", "example.com", "sentry.io",
        ])
    ))
    return {"phones": phones[:5], "emails": emails[:5]}


def _base_domain(url: str) -> str:
    """https://www.buizawaste.com/about → https://www.buizawaste.com"""
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
    Queues the root URL + /contact + /contact-us for every site.
    Uses Playwright so JavaScript-rendered contact info is visible.
    """
    start_urls = []
    for u in urls:
        base = u.rstrip("/")
        start_urls.append({"url": u})
        start_urls.append({"url": f"{base}/contact"})
        start_urls.append({"url": f"{base}/contact-us"})
    return {
        "startUrls":             start_urls,
        "maxCrawlPagesPerCrawl": 5,
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
    contacts_map: dict,        # shared dict — written from this thread
    contacts_lock: threading.Lock,
    crawl_done: threading.Event,
    scrape_run_id,
    log,
):
    """
    Runs in a background thread alongside the SERP actor.
    Crawls sites one-by-one and writes contacts into contacts_map
    immediately after each site — so even a cancelled run saves whatever
    was already extracted.
    """
    total = len(urls_to_crawl)
    log(f"[Google Website Crawl] Starting crawler on {total} contractor site(s) "
        f"(Playwright + JS rendering, /contact pages included)")

    # We crawl in small batches of 5 so results are saved progressively
    BATCH = 5
    batches = [urls_to_crawl[i:i+BATCH] for i in range(0, total, BATCH)]

    for b_idx, batch in enumerate(batches):
        if _is_cancel_requested(scrape_run_id):
            log(f"[Google Website Crawl] Cancel requested — stopping after batch {b_idx} "
                f"({b_idx * BATCH} of {total} sites crawled)")
            break

        batch_domains = ", ".join(u.split("/")[2] for u in batch if len(u.split("/")) > 2)
        log(f"[Google Website Crawl] Batch {b_idx+1}/{len(batches)} — "
            f"crawling: {batch_domains}")

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
        log(f"[Google Website Crawl] Batch {b_idx+1} actor running (run {run_id}) — "
            f"waiting for Playwright to render {len(batch) * 3} page(s)...")

        # Poll until done or cancelled
        finished = False
        poll_count = 0
        while True:
            if _is_cancel_requested(scrape_run_id):
                log(f"[Google Website Crawl] Cancel detected mid-crawl — "
                    f"aborting batch {b_idx+1} and saving partial contacts")
                _abort_apify_run(run_id)
                break
            try:
                status = _poll_run_status(run_id)
            except Exception as e:
                log(f"[Google Website Crawl] Status check error for batch {b_idx+1}: {e}")
                break

            poll_count += 1
            if status == "SUCCEEDED":
                finished = True
                log(f"[Google Website Crawl] Batch {b_idx+1} finished — "
                    f"extracting contacts from crawled pages...")
                break
            elif status in ("FAILED", "ABORTED", "TIMED-OUT"):
                log(f"[Google Website Crawl] Batch {b_idx+1} ended with status: {status} — "
                    f"saving any partial results")
                break
            else:
                # Log a meaningful progress message every 3 polls (~15 seconds)
                if poll_count % 3 == 0:
                    elapsed = poll_count * POLL_INTERVAL
                    log(f"[Google Website Crawl] Batch {b_idx+1} still running "
                        f"({elapsed}s elapsed, status: {status})...")
                time.sleep(POLL_INTERVAL)

        # Always try to fetch whatever was crawled — even on failure/cancel
        try:
            pages = _fetch_dataset(dataset_id)
        except Exception as e:
            log(f"[Google Website Crawl] Could not fetch dataset for batch {b_idx+1}: {e}")
            continue

        # Extract and immediately save contacts per site
        batch_found = 0
        for page in pages:
            page_url  = page.get("url") or page.get("loadedUrl") or ""
            page_text = (
                page.get("text") or
                page.get("markdown") or
                page.get("html") or ""
            )
            if not page_url or not page_text:
                continue

            contacts = _extract_contacts(page_text)
            if not contacts["phones"] and not contacts["emails"]:
                continue

            base = _base_domain(page_url)
            with contacts_lock:
                if base not in contacts_map:
                    contacts_map[base] = {"phones": [], "emails": []}
                contacts_map[base]["phones"] = list(dict.fromkeys(
                    contacts_map[base]["phones"] + contacts["phones"]
                ))[:5]
                contacts_map[base]["emails"] = list(dict.fromkeys(
                    contacts_map[base]["emails"] + contacts["emails"]
                ))[:5]
            batch_found += 1

        phones_found = sum(len(v["phones"]) for v in contacts_map.values())
        emails_found = sum(len(v["emails"]) for v in contacts_map.values())
        log(f"[Google Website Crawl] Batch {b_idx+1} complete — "
            f"{batch_found} site(s) yielded contacts "
            f"(running totals: {phones_found} phone(s), {emails_found} email(s) "
            f"across {len(contacts_map)} site(s))")

    total_sites  = len(contacts_map)
    total_phones = sum(len(v["phones"]) for v in contacts_map.values())
    total_emails = sum(len(v["emails"]) for v in contacts_map.values())
    log(f"[Google Website Crawl] All batches complete — "
        f"{total_sites} site(s) with contact data, "
        f"{total_phones} phone number(s), {total_emails} email address(es) extracted")

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
):
    """
    Generator — yields one bundle per query:
        { "serp_pages": [...], "contacts_map": { domain: { phones, emails } } }

    For each query:
      1. SERP actor is launched immediately.
      2. As soon as SERP returns results, the website crawler is launched
         in a background thread — in parallel, not after.
      3. The generator waits for both to finish (or the crawl timeout),
         then yields the combined bundle.
      4. If cancelled at any point, whatever has been crawled so far is
         still yielded so the pipeline can save partial contacts.
    """
    log = _log_fn or print

    for i, query in enumerate(queries):
        if _is_cancel_requested(scrape_run_id):
            log(f"[Google Search] Cancel requested — stopping before query {i+1}/{len(queries)}")
            return

        log(f"[Google Search] ── Query {i+1}/{len(queries)} ──────────────────────────")
        log(f"[Google Search] Searching: '{query} {location}' "
            f"({max_pages} page(s) ≈ {max_pages * 10} results, "
            f"AI Mode on, paid results on, leads enrichment {'on' if enrich_leads else 'off'})")

        # ── Step 1: Launch SERP actor ─────────────────────────
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
            log(f"[Google Search] Cancelled while launching SERP actor for query {i+1} — stopping")
            return

        serp_run_id, serp_dataset_id = serp_result
        log(f"[Google Search] SERP actor launched (run {serp_run_id}) — "
            f"waiting for Google to return results...")

        # ── Step 2: Poll SERP to completion ───────────────────
        serp_pages = []
        serp_poll_count = 0
        serp_ok = False

        while True:
            if _is_cancel_requested(scrape_run_id):
                log(f"[Google Search] Cancel detected — aborting SERP run and saving partial results")
                _abort_apify_run(serp_run_id)
                try:
                    serp_pages = _fetch_dataset(serp_dataset_id)
                    log(f"[Google Search] Partial SERP: {len(serp_pages)} page(s) saved before cancel")
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

            serp_poll_count += 1

            if status == "SUCCEEDED":
                serp_ok = True
                break
            elif status in ("FAILED", "ABORTED", "TIMED-OUT"):
                log(f"[Google Search] SERP actor ended with: {status}")
                break
            else:
                if serp_poll_count % 3 == 0:
                    elapsed = serp_poll_count * POLL_INTERVAL
                    log(f"[Google Search] SERP running ({elapsed}s elapsed) — "
                        f"Google is processing the search query...")
                time.sleep(POLL_INTERVAL)

        # Fetch SERP results (even on failure — grab partial data)
        try:
            serp_pages = _fetch_dataset(serp_dataset_id)
        except Exception as e:
            log(f"[Google Search] Could not fetch SERP dataset: {e}")
            serp_pages = []

        if not serp_ok:
            if serp_pages:
                log(f"[Google Search] SERP ended early but recovered {len(serp_pages)} page(s) — "
                    f"yielding partial results")
                yield {"serp_pages": serp_pages, "contacts_map": {}}
            else:
                log(f"[Google Search] SERP returned no results for query {i+1} — skipping")
            continue

        # Count what the SERP returned
        total_organic = sum(len(p.get("organicResults") or []) for p in serp_pages)
        total_paid    = sum(len(p.get("paidResults")    or []) for p in serp_pages)
        total_leads   = sum(len(p.get("businessLeads")  or []) for p in serp_pages)
        has_ai        = any(p.get("aiModeResult") or p.get("aiOverview") for p in serp_pages)

        log(f"[Google Search] SERP complete — {len(serp_pages)} page(s) returned: "
            f"{total_organic} organic result(s), "
            f"{total_paid} paid ad(s), "
            f"{total_leads} enriched lead(s)"
            + (", AI answer included" if has_ai else ""))

        # ── Step 3: Launch website crawler IN PARALLEL ────────
        contacts_map  = {}
        crawl_thread  = None
        crawl_done    = threading.Event()
        contacts_lock = threading.Lock()

        if deep_scrape_sites and not _is_cancel_requested(scrape_run_id):
            # Collect unique contractor URLs from organic results
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
                    if domain not in seen_domains:
                        seen_domains.add(domain)
                        urls_to_crawl.append(url)

            if urls_to_crawl:
                log(f"[Google Search] Launching website crawler in parallel — "
                    f"{len(urls_to_crawl)} contractor site(s) to crawl "
                    f"(Playwright, JS rendering, /contact pages included)")
                crawl_thread = threading.Thread(
                    target=_run_crawl_parallel,
                    args=(
                        urls_to_crawl,
                        contacts_map,
                        contacts_lock,
                        crawl_done,
                        scrape_run_id,
                        log,
                    ),
                    daemon=True,
                )
                crawl_thread.start()
            else:
                log(f"[Google Search] No contractor URLs to crawl for query {i+1} "
                    f"(all results were directories or skipped domains)")
                crawl_done.set()
        else:
            crawl_done.set()

        # ── Step 4: Wait for crawl to finish ──────────────────
        # Cap wait at 8 minutes — if sites are slow we don't block forever
        CRAWL_TIMEOUT = 480
        if crawl_thread and not crawl_done.wait(timeout=CRAWL_TIMEOUT):
            log(f"[Google Search] Website crawl timeout ({CRAWL_TIMEOUT}s) — "
                f"yielding whatever contacts were extracted so far "
                f"({len(contacts_map)} site(s) with data)")

        # ── Step 5: Yield combined bundle ─────────────────────
        with contacts_lock:
            snapshot = dict(contacts_map)

        sites_with_phone = sum(1 for v in snapshot.values() if v["phones"])
        sites_with_email = sum(1 for v in snapshot.values() if v["emails"])
        log(f"[Google Search] Query {i+1} complete — yielding to pipeline: "
            f"{len(serp_pages)} SERP page(s), "
            f"{len(snapshot)} site(s) crawled "
            f"({sites_with_phone} with phone, {sites_with_email} with email)")

        yield {"serp_pages": serp_pages, "contacts_map": snapshot}