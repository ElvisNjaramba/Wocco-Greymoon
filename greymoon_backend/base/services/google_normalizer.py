"""
google_normalizer.py

Maps raw Google Search Results Scraper output → unified ServiceLead dicts.

Handles all result types:
  - organicResults[]       standard organic results
  - businessLeads[]        enriched contacts (leads enrichment add-on)
  - aiModeResult{}         Google AI Mode answer + sources
  - aiOverview{}           AI Overview snippet + sources
  - peopleAlsoAsk[]        PAA questions + answers (useful for scoring/context)
  - paidResults[]          paid/ad results (contractors running ads = high intent)
  - contacts_map{}         contacts extracted by website deep-scrape

Priority order for contact data:
  1. businessLeads enrichment (most reliable)
  2. Website deep-scrape (phone/email from actual company site)
  3. Snippet/description mining (fallback)
"""

import hashlib
import json
import re


# ── Shared helpers ────────────────────────────────────────────

def _extract_emails(text: str) -> str | None:
    if not text:
        return None
    matches = re.findall(
        r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+", text
    )
    return matches[0] if matches else None


def _extract_phones(text: str) -> str | None:
    if not text:
        return None
    matches = re.findall(
        r"(\+?1?\s?[\(\-]?\d{3}[\)\-\s]?\s?\d{3}[\-\s]?\d{4})", text
    )
    return matches[0].strip() if matches else None


def _content_hash(data: dict) -> str:
    key = json.dumps(data, sort_keys=True, default=str)
    return hashlib.sha256(key.encode()).hexdigest()


def _make_post_id(url: str) -> str:
    return hashlib.md5(url.encode()).hexdigest()


def _domain_from_url(url: str) -> str:
    try:
        return url.split("/")[2]
    except IndexError:
        return ""


def _base_url(url: str) -> str:
    """Return https://domain.com from any URL."""
    try:
        parts = url.split("/")
        return f"{parts[0]}//{parts[2]}"
    except IndexError:
        return url


SKIP_DOMAINS = {
    "yelp.com", "angi.com", "thumbtack.com", "homeadvisor.com",
    "homedepot.com", "lowes.com", "amazon.com", "indeed.com",
    "linkedin.com", "facebook.com", "instagram.com", "youtube.com",
    "bbb.org", "angieslist.com", "taskrabbit.com", "craigslist.org",
    "nextdoor.com", "google.com", "twitter.com", "x.com",
}


def _is_directory(url: str) -> bool:
    if not url:
        return False
    return any(d in url for d in SKIP_DOMAINS)


# ── Lead builder ──────────────────────────────────────────────

def _build_lead(
    *,
    title: str,
    url: str,
    description: str,
    phone: str | None,
    email: str | None,
    location: str,
    service_category: str,
    search_query: str,
    extra_raw: dict,
    lead_type: str = "organic",
) -> dict | None:
    if not url:
        return None
    post_id   = _make_post_id(url)
    post_body = description or ""
    normalized = {
        "post_id":          post_id,
        "url":              url,
        "title":            title[:500],
        "post":             post_body,
        "phone":            phone or None,
        "email":            email or None,
        "location":         location,
        "category":         service_category,
        "service_category": service_category,
        "state":            "",
        "latitude":         "",
        "longitude":        "",
        "map_accuracy":     "",
        "datetime":         None,
        "source":           "GOOGLE",
        "zip_code":         "",
        "fb_group_name":    "",
        "fb_group_url":     "",
        "raw_json": {
            **extra_raw,
            "_google_query":    search_query,
            "_google_url":      url,
            "_google_title":    title,
            "_lead_type":       lead_type,
        },
    }
    normalized["content_hash"] = _content_hash({
        "url":   url,
        "title": title,
        "post":  post_body[:300],
    })
    return normalized


# ── Contact resolution (enrichment > crawl > snippet) ─────────

def _resolve_contacts(
    *,
    enrich_phone: str,
    enrich_email: str,
    url: str,
    snippet: str,
    contacts_map: dict,
) -> tuple[str | None, str | None]:
    """
    Return (best_phone, best_email) using priority:
      1. Leads enrichment data
      2. Website deep-scrape contacts_map (keyed by base URL / domain)
      3. Snippet/description mining
    """
    phone = enrich_phone or ""
    email = enrich_email or ""

    if not phone or not email:
        # Try contacts_map by base URL
        base = _base_url(url)
        domain = _domain_from_url(url)
        crawled = contacts_map.get(base) or contacts_map.get(domain) or {}
        if not phone and crawled.get("phones"):
            phone = crawled["phones"][0]
        if not email and crawled.get("emails"):
            email = crawled["emails"][0]

    if not phone:
        phone = _extract_phones(snippet) or ""
    if not email:
        email = _extract_emails(snippet) or ""

    return phone or None, email or None


# ── Per-result-type normalizers ───────────────────────────────

def _from_enriched_lead(
    lead: dict,
    organic: dict,
    service_category: str,
    location: str,
    search_query: str,
    contacts_map: dict,
) -> dict | None:
    full_name    = lead.get("fullName") or ""
    job_title    = lead.get("jobTitle") or ""
    company_name = lead.get("companyName") or organic.get("title", "")
    domain       = lead.get("companyDomain") or ""
    linkedin     = lead.get("linkedInUrl") or ""

    company_url = organic.get("url") or (f"https://{domain}" if domain else "")
    if not company_url or _is_directory(company_url):
        return None

    if full_name and company_name:
        title = f"{full_name} — {company_name}"
    elif company_name:
        title = company_name
    else:
        title = organic.get("title", "Unknown")

    snippet = organic.get("description") or ""
    parts = []
    if full_name:    parts.append(f"Contact: {full_name}")
    if job_title:    parts.append(f"Role: {job_title}")
    if company_name: parts.append(f"Company: {company_name}")
    if domain:       parts.append(f"Website: {domain}")
    if linkedin:     parts.append(f"LinkedIn: {linkedin}")
    if snippet:      parts.append(f"\n{snippet}")
    post_body = "\n".join(parts)

    phone, email = _resolve_contacts(
        enrich_phone=lead.get("phone") or lead.get("phoneNumber") or "",
        enrich_email=lead.get("workEmail") or lead.get("email") or "",
        url=company_url,
        snippet=snippet,
        contacts_map=contacts_map,
    )

    return _build_lead(
        title=title,
        url=company_url,
        description=post_body,
        phone=phone,
        email=email,
        location=location,
        service_category=service_category,
        search_query=search_query,
        lead_type="enriched",
        extra_raw={
            **lead,
            "_organic_result": organic,
            "linkedInUrl": linkedin,
            "jobTitle": job_title,
            "companyName": company_name,
            "companyDomain": domain,
        },
    )


def _from_organic_result(
    result: dict,
    service_category: str,
    location: str,
    search_query: str,
    contacts_map: dict,
) -> dict | None:
    url   = result.get("url") or ""
    title = result.get("title") or ""
    desc  = result.get("description") or ""

    if not url or _is_directory(url):
        return None

    phone, email = _resolve_contacts(
        enrich_phone="", enrich_email="",
        url=url, snippet=desc,
        contacts_map=contacts_map,
    )

    return _build_lead(
        title=title,
        url=url,
        description=desc,
        phone=phone,
        email=email,
        location=location,
        service_category=service_category,
        search_query=search_query,
        lead_type="organic",
        extra_raw={**result},
    )


def _from_paid_result(
    result: dict,
    service_category: str,
    location: str,
    search_query: str,
    contacts_map: dict,
) -> dict | None:
    """
    Paid/ad results indicate contractors actively spending on ads —
    high intent signal. Treat same as organic but tag as 'paid'.
    """
    url   = result.get("url") or result.get("displayedUrl") or ""
    title = result.get("title") or ""
    desc  = result.get("description") or ""

    if not url or _is_directory(url):
        return None

    phone, email = _resolve_contacts(
        enrich_phone="", enrich_email="",
        url=url, snippet=desc,
        contacts_map=contacts_map,
    )

    return _build_lead(
        title=f"[Ad] {title}".strip(),
        url=url,
        description=f"[Paid Ad]\n{desc}".strip(),
        phone=phone,
        email=email,
        location=location,
        service_category=service_category,
        search_query=search_query,
        lead_type="paid",
        extra_raw={**result},
    )


def _extract_ai_context(page: dict) -> str:
    """
    Pull text from AI Mode result and AI Overview for inclusion in
    the post body of leads found on the same page — adds rich context.
    Also returns source URLs from AI answers to cross-reference.
    """
    lines = []
    ai_mode = page.get("aiModeResult") or {}
    if ai_mode.get("text"):
        lines.append(f"[AI Mode Answer]\n{ai_mode['text'][:800]}")

    ai_overview = page.get("aiOverview") or {}
    if ai_overview.get("text") or ai_overview.get("snippet"):
        text = ai_overview.get("text") or ai_overview.get("snippet") or ""
        lines.append(f"[AI Overview]\n{text[:400]}")

    paa = page.get("peopleAlsoAsk") or []
    if paa:
        qa = " | ".join(
            f"Q: {item.get('question','')}"
            for item in paa[:3]
        )
        lines.append(f"[People Also Ask] {qa}")

    return "\n\n".join(lines)


def _ai_source_urls(page: dict) -> list[str]:
    """Collect contractor URLs cited in AI Mode / AI Overview answers."""
    urls = []
    for block in [page.get("aiModeResult") or {}, page.get("aiOverview") or {}]:
        for src in block.get("sources") or []:
            u = src.get("url") or ""
            if u and not _is_directory(u):
                urls.append(u)
    return list(dict.fromkeys(urls))


# ── Public entry point ────────────────────────────────────────

def normalize_google_serp_page(
    page: dict,
    service_category: str,
    location: str,
    contacts_map: dict = None,
) -> list[dict]:
    """
    Takes one SERP page record + the contacts_map from website deep-scraping
    and returns a list of unified lead dicts.

    Processing order (deduped by URL):
      1. Enriched business leads (highest quality)
      2. AI Mode / AI Overview source URLs (cited contractor sites)
      3. Organic results not already covered by enrichment
      4. Paid results (contractors running ads)
    """
    if contacts_map is None:
        contacts_map = {}

    search_query = (
        page.get("searchQuery", {}).get("term")
        or page.get("url", "")
    )

    organic_results = page.get("organicResults") or []
    business_leads  = page.get("businessLeads")  or []
    paid_results    = page.get("paidResults")    or []

    leads         = []
    seen_post_ids = set()
    seen_domains  = set()

    def _add(lead_dict):
        if lead_dict is None:
            return
        pid    = lead_dict.get("post_id")
        domain = _domain_from_url(lead_dict.get("url", ""))
        if pid in seen_post_ids:
            return
        seen_post_ids.add(pid)
        seen_domains.add(domain)
        leads.append(lead_dict)

    # Build organic lookup by domain
    organic_by_domain = {}
    for r in organic_results:
        url = r.get("url") or ""
        if url and not _is_directory(url):
            d = _domain_from_url(url)
            if d:
                organic_by_domain[d] = r

    # 1. Enriched leads
    enriched_domains = set()
    for lead in business_leads:
        domain = lead.get("companyDomain") or ""
        organic = organic_by_domain.get(domain, {})
        result  = _from_enriched_lead(
            lead, organic,
            service_category, location, search_query, contacts_map,
        )
        if result:
            enriched_domains.add(domain)
            _add(result)

    # 2. AI-cited sources (contractor sites mentioned in AI answers)
    ai_context = _extract_ai_context(page)
    for url in _ai_source_urls(page):
        if _is_directory(url):
            continue
        domain = _domain_from_url(url)
        if domain in enriched_domains:
            continue
        organic = organic_by_domain.get(domain, {})
        # Build from the organic result if we have it, else minimal lead
        if organic:
            result = _from_organic_result(
                organic, service_category, location, search_query, contacts_map
            )
        else:
            phone, email = _resolve_contacts(
                enrich_phone="", enrich_email="",
                url=url, snippet=ai_context,
                contacts_map=contacts_map,
            )
            result = _build_lead(
                title=url,
                url=url,
                description=f"[Cited in Google AI Answer]\n{ai_context[:400]}",
                phone=phone,
                email=email,
                location=location,
                service_category=service_category,
                search_query=search_query,
                lead_type="ai_cited",
                extra_raw={"_ai_context": ai_context},
            )
        _add(result)

    # 3. Remaining organic results
    for result in organic_results:
        url    = result.get("url") or ""
        domain = _domain_from_url(url)
        if not url or _is_directory(url):
            continue
        if domain in enriched_domains:
            continue
        _add(_from_organic_result(
            result, service_category, location, search_query, contacts_map
        ))

    # 4. Paid results (advertisers = active businesses)
    for result in paid_results:
        url = result.get("url") or result.get("displayedUrl") or ""
        if not url or _is_directory(url):
            continue
        _add(_from_paid_result(
            result, service_category, location, search_query, contacts_map
        ))

    return leads