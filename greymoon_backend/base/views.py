# ============================================================
#  views.py  —  manual-groups-only Facebook integration
# ============================================================

import re
import uuid
import requests
from datetime import datetime, timezone
from django.conf import settings
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response

from .models import ServiceLead, ScrapeRun, ScrapedFbGroup
from .serializers import ServiceLeadSerializer, ScrapeRunSerializer
from .services.tasks import start_pipeline_thread
from .services.category_map import ALL_CATEGORIES, SERVICE_CATEGORY_MAP
from .services.location_resolver import resolve_location, LocationResolutionError
from .services.city_structure import US_CITY_STRUCTURE

PAGE_SIZE = 50

_STATE_NAME_TO_ABBREV = {
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR",
    "California": "CA", "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE",
    "District of Columbia": "DC", "Florida": "FL", "Georgia": "GA", "Hawaii": "HI",
    "Idaho": "ID", "Illinois": "IL", "Indiana": "IN", "Iowa": "IA",
    "Kansas": "KS", "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME",
    "Maryland": "MD", "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN",
    "Mississippi": "MS", "Missouri": "MO", "Montana": "MT", "Nebraska": "NE",
    "Nevada": "NV", "New Hampshire": "NH", "New Jersey": "NJ", "New Mexico": "NM",
    "New York": "NY", "North Carolina": "NC", "North Dakota": "ND", "Ohio": "OH",
    "Oklahoma": "OK", "Oregon": "OR", "Pennsylvania": "PA", "Rhode Island": "RI",
    "South Carolina": "SC", "South Dakota": "SD", "Tennessee": "TN", "Texas": "TX",
    "Utah": "UT", "Vermont": "VT", "Virginia": "VA", "Washington": "WA",
    "West Virginia": "WV", "Wisconsin": "WI", "Wyoming": "WY",
}


# ── Scrape: Start ─────────────────────────────────────────────

@api_view(["POST"])
@permission_classes([IsAuthenticated])
def manual_scrape(request):
    data = request.data

    sources = data.get("sources", ["craigslist"])
    valid_sources   = {"craigslist", "facebook", "indeed", "google"}
    invalid_sources = [s for s in sources if s not in valid_sources]
    if invalid_sources:
        return Response(
            {"error": f"Invalid sources: {invalid_sources}. Valid: {list(valid_sources)}"},
            status=400,
        )

    # ── Location (optional for FB-only runs) ──────────────────
    location = data.get("location") or {}
    location_type  = location.get("type", "").lower() if location else ""
    location_value = (location.get("value") or "").strip() if location else ""

    if location_type and location_type not in ("state", "city", "zip"):
        return Response({"error": "location.type must be 'state', 'city', or 'zip'."}, status=400)

    # ── Facebook group URLs ───────────────────────────────────
    fb_group_urls_raw = data.get("fb_group_urls") or []
    if isinstance(fb_group_urls_raw, str):
        fb_group_urls_raw = [u.strip() for u in fb_group_urls_raw.split(",") if u.strip()]
    fb_group_urls = [u for u in fb_group_urls_raw if u.startswith("http")]

    fb_only = set(sources) == {"facebook"}

    # Validation
    if not location_value and not fb_group_urls:
        return Response({"error": "Provide a location or at least one Facebook group URL."}, status=400)
    if not location_value and "craigslist" in sources:
        return Response({"error": "location.value is required when scraping Craigslist."}, status=400)

    categories = data.get("categories", [])
    # Categories are only required when scraping non-Facebook sources
    non_fb_sources = [s for s in sources if s != "facebook"]
    if non_fb_sources and not categories:
        return Response(
            {"error": f"At least one category required for {non_fb_sources}. Options: {ALL_CATEGORIES}"},
            status=400,
        )

    if categories:
        invalid_cats = [c for c in categories if c not in ALL_CATEGORIES]
        if invalid_cats:
            return Response(
                {"error": f"Invalid categories: {invalid_cats}. Valid: {ALL_CATEGORIES}"},
                status=400,
            )

    # Posts per group limit
    max_posts_per_group = int(data.get("max_posts_per_group", 50))
    max_posts_per_group = max(5, min(max_posts_per_group, 500))

    # Google settings
    google_max_pages   = int(data.get("google_max_pages", 3))
    google_max_pages   = max(1, min(google_max_pages, 10))
    google_deep_scrape = bool(data.get("google_deep_scrape", True))

    # Resolve location
    if location_value:
        try:
            location_data = resolve_location(location_type, location_value)
        except LocationResolutionError as e:
            return Response({"error": str(e)}, status=400)
        location_display = location_data["display"]
    else:
        location_data    = None
        location_display = "Facebook Groups"

    run = ScrapeRun.objects.create(
        run_id=str(uuid.uuid4()),
        status="RUNNING",
        location_type=location_type or "custom",
        location_value=location_value or "",
        location_display=location_display,
        max_posts_per_group=max_posts_per_group,
        categories=categories,
        sources=sources,
        current_stage="Starting",
        stage_detail="Pipeline initialising…",
        activity_log=[],
        google_max_pages=google_max_pages,
        google_deep_scrape=google_deep_scrape,
    )

    start_pipeline_thread(
        location_type=location_type or "custom",
        location_value=location_value or "",
        categories=categories,
        sources=sources,
        scrape_run_id=run.pk,
        max_posts_per_group=max_posts_per_group,
        fb_group_urls=fb_group_urls,
        google_max_pages=google_max_pages,
        google_deep_scrape=google_deep_scrape,
    )

    return Response({
        "message": "Scrape started successfully.",
        "run_id":  run.run_id,
        "location": location_display,
        "categories": categories,
        "sources": sources,
        "fb_groups": len(fb_group_urls),
        "estimated_time": "2–10 minutes depending on sources and group size",
    }, status=202)


# ── Scrape: Cancel helpers ────────────────────────────────────

def _sweep_and_abort(apify_headers: dict, already_aborted: set) -> list[str]:
    newly = []
    for status_filter in ("RUNNING", "READY"):
        try:
            resp = requests.get(
                "https://api.apify.com/v2/actor-runs",
                headers=apify_headers,
                params={"status": status_filter, "limit": 100},
                timeout=15,
            )
            if resp.status_code != 200:
                continue
            for actor_run in resp.json().get("data", {}).get("items", []):
                aid = actor_run.get("id")
                if aid and aid not in already_aborted:
                    try:
                        ar = requests.post(
                            f"https://api.apify.com/v2/actor-runs/{aid}/abort",
                            headers=apify_headers,
                            timeout=10,
                        )
                        if ar.status_code in (200, 204):
                            newly.append(aid)
                            already_aborted.add(aid)
                    except Exception as e:
                        print(f"[Cancel] Could not abort actor {aid}: {e}")
        except Exception as e:
            print(f"[Cancel] Sweep ({status_filter}) error: {e}")
    return newly


# ── Scrape: Cancel ────────────────────────────────────────────

@api_view(["POST"])
@permission_classes([IsAuthenticated])
def cancel_scrape(request):
    run_id = request.data.get("run_id")
    if not run_id:
        return Response({"error": "run_id required"}, status=400)

    run = ScrapeRun.objects.filter(run_id=run_id).first()
    if not run:
        return Response({"error": "Run not found"}, status=404)

    ScrapeRun.objects.filter(run_id=run_id).update(cancel_requested=True)

    apify_headers  = {"Authorization": f"Bearer {settings.APIFY_TOKEN}"}
    aborted_set: set = set()

    for apify_id in (run.apify_run_ids or []):
        try:
            resp = requests.post(
                f"https://api.apify.com/v2/actor-runs/{apify_id}/abort",
                headers=apify_headers,
                timeout=10,
            )
            if resp.status_code in (200, 204):
                aborted_set.add(apify_id)
        except Exception as e:
            print(f"[Cancel] Abort failed for registered actor {apify_id}: {e}")

    _sweep_and_abort(apify_headers, aborted_set)

    import time as _time
    _time.sleep(3)
    _sweep_and_abort(apify_headers, aborted_set)

    run.refresh_from_db()
    detail = (
        f"Run cancelled — {run.leads_collected} lead(s) already saved to database."
        if run.leads_collected > 0
        else "Run cancelled before any leads were collected."
    )
    activity_log = run.activity_log or []
    activity_log.append({
        "ts":     datetime.now(timezone.utc).isoformat(),
        "stage":  "Stopped by user",
        "detail": detail,
        "level":  "warning",
    })
    run.status        = "PARTIAL" if run.leads_collected > 0 else "ABORTED"
    run.finished_at   = datetime.now(timezone.utc)
    run.current_stage = "Stopped by user"
    run.stage_detail  = detail
    run.activity_log  = activity_log
    run.save()

    return Response({
        "message":            f"Scrape {run_id} cancelled.",
        "apify_runs_aborted": len(aborted_set),
        "leads_saved_so_far": run.leads_collected,
    })


# ── Scrape: Status ─────────────────────────────────────────────

@api_view(["GET"])
@permission_classes([IsAuthenticated])
def scrape_status(request):
    run = ScrapeRun.objects.order_by("-created_at").first()
    if not run:
        return Response({"status": "IDLE"})

    return Response({
        "run_id":          run.run_id,
        "status":          run.status,
        "location":        run.location_display,
        "categories":      run.categories,
        "sources":         run.sources,
        "leads_collected": run.leads_collected,
        "leads_skipped":   run.leads_skipped,
        "current_stage":   run.current_stage or "",
        "stage_detail":    run.stage_detail or "",
        "activity_log":    run.activity_log or [],
        "started_at":      run.created_at,
        "finished_at":     run.finished_at,
    })


# ── Scrape: History ───────────────────────────────────────────

@api_view(["GET"])
@permission_classes([IsAuthenticated])
def scrape_history(request):
    runs = ScrapeRun.objects.order_by("-created_at")[:50]
    return Response([
        {
            "run_id":          r.run_id,
            "status":          r.status,
            "location":        r.location_display,
            "categories":      r.categories,
            "sources":         r.sources,
            "leads_collected": r.leads_collected,
            "leads_skipped":   r.leads_skipped,
            "started_at":      r.created_at,
            "finished_at":     r.finished_at,
        }
        for r in runs
    ])


# ── FB Groups: Add groups (no scraping yet) ───────────────────

@api_view(["POST"])
@permission_classes([IsAuthenticated])
def add_fb_groups(request):
    """
    Register one or more Facebook group URLs without scraping them.
    The user can then trigger scraping from the Groups tab.
    """
    urls_raw = request.data.get("group_urls") or []
    if isinstance(urls_raw, str):
        urls_raw = [u.strip() for u in re.split(r"[\n,]+", urls_raw) if u.strip()]

    valid_urls = [u for u in urls_raw if u.startswith("http")]
    if not valid_urls:
        return Response({"error": "No valid group URLs provided."}, status=400)

    added = []
    already_exists = []
    for url in valid_urls:
        url = url.rstrip("/") + "/"
        obj, created = ScrapedFbGroup.objects.get_or_create(
            group_url=url,
            defaults={"group_name": "", "post_count": 0},
        )
        if created:
            added.append(url)
        else:
            already_exists.append(url)

    return Response({
        "added":          len(added),
        "already_exists": len(already_exists),
        "total":          ScrapedFbGroup.objects.count(),
    })


# ── FB Groups: List ───────────────────────────────────────────

@api_view(["GET"])
@permission_classes([IsAuthenticated])
def list_scraped_groups(request):
    def _display_name(group_name: str, group_url: str) -> str:
        if group_name and group_name.strip():
            return group_name.strip()
        m = re.search(r"/groups/([^/?#]+)", group_url or "")
        if m:
            slug = m.group(1)
            slug = re.sub(r"([a-z])([A-Z])", r"\1 \2", slug)
            slug = re.sub(r"[-_]", " ", slug)
            slug = re.sub(r"\s+", " ", slug).strip()
            return slug.title() if slug else group_url
        return group_url or "Unknown group"

    groups = list(ScrapedFbGroup.objects.all().values(
        "id", "group_url", "group_name", "post_count", "last_scraped", "first_scraped"
    ))
    for g in groups:
        g["group_name"] = _display_name(g["group_name"], g["group_url"])

    return Response({"groups": groups, "total": len(groups)})


# ── FB Groups: Scrape selected groups ────────────────────────

@api_view(["POST"])
@permission_classes([IsAuthenticated])
def scrape_selected_groups(request):
    """
    Trigger a Facebook-only scrape run for a specific set of group URLs.
    No category or location required — Facebook scraping is fully independent.
    max_posts_per_group can be specified per-request.
    """
    group_urls = request.data.get("group_urls") or []
    if isinstance(group_urls, str):
        group_urls = [u.strip() for u in re.split(r"[\n,]+", group_urls) if u.strip()]
    group_urls = [u for u in group_urls if u.startswith("http")]

    if not group_urls:
        return Response({"error": "No valid group URLs provided."}, status=400)

    max_posts_per_group = int(request.data.get("max_posts_per_group", 50))
    max_posts_per_group = max(5, min(max_posts_per_group, 500))

    run = ScrapeRun.objects.create(
        run_id=str(uuid.uuid4()),
        status="RUNNING",
        location_type="custom",
        location_value="",
        location_display=f"{len(group_urls)} Facebook group(s)",
        max_posts_per_group=max_posts_per_group,
        categories=[],
        sources=["facebook"],
        current_stage="Starting",
        stage_detail="Pipeline initialising…",
        activity_log=[],
        google_max_pages=3,
        google_deep_scrape=False,
    )

    start_pipeline_thread(
        location_type="custom",
        location_value="",
        categories=[],
        sources=["facebook"],
        scrape_run_id=run.pk,
        max_posts_per_group=max_posts_per_group,
        fb_group_urls=group_urls,
    )

    return Response({
        "message":   "Facebook scrape started.",
        "run_id":    run.run_id,
        "groups":    len(group_urls),
        "max_posts": max_posts_per_group,
    }, status=202)


# ── FB Groups: Leads for a group ─────────────────────────────

@api_view(["GET"])
@permission_classes([IsAuthenticated])
def list_group_leads(request):
    group_url = request.query_params.get("group_url", "")
    if not group_url:
        return Response({"error": "group_url required"}, status=400)
    leads       = ServiceLead.objects.filter(fb_group_url=group_url).order_by("-created_at")
    page_size   = min(200, int(request.query_params.get("page_size", 50)))
    page        = max(1, int(request.query_params.get("page", 1)))
    total       = leads.count()
    total_pages = max(1, (total + page_size - 1) // page_size)
    page        = min(page, total_pages)
    offset      = (page - 1) * page_size
    serializer  = ServiceLeadSerializer(leads[offset:offset + page_size], many=True)
    return Response({
        "results":     serializer.data,
        "total":       total,
        "page":        page,
        "total_pages": total_pages,
        "has_next":    page < total_pages,
    })


# ── FB Groups: Delete a group ─────────────────────────────────

@api_view(["DELETE"])
@permission_classes([IsAuthenticated])
def delete_scraped_group(request):
    group_url = request.data.get("group_url", "")
    if not group_url:
        return Response({"error": "group_url required"}, status=400)
    deleted, _ = ScrapedFbGroup.objects.filter(group_url=group_url).delete()
    return Response({"deleted": deleted})


# ── Leads: List ───────────────────────────────────────────────

@api_view(["GET"])
@permission_classes([IsAuthenticated])
def list_services(request):
    leads = ServiceLead.objects.all()

    source = request.query_params.get("source")
    if source:
        leads = leads.filter(source=source.upper())

    service_category = request.query_params.get("service_category")
    if service_category:
        leads = leads.filter(service_category=service_category.lower())

    status = request.query_params.get("status")
    if status:
        leads = leads.filter(status=status.upper())

    min_score = request.query_params.get("min_score")
    if min_score:
        try:
            leads = leads.filter(score__gte=int(min_score))
        except ValueError:
            pass

    search = request.query_params.get("search")
    if search:
        leads = leads.filter(location__icontains=search) | \
                leads.filter(title__icontains=search)

    if request.query_params.get("has_phone") in ("true", "1"):
        leads = leads.exclude(phone__isnull=True).exclude(phone="")
    if request.query_params.get("has_email") in ("true", "1"):
        leads = leads.exclude(email__isnull=True).exclude(email="")

    fb_group = request.query_params.get("fb_group")
    if fb_group:
        leads = leads.filter(fb_group_name__icontains=fb_group)

    date_after = request.query_params.get("date_after")
    if date_after:
        try:
            from django.utils.dateparse import parse_datetime
            dt = parse_datetime(date_after)
            if dt:
                leads = leads.filter(created_at__gte=dt)
        except Exception:
            pass

    ordering_param = request.query_params.get("ordering", "-created_at")
    ALLOWED_ORDERING_FIELDS = {
        "datetime", "-datetime",
        "score", "-score",
        "created_at", "-created_at",
    }
    if ordering_param not in ALLOWED_ORDERING_FIELDS:
        ordering_param = "-created_at"

    if ordering_param == "-datetime":
        leads = leads.order_by("-created_at")
    elif ordering_param == "datetime":
        leads = leads.order_by("created_at")
    else:
        leads = leads.order_by(ordering_param, "-created_at")

    total = leads.count()

    try:
        page = max(1, int(request.query_params.get("page", 1)))
    except ValueError:
        page = 1

    try:
        page_size = min(200, max(1, int(request.query_params.get("page_size", PAGE_SIZE))))
    except ValueError:
        page_size = PAGE_SIZE

    total_pages = max(1, (total + page_size - 1) // page_size)
    page        = min(page, total_pages)
    offset      = (page - 1) * page_size
    page_leads  = leads[offset:offset + page_size]

    serializer = ServiceLeadSerializer(page_leads, many=True)
    return Response({
        "results":     serializer.data,
        "total":       total,
        "page":        page,
        "page_size":   page_size,
        "total_pages": total_pages,
        "has_next":    page < total_pages,
        "has_prev":    page > 1,
    })


# ── Leads: Update status ──────────────────────────────────────

@api_view(["PATCH"])
@permission_classes([IsAuthenticated])
def update_lead_status(request, post_id):
    try:
        lead = ServiceLead.objects.get(post_id=post_id)
    except ServiceLead.DoesNotExist:
        return Response({"error": "Lead not found"}, status=404)

    new_status = request.data.get("status")
    if new_status not in dict(ServiceLead.STATUS_CHOICES):
        return Response({"error": "Invalid status"}, status=400)

    lead.status = new_status
    lead.save()
    return Response({"message": "Status updated", "status": new_status})


# ── Meta: Cities ──────────────────────────────────────────────

@api_view(["GET"])
@permission_classes([IsAuthenticated])
def get_cities(request):
    state_filter = request.query_params.get("state", "").title()
    cities_data  = []
    for state_name, state_info in US_CITY_STRUCTURE.items():
        if state_filter and state_name != state_filter:
            continue
        abbrev = _STATE_NAME_TO_ABBREV.get(state_name, state_name)
        for region in state_info["regions"]:
            cities_data.append({
                "code":    region["code"],
                "name":    region["name"],
                "state":   abbrev,
                "display": f"{region['name']}, {abbrev}",
            })
    return Response({"cities": cities_data, "total": len(cities_data)})


# ── Meta: Categories ──────────────────────────────────────────

@api_view(["GET"])
def get_categories(request):
    return Response({
        "categories": [
            {"key": key, "label": val["label"]}
            for key, val in SERVICE_CATEGORY_MAP.items()
        ]
    })