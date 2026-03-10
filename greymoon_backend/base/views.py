# ============================================================
#  views.py
# ============================================================

import uuid
import requests
from datetime import datetime, timezone
from django.conf import settings
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response

from .models import ServiceLead, ScrapeRun
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
    location = data.get("location")
    if not location or not isinstance(location, dict):
        return Response({"error": "location is required. Provide {type, value}."}, status=400)

    location_type = location.get("type", "").lower()
    location_value = location.get("value", "").strip()

    if location_type not in ("state", "city", "zip"):
        return Response({"error": "location.type must be 'state', 'city', or 'zip'."}, status=400)
    if not location_value:
        return Response({"error": "location.value cannot be empty."}, status=400)

    categories = data.get("categories", [])
    if not categories:
        return Response({"error": f"At least one category required. Options: {ALL_CATEGORIES}"}, status=400)

    invalid_cats = [c for c in categories if c not in ALL_CATEGORIES]
    if invalid_cats:
        return Response({"error": f"Invalid categories: {invalid_cats}. Valid: {ALL_CATEGORIES}"}, status=400)

    sources = data.get("sources", ["craigslist"])
    valid_sources = {"craigslist", "facebook"}
    invalid_sources = [s for s in sources if s not in valid_sources]
    if invalid_sources:
        return Response({"error": f"Invalid sources: {invalid_sources}. Valid: {list(valid_sources)}"}, status=400)

    # Facebook group discovery limit (how many groups the search actor should return)
    max_groups = int(data.get("max_groups", 20))
    max_groups = max(1, min(max_groups, 100))  # clamp 1–100

    try:
        location_data = resolve_location(location_type, location_value)
    except LocationResolutionError as e:
        return Response({"error": str(e)}, status=400)

    run = ScrapeRun.objects.create(
        run_id=str(uuid.uuid4()),
        status="RUNNING",
        location_type=location_type,
        location_value=location_value,
        location_display=location_data["display"],
        categories=categories,
        sources=sources,
        current_stage="Starting",
        stage_detail="Pipeline initialising…",
        activity_log=[],
    )

    start_pipeline_thread(
        location_type=location_type,
        location_value=location_value,
        categories=categories,
        sources=sources,
        scrape_run_id=run.pk,
        max_groups=max_groups,
    )

    return Response({
        "message": "Scrape started successfully.",
        "run_id": run.run_id,
        "location": location_data["display"],
        "categories": categories,
        "sources": sources,
        "estimated_time": "2–10 minutes depending on sources and location size",
    }, status=202)


# ── Scrape: Cancel ────────────────────────────────────────────

@api_view(["POST"])
@permission_classes([IsAuthenticated])
def cancel_scrape(request):
    run_id = request.data.get("run_id")
    if not run_id:
        return Response({"error": "run_id required"}, status=400)

    headers = {"Authorization": f"Bearer {settings.APIFY_TOKEN}"}
    try:
        requests.post(
            f"https://api.apify.com/v2/actor-runs/{run_id}/abort",
            headers=headers,
            timeout=10,
        )
    except Exception as e:
        print(f"[Cancel] Apify abort call failed (non-fatal): {e}")

    run = ScrapeRun.objects.filter(run_id=run_id).first()
    if run:
        run.status = "PARTIAL" if run.leads_collected > 0 else "ABORTED"
        run.finished_at = datetime.now(timezone.utc)
        run.current_stage = "Stopped by user"
        run.stage_detail = (
            f"Run cancelled — {run.leads_collected} lead(s) already saved to database."
            if run.leads_collected > 0
            else "Run cancelled before any leads were collected."
        )
        # Append cancel entry to the log
        log = run.activity_log or []
        log.append({
            "ts": datetime.now(timezone.utc).isoformat(),
            "stage": "Stopped by user",
            "detail": run.stage_detail,
            "level": "warning",
        })
        run.activity_log = log
        run.save()

    return Response({
        "message": f"Scrape {run_id} cancelled.",
        "leads_saved_so_far": run.leads_collected if run else 0,
    })


# ── Scrape: Status ─────────────────────────────────────────────
# This is polled every 5 s by the frontend while a run is active.
# It returns everything the live activity panel needs.

@api_view(["GET"])
@permission_classes([IsAuthenticated])
def scrape_status(request):
    run = ScrapeRun.objects.order_by("-created_at").first()
    if not run:
        return Response({"status": "IDLE"})

    return Response({
        "run_id": run.run_id,
        "status": run.status,
        "location": run.location_display,
        "categories": run.categories,
        "sources": run.sources,
        "leads_collected": run.leads_collected,
        "leads_skipped": run.leads_skipped,
        # ── live progress fields ──
        "current_stage": run.current_stage or "",
        "stage_detail": run.stage_detail or "",
        "activity_log": run.activity_log or [],
        # ── timestamps ──
        "started_at": run.created_at,
        "finished_at": run.finished_at,
    })


# ── Scrape: History ───────────────────────────────────────────

@api_view(["GET"])
@permission_classes([IsAuthenticated])
def scrape_history(request):
    runs = ScrapeRun.objects.order_by("-created_at")[:50]
    return Response([
        {
            "run_id": r.run_id,
            "status": r.status,
            "location": r.location_display,
            "categories": r.categories,
            "sources": r.sources,
            "leads_collected": r.leads_collected,
            "leads_skipped": r.leads_skipped,
            "started_at": r.created_at,
            "finished_at": r.finished_at,
        }
        for r in runs
    ])


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

    leads = leads.order_by("-score", "-created_at")

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
    page = min(page, total_pages)
    offset = (page - 1) * page_size
    page_leads = leads[offset:offset + page_size]

    serializer = ServiceLeadSerializer(page_leads, many=True)
    return Response({
        "results": serializer.data,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
        "has_next": page < total_pages,
        "has_prev": page > 1,
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
    cities_data = []
    for state_name, state_info in US_CITY_STRUCTURE.items():
        if state_filter and state_name != state_filter:
            continue
        abbrev = _STATE_NAME_TO_ABBREV.get(state_name, state_name)
        for region in state_info["regions"]:
            cities_data.append({
                "code": region["code"],
                "name": region["name"],
                "state": abbrev,
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