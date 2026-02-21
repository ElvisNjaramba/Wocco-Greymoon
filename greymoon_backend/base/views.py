from rest_framework.decorators import api_view
from rest_framework.response import Response
from .services.apify_service import US_CITIES, run_actor
from .models import ServiceLead
from .serializers import ServiceLeadSerializer

from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import IsAuthenticated

from .services.tasks import start_scrape_thread

from .models import ScrapeRun
import requests
from django.conf import settings

# @api_view(["POST"])
# @permission_classes([IsAuthenticated])
# def manual_scrape(request):
#     run_id, dataset_id = run_actor()

#     scrape_run = ScrapeRun.objects.create(
#         run_id=run_id,
#         status="RUNNING"
#     )

#     start_scrape_thread(run_id, dataset_id)

#     return Response({
#         "message": "Scraping started",
#         "run_id": run_id
#     })

@api_view(["POST"])
@permission_classes([IsAuthenticated])
def manual_scrape(request):
    selected_cities = request.data.get("cities", [])

    if not selected_cities:
        return Response(
            {"error": "At least one city must be selected"},
            status=400
        )

    # Validate cities (security layer)
    invalid = [c for c in selected_cities if c not in US_CITIES]
    if invalid:
        return Response(
            {"error": f"Invalid cities: {invalid}"},
            status=400
        )

    run_id, dataset_id = run_actor(selected_cities)

    scrape_run = ScrapeRun.objects.create(
        run_id=run_id,
        status="RUNNING"
    )

    start_scrape_thread(run_id, dataset_id)

    return Response({
        "message": "Scraping started",
        "run_id": run_id
    })


@api_view(["POST"])
@permission_classes([IsAuthenticated])
def cancel_scrape(request):
    run_id = request.data.get("run_id")

    if not run_id:
        return Response({"error": "run_id required"}, status=400)

    url = f"https://api.apify.com/v2/actor-runs/{run_id}/abort"

    headers = {
        "Authorization": f"Bearer {settings.APIFY_TOKEN}"
    }

    requests.post(url, headers=headers)

    ScrapeRun.objects.filter(run_id=run_id).update(status="ABORTED")

    return Response({"message": "Scrape aborted"})

@api_view(["GET"])
@permission_classes([IsAuthenticated])
def scrape_status(request):
    run = ScrapeRun.objects.order_by("-created_at").first()

    if not run:
        return Response({"status": "IDLE"})

    return Response({
        "run_id": run.run_id,
        "status": run.status
    })


@api_view(["GET"])
def list_services(request):
    leads = ServiceLead.objects.all().order_by("-created_at")[:500]
    serializer = ServiceLeadSerializer(leads, many=True)
    return Response(serializer.data)

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

    return Response({"message": "Status updated"})

@api_view(["GET"])
@permission_classes([IsAuthenticated])
def scrape_history(request):
    runs = ScrapeRun.objects.order_by("-created_at")
    data = []

    for run in runs:
        data.append({
            "run_id": run.run_id,
            "status": run.status,
            "leads_collected": run.leads_collected,
            "started_at": run.created_at,
            "finished_at": run.finished_at,
        })

    return Response(data)

# @api_view(["GET"])
# @permission_classes([IsAuthenticated])
# def get_cities(request):
#     return Response({"cities": US_CITIES})

from .services.city_structure import US_CITY_STRUCTURE
@api_view(["GET"])
@permission_classes([IsAuthenticated])
def get_cities(request):
    cities_data = []
    for state_code, state_info in US_CITY_STRUCTURE.items():
        for region in state_info["regions"]:
            cities_data.append({
                "code": region["code"],
                "name": region["name"],
                "state": state_code,
                "display": f"{region['name']}, {state_code}"
            })
    return Response({"cities": cities_data})