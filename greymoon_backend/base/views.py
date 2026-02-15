from rest_framework.decorators import api_view
from rest_framework.response import Response
from .services.apify_service import run_actor
from .models import ServiceLead
from .serializers import ServiceLeadSerializer

from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import IsAuthenticated

from .services.tasks import start_scrape_thread

from .models import ScrapeRun
import requests
from django.conf import settings

@api_view(["POST"])
@permission_classes([IsAuthenticated])
def manual_scrape(request):
    run_id, dataset_id = run_actor()

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
