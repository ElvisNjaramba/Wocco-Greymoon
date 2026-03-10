from django.urls import path
from .views import (
    manual_scrape,
    cancel_scrape,
    scrape_status,
    scrape_history,
    list_services,
    update_lead_status,
    get_cities,
    get_categories,
)

urlpatterns = [
    # ── Scrape control ──────────────────────────────────────────
    path("scrape/start/", manual_scrape, name="scrape_start"),
    path("scrape/cancel/", cancel_scrape, name="scrape_cancel"),
    path("scrape/status/", scrape_status, name="scrape_status"),
    path("scrape/history/", scrape_history, name="scrape_history"),

    # ── Leads ───────────────────────────────────────────────────
    path("leads/", list_services, name="leads_list"),
    path("leads/<str:post_id>/status/", update_lead_status, name="lead_status_update"),

    # ── Meta / config ───────────────────────────────────────────
    path("meta/cities/", get_cities, name="get_cities"),
    path("meta/categories/", get_categories, name="get_categories"),

    # ── Legacy compat (keep old paths working) ──────────────────
    path("scrape-services/", manual_scrape),
    path("cancel-scrape/", cancel_scrape),
    path("services/", list_services),
    path("scrape-status/", scrape_status),
    path("scrape-history/", scrape_history),
    path("cities/", get_cities),
]