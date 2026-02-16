from django.urls import path
from .views import manual_scrape, list_services, cancel_scrape, scrape_status, update_lead_status, scrape_history, get_cities

urlpatterns = [
    path("scrape-services/", manual_scrape),
    path("cancel-scrape/", cancel_scrape),
    path("services/", list_services),
    path("scrape-status/", scrape_status),
    path("leads/<str:post_id>/status/", update_lead_status),
    path("scrape-history/", scrape_history),
    path("cities/", get_cities, name="get_cities"),
]

