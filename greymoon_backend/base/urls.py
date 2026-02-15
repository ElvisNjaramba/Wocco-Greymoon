from django.urls import path
from .views import manual_scrape, list_services, cancel_scrape, scrape_status

urlpatterns = [
    path("scrape-services/", manual_scrape),
    path("cancel-scrape/", cancel_scrape),
    path("services/", list_services),
    path("scrape-status/", scrape_status),
]

