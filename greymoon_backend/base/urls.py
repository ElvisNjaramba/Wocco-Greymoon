from django.urls import path
from .views import (
    manual_scrape, cancel_scrape, scrape_status, scrape_history,
    list_services, update_lead_status,
    get_cities, get_categories, parse_keywords_preview,
    list_scraped_groups, list_group_leads, delete_scraped_group,
)

urlpatterns = [
    # ── Scrape control ───────────────────────────────────────────
    path("scrape/start/",   manual_scrape,   name="scrape_start"),
    path("scrape/cancel/",  cancel_scrape,   name="scrape_cancel"),
    path("scrape/status/",  scrape_status,   name="scrape_status"),
    path("scrape/history/", scrape_history,  name="scrape_history"),

    # ── Leads ────────────────────────────────────────────────────
    path("leads/",                         list_services,      name="leads_list"),
    path("leads/<str:post_id>/status/",    update_lead_status, name="lead_status_update"),

    # ── FB Groups ────────────────────────────────────────────────
    path("fb-groups/",        list_scraped_groups,  name="fb_groups_list"),
    path("fb-groups/leads/",  list_group_leads,     name="fb_group_leads"),
    path("fb-groups/delete/", delete_scraped_group, name="fb_group_delete"),

    # ── Meta ─────────────────────────────────────────────────────
    path("meta/cities/",           get_cities,            name="get_cities"),
    path("meta/categories/",       get_categories,        name="get_categories"),
    path("meta/parse-keywords/",   parse_keywords_preview, name="parse_keywords_preview"),
]