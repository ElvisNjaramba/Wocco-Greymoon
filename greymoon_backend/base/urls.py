from django.urls import path
from .views import (
    manual_scrape, cancel_scrape, scrape_status, scrape_history,
    list_services, update_lead_status,
    get_cities, get_categories,
    list_scraped_groups, list_group_leads, delete_scraped_group,
    add_fb_groups, scrape_selected_groups, export_leads
)

urlpatterns = [
    path("scrape/start/",   manual_scrape,   name="scrape_start"),
    path("scrape/cancel/",  cancel_scrape,   name="scrape_cancel"),
    path("scrape/status/",  scrape_status,   name="scrape_status"),
    path("scrape/history/", scrape_history,  name="scrape_history"),

    path("leads/",                         list_services,      name="leads_list"),
    path("leads/<str:post_id>/status/",    update_lead_status, name="lead_status_update"),
    path("leads/export/", export_leads, name="leads_export"),

    path("fb-groups/",         list_scraped_groups,   name="fb_groups_list"),
    path("fb-groups/add/",     add_fb_groups,         name="fb_groups_add"),
    path("fb-groups/scrape/",  scrape_selected_groups, name="fb_groups_scrape"),
    path("fb-groups/leads/",   list_group_leads,      name="fb_group_leads"),
    path("fb-groups/delete/",  delete_scraped_group,  name="fb_group_delete"),

    path("meta/cities/",     get_cities,     name="get_cities"),
    path("meta/categories/", get_categories, name="get_categories"),
]