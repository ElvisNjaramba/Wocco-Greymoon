import threading
from .pipeline import run_pipeline

def start_pipeline_thread(
    location_type,
    location_value,
    categories,
    sources,
    scrape_run_id=None,
    max_posts_per_group=50,
    fb_group_urls=None,
    google_max_pages=3,
    google_deep_scrape=True,
    max_leads=0,
):
    def _run():
        run_pipeline(
            location_type=location_type,
            location_value=location_value,
            categories=categories,
            sources=sources,
            scrape_run_id=scrape_run_id,
            max_posts_per_group=max_posts_per_group,
            fb_group_urls=fb_group_urls or [],
            google_max_pages=google_max_pages,
            google_deep_scrape=google_deep_scrape,
            max_leads=max_leads,
        )

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return t