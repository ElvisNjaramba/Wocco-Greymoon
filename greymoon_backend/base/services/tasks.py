import threading
from .pipeline import run_pipeline

def start_pipeline_thread(location_type, location_value, categories, sources,
                          scrape_run_id=None, max_groups=20, fb_custom_keywords=None):
    """Spin up run_pipeline in a background thread."""
    def _run():
        run_pipeline(
            location_type=location_type,
            location_value=location_value,
            categories=categories,
            sources=sources,
            scrape_run_id=scrape_run_id,
            max_groups=max_groups,
            fb_custom_keywords=fb_custom_keywords or [],
        )

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return t