from django.db import models


class ServiceLead(models.Model):

    STATUS_CHOICES = [
        ("NEW", "New"),
        ("CONTACTED", "Contacted"),
        ("QUALIFIED", "Qualified"),
        ("WON", "Won"),
        ("LOST", "Lost"),
    ]

    SOURCE_CHOICES = [
        ("CRAIGSLIST", "Craigslist"),
        ("FACEBOOK", "Facebook"),
    ]

    # ── Identity ──────────────────────────────────────────────────
    post_id = models.CharField(max_length=100, unique=True)
    url = models.URLField(max_length=1000, blank=True)
    content_hash = models.CharField(max_length=64, null=True, blank=True, db_index=True)

    # ── Content ───────────────────────────────────────────────────
    title = models.CharField(max_length=500)
    post = models.TextField(null=True, blank=True)
    datetime = models.DateTimeField(null=True, blank=True)

    # ── Classification ────────────────────────────────────────────
    source = models.CharField(max_length=20, choices=SOURCE_CHOICES, default="CRAIGSLIST")
    category = models.CharField(max_length=255, null=True, blank=True)
    service_category = models.CharField(max_length=100, null=True, blank=True)
    label = models.CharField(max_length=100, null=True, blank=True)

    # ── Location ──────────────────────────────────────────────────
    location = models.CharField(max_length=255, null=True, blank=True)
    state = models.CharField(max_length=100, null=True, blank=True)
    latitude = models.CharField(max_length=50, null=True, blank=True)
    longitude = models.CharField(max_length=50, null=True, blank=True)
    map_accuracy = models.CharField(max_length=50, null=True, blank=True)

    # ── Contact ───────────────────────────────────────────────────
    phone = models.CharField(max_length=50, null=True, blank=True)
    email = models.CharField(max_length=255, null=True, blank=True)
    zip_code = models.CharField(max_length=20, null=True, blank=True)

    # ── Facebook group attribution ────────────────────────────────
    # Stores which group the post was scraped from so leads can be
    # filtered/grouped by source group in the UI.
    fb_group_name = models.CharField(max_length=500, null=True, blank=True)
    fb_group_url = models.URLField(max_length=1000, null=True, blank=True)

    # ── CRM ───────────────────────────────────────────────────────
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default="NEW")

    # ── Scoring ───────────────────────────────────────────────────
    score = models.IntegerField(default=0)
    score_reason = models.JSONField(null=True, blank=True)

    # ── Raw data ──────────────────────────────────────────────────
    raw_json = models.JSONField(null=True, blank=True)

    # ── Timestamps ───────────────────────────────────────────────
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"[{self.source}] {self.title[:60]}"

    class Meta:
        indexes = [
            models.Index(fields=["post_id"]),
            models.Index(fields=["content_hash"]),
            models.Index(fields=["created_at"]),
            models.Index(fields=["source"]),
            models.Index(fields=["service_category"]),
            models.Index(fields=["score"]),
            models.Index(fields=["fb_group_name"]),
        ]


class ScrapeRun(models.Model):

    STATUS_CHOICES = [
        ("RUNNING", "Running"),
        ("SUCCEEDED", "Succeeded"),
        ("PARTIAL", "Partial"),
        ("FAILED", "Failed"),
        ("ABORTED", "Aborted"),
    ]

    # ── Live progress ─────────────────────────────────────────────
    current_stage = models.CharField(max_length=200, null=True, blank=True)
    stage_detail = models.TextField(null=True, blank=True)
    activity_log = models.JSONField(default=list, blank=True)

    # ── Identity ──────────────────────────────────────────────────
    run_id = models.CharField(max_length=100)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default="RUNNING")

    # ── Search context ────────────────────────────────────────────
    location_type = models.CharField(max_length=20, null=True, blank=True)
    location_value = models.CharField(max_length=100, null=True, blank=True)
    location_display = models.CharField(max_length=255, null=True, blank=True)
    categories = models.JSONField(null=True, blank=True)
    sources = models.JSONField(null=True, blank=True)

    # ── Results ───────────────────────────────────────────────────
    leads_collected = models.IntegerField(default=0)
    leads_skipped = models.IntegerField(default=0)
    source_stats = models.JSONField(default=dict, blank=True)


    # ── FB scrape settings ───────────────────────────────────────
    max_posts_per_group = models.IntegerField(default=50)

    google_max_pages   = models.IntegerField(default=3)
    google_deep_scrape = models.BooleanField(default=True)

    # ── Cancellation ──────────────────────────────────────────────
    # apify_run_ids: list of active Apify actor run IDs for this scrape run.
    # The pipeline registers each Apify run here immediately after launch,
    # so cancel_scrape can call /abort on the right IDs.
    apify_run_ids = models.JSONField(default=list, blank=True)

    # cancel_requested: set to True by cancel_scrape view.
    # The pipeline thread checks this between batches and stops if True.
    cancel_requested = models.BooleanField(default=False)

    # ── Timestamps ───────────────────────────────────────────────
    created_at = models.DateTimeField(auto_now_add=True)
    finished_at = models.DateTimeField(null=True, blank=True)

    def __str__(self):
        return f"{self.run_id} | {self.status} | {self.location_display}"

    class Meta:
        ordering = ["-created_at"]

class ScrapedFbGroup(models.Model):
    """
    Tracks every Facebook group that has been scraped.
    The pipeline checks this before scraping a group — if it already
    exists it is skipped entirely, preventing duplicate posts.
    """
    group_url   = models.URLField(max_length=1000, unique=True, db_index=True)
    group_name  = models.CharField(max_length=500, blank=True)
    # Number of posts scraped from this group across all runs
    post_count  = models.IntegerField(default=0)
    last_scraped = models.DateTimeField(auto_now=True)
    first_scraped = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.group_name or self.group_url} ({self.post_count} posts)"

    class Meta:
        ordering = ["-last_scraped"]


class ScrapedFbPost(models.Model):
    """
    Tracks every Facebook post URL that has been scraped, so we never
    store the same post twice even across different scrape runs or groups.
    """
    post_url   = models.URLField(max_length=1000, unique=True, db_index=True)
    group_url  = models.URLField(max_length=1000, blank=True, db_index=True)
    scraped_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-scraped_at"]