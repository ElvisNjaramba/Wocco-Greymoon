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

    post_id = models.CharField(max_length=100, unique=True)
    url = models.URLField(max_length=1000, blank=True)
    content_hash = models.CharField(max_length=64, null=True, blank=True, db_index=True)
    title_ngram_hash = models.CharField(max_length=64, null=True, blank=True, db_index=True)

    title = models.CharField(max_length=500)
    post = models.TextField(null=True, blank=True)
    datetime = models.DateTimeField(null=True, blank=True)

    source = models.CharField(max_length=20, choices=SOURCE_CHOICES, default="CRAIGSLIST")
    category = models.CharField(max_length=255, null=True, blank=True)
    service_category = models.CharField(max_length=100, null=True, blank=True)
    label = models.CharField(max_length=100, null=True, blank=True)

    location = models.CharField(max_length=255, null=True, blank=True)
    state = models.CharField(max_length=100, null=True, blank=True)
    latitude = models.CharField(max_length=50, null=True, blank=True)
    longitude = models.CharField(max_length=50, null=True, blank=True)
    map_accuracy = models.CharField(max_length=50, null=True, blank=True)

    phone = models.CharField(max_length=50, null=True, blank=True)
    email = models.CharField(max_length=255, null=True, blank=True)
    zip_code = models.CharField(max_length=20, null=True, blank=True)

    fb_group_name = models.CharField(max_length=500, null=True, blank=True)
    fb_group_url = models.URLField(max_length=1000, null=True, blank=True)

    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default="NEW")

    score = models.IntegerField(default=0)
    score_reason = models.JSONField(null=True, blank=True)

    raw_json = models.JSONField(null=True, blank=True)

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
            models.Index(fields=["title"]),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["content_hash"],
                condition=models.Q(content_hash__isnull=False) & ~models.Q(content_hash=""),
                name="unique_content_hash_nonempty",
            )
        ]


class ScrapeRun(models.Model):

    STATUS_CHOICES = [
        ("RUNNING", "Running"),
        ("SUCCEEDED", "Succeeded"),
        ("PARTIAL", "Partial"),
        ("FAILED", "Failed"),
        ("ABORTED", "Aborted"),
    ]

    current_stage = models.CharField(max_length=200, null=True, blank=True)
    stage_detail = models.TextField(null=True, blank=True)
    activity_log = models.JSONField(default=list, blank=True)

    run_id = models.CharField(max_length=100)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default="RUNNING")

    location_type = models.CharField(max_length=20, null=True, blank=True)
    location_value = models.CharField(max_length=100, null=True, blank=True)
    location_display = models.CharField(max_length=255, null=True, blank=True)
    categories = models.JSONField(null=True, blank=True)
    sources = models.JSONField(null=True, blank=True)

    leads_collected = models.IntegerField(default=0)
    leads_skipped = models.IntegerField(default=0)
    source_stats = models.JSONField(default=dict, blank=True)

    max_posts_per_group = models.IntegerField(default=50)
    max_leads    = models.IntegerField(default=0)   # 0 = no limit
    limit_stop   = models.BooleanField(default=False)

    google_max_pages   = models.IntegerField(default=3)
    google_deep_scrape = models.BooleanField(default=True)

    apify_run_ids = models.JSONField(default=list, blank=True)

    cancel_requested = models.BooleanField(default=False)

    created_at = models.DateTimeField(auto_now_add=True)
    finished_at = models.DateTimeField(null=True, blank=True)

    def __str__(self):
        return f"{self.run_id} | {self.status} | {self.location_display}"

    class Meta:
        ordering = ["-created_at"]

class ScrapedFbGroup(models.Model):

    group_url   = models.URLField(max_length=1000, unique=True, db_index=True)
    group_name  = models.CharField(max_length=500, blank=True)
    post_count  = models.IntegerField(default=0)
    last_scraped = models.DateTimeField(auto_now=True)
    first_scraped = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.group_name or self.group_url} ({self.post_count} posts)"

    class Meta:
        ordering = ["-last_scraped"]


class ScrapedFbPost(models.Model):

    post_url   = models.URLField(max_length=1000, unique=True, db_index=True)
    group_url  = models.URLField(max_length=1000, blank=True, db_index=True)
    scraped_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-scraped_at"]