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
    service_category = models.CharField(max_length=100, null=True, blank=True)  # cleaning / maintenance / waste_management
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
    stage_detail   = models.TextField(null=True, blank=True)
    activity_log   = models.JSONField(default=list, blank=True)
    # ── Identity ──────────────────────────────────────────────────
    run_id = models.CharField(max_length=100)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default="RUNNING")

    # ── Search context (what the user asked for) ──────────────────
    location_type = models.CharField(max_length=20, null=True, blank=True)    # state/city/zip
    location_value = models.CharField(max_length=100, null=True, blank=True)  # TX / Houston / 77001
    location_display = models.CharField(max_length=255, null=True, blank=True)
    categories = models.JSONField(null=True, blank=True)   # ["cleaning", "maintenance"]
    sources = models.JSONField(null=True, blank=True)      # ["craigslist", "facebook"]

    # ── Results ───────────────────────────────────────────────────
    leads_collected = models.IntegerField(default=0)
    leads_skipped = models.IntegerField(default=0)

    # ── Timestamps ───────────────────────────────────────────────
    created_at = models.DateTimeField(auto_now_add=True)
    finished_at = models.DateTimeField(null=True, blank=True)

    def __str__(self):
        return f"{self.run_id} | {self.status} | {self.location_display}"

    class Meta:
        ordering = ["-created_at"]