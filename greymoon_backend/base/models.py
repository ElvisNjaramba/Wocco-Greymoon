from django.db import models

class ServiceLead(models.Model):
    post_id = models.CharField(max_length=50, unique=True)
    url = models.URLField()
    title = models.CharField(max_length=500)
    datetime = models.DateTimeField(null=True, blank=True)

    location = models.CharField(max_length=255, null=True, blank=True)
    category = models.CharField(max_length=255, null=True, blank=True)
    label = models.CharField(max_length=100, null=True, blank=True)

    longitude = models.CharField(max_length=50, null=True, blank=True)
    latitude = models.CharField(max_length=50, null=True, blank=True)
    map_accuracy = models.CharField(max_length=50, null=True, blank=True)

    post = models.TextField(null=True, blank=True)

    phone = models.CharField(max_length=50, null=True, blank=True)
    email = models.CharField(max_length=255, null=True, blank=True)
    zip_code = models.CharField(max_length=20, null=True, blank=True)

    raw_json = models.JSONField(null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return self.title


class ScrapeRun(models.Model):
    run_id = models.CharField(max_length=100)
    status = models.CharField(max_length=50, default="RUNNING")
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.run_id} - {self.status}"
