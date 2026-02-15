from django.conf import settings
from django.db import models

User = settings.AUTH_USER_MODEL

class Profile(models.Model):
    user = models.OneToOneField(User, on_delete=models.CASCADE)
    company = models.CharField(max_length=255, blank=True, null=True)
    is_scraper = models.BooleanField(default=False)

    def __str__(self):
        return self.user.email or self.user.username
    
