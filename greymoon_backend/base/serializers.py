from rest_framework import serializers
from .models import ServiceLead, ScrapeRun


class ServiceLeadSerializer(serializers.ModelSerializer):
    class Meta:
        model = ServiceLead
        fields = "__all__"


class ScrapeRunSerializer(serializers.ModelSerializer):
    class Meta:
        model = ScrapeRun
        fields = "__all__"