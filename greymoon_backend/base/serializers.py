from rest_framework import serializers
from .models import ServiceLead

class ServiceLeadSerializer(serializers.ModelSerializer):
    class Meta:
        model = ServiceLead
        fields = "__all__"

