from django.utils import timezone
from django.conf import settings
from django.db import models


class Dataset(models.Model): 
    class Group(models.TextChoices):
        STUDENT = "Student", "Student Datasets"
        AGGREGATE = "Aggregate", "Aggregate Datasets"
        COMBINED = "Combined", "Combined Datasets"
        ANALYTICS = "Analytics", "Analytics Datasets" 

    name = models.CharField()
    group = models.CharField(choices=Group.choices)
    description = models.TextField()
    query = models.TextField()
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.name.replace("_", " ")