from django.contrib import admin
from .models import Dataset

class DatasetAdmin(admin.ModelAdmin):
    list_display = ('name', 'group',)
    list_filter = ('group',)
    search_fields = ('name',)
    ordering = ('name',)
    readonly_fields = ['created_at', 'updated_at']


admin.site.register(Dataset, DatasetAdmin)