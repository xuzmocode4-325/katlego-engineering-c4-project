"""
Django admin customisation
"""
from django.contrib import admin
from django.contrib.auth.admin import UserAdmin as BaseUserAdmin


from .models import User


class CustomUserAdmin(BaseUserAdmin):
    """Define the admin pages for users"""
    list_display = ('email', 'name', 'is_active')
    list_filter = ('is_active',)
    search_fields = ('email', 'name')
    ordering = ('email',)
    filter_horizontal = ()
    fieldsets = (
        (None, {'fields': ('email', 'password')}),
        ('Personal info', {'fields': ('name',)}),
        ('Permissions', {'fields': (
            'is_active',
            'is_staff',
            'is_superuser')}),
        ('Important Dates', {'fields': ('last_login',)})
    )
    add_fieldsets = ((
        None, {
            'classes': ('wide',),
            'fields': (
                'email',
                'name',
                'password1',
                'password2',
                'is_active',
                'is_staff',
                'is_superuser'
                )
            }
        ),
    )
    readonly_fields = ['last_login']

    def changelist_view(self, request, extra_context=None):
        # Add custom context data if needed
        extra_context = extra_context or {}
        message = "Welcome to the user management panel!"
        extra_context['custom_message'] = message

        return super().changelist_view(request, extra_context=extra_context)


admin.site.register(User, CustomUserAdmin)