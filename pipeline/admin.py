from django.contrib import admin
from .models import DataFile


@admin.register(DataFile)
class DataFileAdmin(admin.ModelAdmin):
    list_display = (
        'id',
        'file',
        'status',
        'total_rows',
        'processed_rows',
        'progress_percent_display',
        'total_amount',
        'duration_seconds_display',
        'created_at',
    )
    list_filter = ('status', 'created_at')
    readonly_fields = (
        'celery_task_id',
        'total_rows',
        'processed_rows',
        'total_amount',
        'category_summary',
        'error_message',
        'started_at',
        'finished_at',
        'created_at',
        'updated_at',
    )

    @admin.display(description='Прогресс')
    def progress_percent_display(self, obj):
        return f"{obj.progress_percent}%"

    @admin.display(description='Длительность (сек)')
    def duration_seconds_display(self, obj):
        duration = obj.duration_seconds
        return f"{duration} с" if duration is not None else "-"
