from django.urls import path
from .views import (
    dashboard_view,
    upload_file_view,
    start_processing_view,
    file_progress_view,
    file_details_view,
)

app_name = 'pipeline'

urlpatterns = [
    path('', dashboard_view, name='dashboard'),
    path('api/upload/', upload_file_view, name='upload_file'),
    path('api/start/<int:file_id>/', start_processing_view, name='start_processing'),
    path('api/progress/<int:file_id>/', file_progress_view, name='file_progress'),
    path('api/details/<int:file_id>/', file_details_view, name='file_details'),
]
