import os
import redis
from django.conf import settings
from django.http import JsonResponse, HttpResponseBadRequest
from django.shortcuts import render, get_object_or_404
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST
from .models import DataFile, ProcessStatus
from .tasks import process_file

redis_client = redis.Redis.from_url(settings.CELERY_BROKER_URL, decode_responses=True)


def dashboard_view(request):
    """
    Главная страница дашборда: список файлов, статистика и панель управления.
    """
    files = DataFile.objects.all().order_by('-created_at')[:25]
    total_files = DataFile.objects.count()
    completed_files = DataFile.objects.filter(status=ProcessStatus.COMPLETED).count()

    context = {
        'files': files,
        'total_files': total_files,
        'completed_files': completed_files,
    }
    return render(request, 'pipeline/dashboard.html', context)


@csrf_exempt
@require_POST
def upload_file_view(request):
    """
    Эндпоинт загрузки CSV-файла через веб-интерфейс.
    """
    if 'file' not in request.FILES:
        return HttpResponseBadRequest(JsonResponse({'error': 'Файл не передан'}))

    uploaded_file = request.FILES['file']
    num_chunks = int(request.POST.get('num_chunks', 4))
    autostart = request.POST.get('autostart', 'true').lower() == 'true'

    data_file = DataFile.objects.create(file=uploaded_file)

    if autostart:
        task_res = process_file.delay(data_file.id, num_chunks=num_chunks)
        task_id = task_res.id
    else:
        task_id = None

    return JsonResponse({
        'success': True,
        'file_id': data_file.id,
        'filename': os.path.basename(data_file.file.name),
        'file_size_mb': round(data_file.file.size / (1024 * 1024), 2),
        'celery_task_id': task_id,
        'status': data_file.status,
    })


@csrf_exempt
@require_POST
def start_processing_view(request, file_id: int):
    """
    Ручной запуск обработки для ранее загруженного файла.
    """
    data_file = get_object_or_404(DataFile, id=file_id)
    num_chunks = int(request.POST.get('num_chunks', 4))

    task_res = process_file.delay(data_file.id, num_chunks=num_chunks)

    return JsonResponse({
        'success': True,
        'file_id': data_file.id,
        'celery_task_id': task_res.id,
        'status': ProcessStatus.PROCESSING,
    })


def file_progress_view(request, file_id: int):
    """
    Возвращает актуальный прогресс обработки файла.
    Приоритет: оперативная память Redis (до 1 мс) -> БД (fallback).
    """
    progress_key = f"file_progress:{file_id}"
    redis_data = redis_client.hgetall(progress_key)

    if redis_data:
        processed_rows = int(redis_data.get("processed_rows", 0))
        total_rows = int(redis_data.get("total_rows", 0)) if "total_rows" in redis_data else None
        status = redis_data.get("status", "UNKNOWN")
        error_message = redis_data.get("error_message") or None

        progress_percent = None
        if total_rows and total_rows > 0:
            progress_percent = round((processed_rows / total_rows) * 100, 2)

        return JsonResponse({
            "source": "redis",
            "file_id": file_id,
            "status": status,
            "processed_rows": processed_rows,
            "total_rows": total_rows,
            "progress_percent": progress_percent,
            "celery_task_id": redis_data.get("celery_task_id") or None,
            "error_message": error_message,
        })

    # Фоллбэк на базу данных
    data_file = get_object_or_404(DataFile, id=file_id)
    return JsonResponse({
        "source": "database",
        "file_id": data_file.id,
        "status": data_file.status,
        "processed_rows": data_file.processed_rows,
        "total_rows": data_file.total_rows,
        "progress_percent": data_file.progress_percent,
        "celery_task_id": data_file.celery_task_id,
        "error_message": data_file.error_message,
    })


def file_details_view(request, file_id: int):
    """
    Возвращает детальные финансовые агрегаты и сводку по категориям.
    """
    data_file = get_object_or_404(DataFile, id=file_id)
    return JsonResponse({
        "file_id": data_file.id,
        "status": data_file.status,
        "total_rows": data_file.total_rows,
        "processed_rows": data_file.processed_rows,
        "total_amount": float(data_file.total_amount),
        "duration_seconds": data_file.duration_seconds,
        "category_summary": data_file.category_summary,
        "error_message": data_file.error_message,
        "created_at": data_file.created_at.strftime("%Y-%m-%d %H:%M:%S"),
    })
