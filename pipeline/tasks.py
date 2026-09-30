import csv
import os
from collections import defaultdict
from decimal import Decimal
import time
from celery import shared_task, chord
from django.conf import settings
from django.utils import timezone
import redis
from .models import DataFile, ProcessStatus

# Redis-клиент для хранения оперативного прогресса
redis_client = redis.Redis.from_url(settings.CELERY_BROKER_URL, decode_responses=True)

PROGRESS_BATCH_SIZE = getattr(settings, 'PROGRESS_BATCH_SIZE', 10_000)
PROGRESS_TTL = getattr(settings, 'PROGRESS_TTL', 86400)  # 24 часа
DEFAULT_NUM_CHUNKS = getattr(settings, 'DEFAULT_NUM_CHUNKS', 4)


def get_progress_key(file_id: int) -> str:
    return f"file_progress:{file_id}"


def get_file_chunks(file_path: str, num_chunks: int = 4) -> list[tuple[int, int]]:
    """
    Разбивает файл на равные диапазоны байт без физического копирования.
    Возвращает список кортежей [(start_byte, end_byte), ...]
    """
    total_size = os.path.getsize(file_path)

    # Если файл меньше 1 МБ или запрошен 1 чанк — обрабатываем целиком
    if total_size < 1024 * 1024 or num_chunks <= 1:
        return [(0, total_size)]

    chunk_size = total_size // num_chunks
    chunks = []

    for i in range(num_chunks):
        start = i * chunk_size
        # Последний чанк забирает остаток до самого конца файла
        end = total_size if i == num_chunks - 1 else (i + 1) * chunk_size
        chunks.append((start, end))

    return chunks


@shared_task
def test_processing(seconds):
    time.sleep(seconds)
    return f"Task completed in {seconds} seconds"


@shared_task
def process_chunk(file_id: int, start_byte: int, end_byte: int) -> dict:
    """
    Map-задача: обрабатывает один байтовый диапазон файла [start_byte, end_byte).
    Выполняется параллельно в отдельном воркере на отдельном ядре.
    """
    data_file = DataFile.objects.get(id=file_id)
    progress_key = get_progress_key(file_id)

    total_amount = Decimal('0.00')
    rows_count = 0
    batch_counter = 0
    category_summary = defaultdict(Decimal)

    with open(data_file.file.path, mode='r', encoding='utf-8') as f:
        f.seek(start_byte)

        # Выравнивание строк:
        # Если start_byte == 0 -> пропускаем строку заголовков CSV
        # Если start_byte > 0  -> пропускаем обрубок строки, начатый в прошлом чанке
        f.readline()

        while True:
            line = f.readline()
            if not line:
                break

            parts = line.rstrip('\r\n').split(',')
            if len(parts) < 5:
                continue

            amount = Decimal(parts[3])
            category = parts[4]

            rows_count += 1
            batch_counter += 1
            total_amount += amount
            category_summary[category] += amount

            # Каждые 10 000 строк атомарно инкрементируем общий счетчик в Redis
            if batch_counter >= PROGRESS_BATCH_SIZE:
                redis_client.hincrby(progress_key, "processed_rows", batch_counter)
                batch_counter = 0

            # Если перешагнули границу своего байтового диапазона — завершаем
            if f.tell() >= end_byte:
                break

    # Сбрасываем хвост батча в Redis
    if batch_counter > 0:
        redis_client.hincrby(progress_key, "processed_rows", batch_counter)

    return {
        "rows_count": rows_count,
        "total_amount": str(total_amount),
        "category_summary": {k: str(v) for k, v in category_summary.items()},
    }


@shared_task
def combine_chunk_results(results: list[dict], file_id: int):
    """
    Reduce-задача (Callback): вызывается Celery автоматически, когда ВСЕ чанки завершены.
    Суммирует промежуточные результаты всех воркеров и сохраняет итог в БД и Redis.
    """
    try:
        data_file = DataFile.objects.get(id=file_id)
    except DataFile.DoesNotExist:
        return f"File {file_id} not found"

    progress_key = get_progress_key(file_id)

    total_rows = 0
    total_amount = Decimal('0.00')
    category_summary = defaultdict(Decimal)

    # Суммируем результаты от всех параллельных воркеров
    for chunk_res in results:
        total_rows += chunk_res["rows_count"]
        total_amount += Decimal(chunk_res["total_amount"])
        for cat, val in chunk_res["category_summary"].items():
            category_summary[cat] += Decimal(val)

    # 1. Фиксируем итоговый результат в Базе Данных
    data_file.total_rows = total_rows
    data_file.processed_rows = total_rows
    data_file.total_amount = total_amount
    data_file.category_summary = {k: str(v) for k, v in category_summary.items()}
    data_file.status = ProcessStatus.COMPLETED
    data_file.finished_at = timezone.now()
    data_file.save()

    # 2. Обновляем финальный статус в Redis
    redis_client.hset(progress_key, mapping={
        "status": ProcessStatus.COMPLETED,
        "processed_rows": total_rows,
        "total_rows": total_rows,
    })
    redis_client.expire(progress_key, PROGRESS_TTL)
    return f"File {file_id} completed: {total_rows} rows"


@shared_task(bind=True)
def process_file(self, file_id: int, num_chunks: int = 4):
    """
    Диспетчер (Orchestrator):
    Нарезает файл на чанки и распределяет их по воркерам через Celery chord.
    """
    try:
        data_file = DataFile.objects.get(id=file_id)
    except DataFile.DoesNotExist:
        return f"File {file_id} not found"

    progress_key = get_progress_key(file_id)

    # 1. Инициализация в БД и Redis
    data_file.status = ProcessStatus.PROCESSING
    data_file.celery_task_id = self.request.id
    data_file.started_at = timezone.now()
    data_file.error_message = None
    data_file.save(update_fields=['status', 'celery_task_id', 'started_at', 'error_message'])

    redis_client.hset(progress_key, mapping={
        "status": ProcessStatus.PROCESSING,
        "processed_rows": 0,
        "celery_task_id": self.request.id or "",
        "error_message": "",
    })
    redis_client.expire(progress_key, PROGRESS_TTL)

    try:
        # 2. Нарезаем файл на байтовые диапазоны
        chunks = get_file_chunks(data_file.file.path, num_chunks=num_chunks)

        # 3. Формируем группу параллельных задач (Map) и колбэк сборщика (Reduce)
        header_tasks = [process_chunk.s(file_id, start, end) for start, end in chunks]
        callback_task = combine_chunk_results.s(file_id)

        # Celery Chord: параллельно запускает воркеры и передает их ответы в combine_chunk_results
        return chord(header_tasks)(callback_task)

    except Exception as exc:
        data_file.status = ProcessStatus.FAILED
        data_file.error_message = str(exc)
        data_file.finished_at = timezone.now()
        data_file.save()

        redis_client.hset(progress_key, mapping={
            "status": ProcessStatus.FAILED,
            "error_message": str(exc),
        })
        redis_client.expire(progress_key, PROGRESS_TTL)
        raise exc
