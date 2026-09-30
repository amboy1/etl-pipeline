import os
import time
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'etl_project.settings')
django.setup()

import redis
from django.conf import settings
from django.core.files import File
from django.test import Client
from pipeline.models import DataFile, ProcessStatus
from pipeline.tasks import process_file


def main():
    # Проверяем наличие 1M датасета, если нет — берем small
    csv_source = 'data/transactions_1m.csv' if os.path.exists('data/transactions_1m.csv') else 'data/transactions_small.csv'
    if not os.path.exists(csv_source):
        print(f"Файл {csv_source} не найден!")
        return

    redis_client = redis.Redis.from_url(settings.CELERY_BROKER_URL, decode_responses=True)
    client = Client()

    print(f"1. Создаем объект DataFile в базе данных из '{csv_source}'...")
    with open(csv_source, 'rb') as f:
        data_file = DataFile.objects.create(
            file=File(f, name=os.path.basename(csv_source))
        )
    print(f"   DataFile создан: ID={data_file.id}, путь: {data_file.file.path}")

    print("\n2. Отправляем задачу process_file в Celery...")
    async_result = process_file.delay(data_file.id)
    print(f"   Task ID: {async_result.id}")

    print("\n3. Опрашиваем эндпоинт /api/progress/{file_id}/ в реальном времени...")
    for _ in range(40):
        # Делаем HTTP GET запрос к нашему новому эндпоинту
        response = client.get(f"/api/progress/{data_file.id}/")
        api_data = response.json()

        status = api_data.get('status')
        processed = api_data.get('processed_rows', 0)
        source = api_data.get('source')

        print(f"   [API /api/progress/{data_file.id}/] Источник: {source:<8} | Статус: {status:<10} | Обработано строк: {processed:>10,}")

        if status in (ProcessStatus.COMPLETED, ProcessStatus.FAILED):
            break
        time.sleep(0.3)

    print("\n--- Финальный результат из Базы Данных ---")
    data_file.refresh_from_db()
    print(f"Итоговый статус: {data_file.status}")
    print(f"Всего строк: {data_file.total_rows:,}")
    print(f"Общая сумма транзакций: {data_file.total_amount:,.2f} руб.")
    print(f"Время выполнения: {data_file.duration_seconds} сек.")
    print(f"Категории:")
    for cat, total in data_file.category_summary.items():
        print(f"  - {cat}: {float(total):,.2f}")

    # Финальное состояние ключа в Redis
    final_redis = redis_client.hgetall(f"file_progress:{data_file.id}")
    print(f"\n--- Содержимое ключа 'file_progress:{data_file.id}' в Redis ---")
    print(final_redis)

    if data_file.error_message:
        print(f"ОШИБКА: {data_file.error_message}")


if __name__ == '__main__':
    main()

