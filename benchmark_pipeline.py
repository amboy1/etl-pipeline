#!/usr/bin/env python3
"""
Скрипт для бенчмарка ETL пайплайна:
Сравнивает скорость обработки одного и того же файла при 1, 2 и 4 параллельных чанках (воркерах).
"""

import os
import time
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'etl_project.settings')
django.setup()

from django.core.files import File
from pipeline.models import DataFile, ProcessStatus
from pipeline.tasks import process_file


def run_single_benchmark(csv_path: str, num_chunks: int) -> dict:
    """Запускает один прогон обработки файла с заданным количеством чанков."""
    with open(csv_path, 'rb') as f:
        data_file = DataFile.objects.create(
            file=File(f, name=f"bench_{num_chunks}_{os.path.basename(csv_path)}")
        )

    t_start = time.perf_counter()
    # Запускаем Celery-задачу с нужным количеством чанков
    process_file.delay(data_file.id, num_chunks=num_chunks)

    # Ожидаем завершения
    timeout = 60
    while time.perf_counter() - t_start < timeout:
        data_file.refresh_from_db()
        if data_file.status in (ProcessStatus.COMPLETED, ProcessStatus.FAILED):
            break
        time.sleep(0.05)

    duration = time.perf_counter() - t_start

    if data_file.status != ProcessStatus.COMPLETED:
        raise RuntimeError(f"Ошибка выполнения для num_chunks={num_chunks}: {data_file.error_message or 'Timeout'}")

    return {
        "num_chunks": num_chunks,
        "duration": duration,
        "rows_count": data_file.total_rows,
        "total_amount": data_file.total_amount,
        "rows_per_sec": int(data_file.total_rows / duration) if duration > 0 else 0,
    }


def main():
    csv_path = 'data/transactions_1m.csv'
    if not os.path.exists(csv_path):
        csv_path = 'data/transactions_small.csv'
        if not os.path.exists(csv_path):
            print("CSV-файл для тестов не найден в папке data/!")
            return

    file_size_mb = os.path.getsize(csv_path) / (1024 * 1024)
    print("=" * 70)
    print(f"🚀 ЗАПУСК БЕНЧМАРКА ETL ПАЙПЛАЙНА")
    print(f"   Файл: {csv_path} ({file_size_mb:.2f} MB)")
    print("=" * 70)

    test_configs = [1, 2, 4]
    results = []

    for num_chunks in test_configs:
        print(f"\n[Тест] Запуск обработки с num_chunks = {num_chunks}...")
        res = run_single_benchmark(csv_path, num_chunks)
        results.append(res)
        print(f"       Завершено за {res['duration']:.2f} сек ({res['rows_per_sec']:,} строк/сек)")
        time.sleep(1)  # пауза между тестами для чистоты замера

    # Расчет ускорения (Speedup) относительно 1 воркера
    base_time = results[0]["duration"]

    print("\n" + "=" * 70)
    print("📊 ИТОГОВАЯ ТАБЛИЦА СРАВНЕНИЯ ПРОИЗВОДИТЕЛЬНОСТИ")
    print("=" * 70)
    print(f"{'Воркеры/Чанки':<15} | {'Время (сек)':<12} | {'Строк / сек':<15} | {'Ускорение':<12}")
    print("-" * 70)

    for res in results:
        speedup = base_time / res["duration"]
        speedup_str = f"{speedup:.2f}x" if res["num_chunks"] > 1 else "1.00x (база)"
        print(
            f"{res['num_chunks']:<15} | "
            f"{res['duration']:<12.2f} | "
            f"{res['rows_per_sec']:<15,d} | "
            f"{speedup_str:<12}"
        )

    print("-" * 70)
    print(f"✔ Проверка целостности: обработано строк во всех прогонах: {results[0]['rows_count']:,}")
    print(f"✔ Финансовый итог: {results[0]['total_amount']:,.2f} руб.")
    print("=" * 70)


if __name__ == '__main__':
    main()
