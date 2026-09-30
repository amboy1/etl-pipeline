#!/usr/bin/env python3
"""
Скрипт для генерации тестового набора данных о транзакциях в формате CSV.
Генерирует поля: transaction_id, date, user_id, amount, category.
Работает потоково батчами, что позволяет генерировать файлы любого размера (1-2+ ГБ)
за минимальное время без переполнения оперативной памяти.
"""

import argparse
import csv
import os
import random
import sys
import time
import uuid
from datetime import datetime, timedelta

CATEGORIES = [
    "groceries",
    "electronics",
    "clothing",
    "restaurants",
    "entertainment",
    "utilities",
    "travel",
    "health",
    "services",
]


def generate_dataset(output_path: str, target_size_mb: float = None, total_rows: int = None, batch_size: int = 25000):
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

    start_time = time.time()
    start_date = datetime(2025, 1, 1, 0, 0, 0)
    date_range_seconds = int(timedelta(days=365).total_seconds())

    target_bytes = int(target_size_mb * 1024 * 1024) if target_size_mb else None
    rows_written = 0

    print(f"Начало генерации данных в файл: {output_path}")
    if target_bytes:
        print(f"Целевой размер: {target_size_mb} MB (~{target_bytes / (1024 * 1024):.1f} MB)")
    elif total_rows:
        print(f"Целевое количество строк: {total_rows:,}")

    with open(output_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["transaction_id", "date", "user_id", "amount", "category"])

        while True:
            # Проверка условий завершения
            if total_rows and rows_written >= total_rows:
                break
            if target_bytes:
                current_size = f.tell()
                if current_size >= target_bytes:
                    break

            batch = []
            current_batch_limit = batch_size
            if total_rows:
                current_batch_limit = min(batch_size, total_rows - rows_written)

            for _ in range(current_batch_limit):
                tx_id = uuid.uuid4().hex
                random_seconds = random.randint(0, date_range_seconds)
                tx_date = (start_date + timedelta(seconds=random_seconds)).strftime("%Y-%m-%d %H:%M:%S")
                user_id = random.randint(1, 200_000)
                amount = round(random.uniform(1.0, 5000.0), 2)
                category = random.choice(CATEGORIES)
                batch.append((tx_id, tx_date, user_id, amount, category))

            writer.writerows(batch)
            rows_written += len(batch)

            # Прогресс каждые 100k строк
            if rows_written % 100_000 == 0:
                elapsed = time.time() - start_time
                current_mb = f.tell() / (1024 * 1024)
                print(f"Сгенерировано {rows_written:,} строк | Размер: {current_mb:.1f} MB | Прошло: {elapsed:.1f} сек")

    file_size_mb = os.path.getsize(output_path) / (1024 * 1024)
    total_time = time.time() - start_time
    print("\nГенерация успешно завершена!")
    print(f"Файл: {output_path}")
    print(f"Строк: {rows_written:,}")
    print(f"Итоговый размер: {file_size_mb:.2f} MB")
    print(f"Время выполнения: {total_time:.2f} сек")


def main():
    parser = argparse.ArgumentParser(description="Генератор тестового CSV-файла транзакций для ETL пайплайна")
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--size-mb",
        type=float,
        help="Желаемый размер файла в мегабайтах (например: 100, 1024 для 1 ГБ, 2048 для 2 ГБ)",
    )
    group.add_argument(
        "--rows",
        type=int,
        help="Количество строк для генерации (например: 1000000)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="data/transactions.csv",
        help="Путь сохранения файла (по умолчанию: data/transactions.csv)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=25000,
        help="Размер батча для записи (по умолчанию: 25000)",
    )

    args = parser.parse_args()

    # Если ничего не задано, по умолчанию создаем небольшой файл на 50 000 строк для быстрых тестов
    if not args.size_mb and not args.rows:
        args.rows = 50_000

    generate_dataset(
        output_path=args.output,
        target_size_mb=args.size_mb,
        total_rows=args.rows,
        batch_size=args.batch_size,
    )


if __name__ == "__main__":
    main()
