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


def generate_dataset(output_path: str, target_size_mb: float = None, total_rows: int = None, batch_size: int = 50000):
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

    start_time = time.time()
    target_bytes = int(target_size_mb * 1024 * 1024) if target_size_mb else None
    rows_written = 0

    print(f"Начало генерации данных в файл: {output_path}")
    if target_bytes:
        print(f"Целевой размер: {target_size_mb} MB (~{target_bytes / (1024 * 1024):.1f} MB)")
    elif total_rows:
        print(f"Целевое количество строк: {total_rows:,}")

    # Предварительно генерируем пул дат для максимальной скорости
    dates_pool = [
        f"2025-{(m % 12) + 1:02d}-{(d % 28) + 1:02d} {(h % 24):02d}:{(s % 60):02d}:00"
        for m in range(12) for d in range(28) for h in range(4) for s in range(5)
    ]
    pool_len = len(dates_pool)
    cat_len = len(CATEGORIES)

    with open(output_path, mode="w", newline="", encoding="utf-8", buffering=1024 * 1024 * 16) as f:
        f.write("transaction_id,date,user_id,amount,category\n")

        while True:
            if total_rows and rows_written >= total_rows:
                break
            if target_bytes:
                if f.tell() >= target_bytes:
                    break

            current_batch_limit = batch_size
            if total_rows:
                current_batch_limit = min(batch_size, total_rows - rows_written)

            # Формируем текстовый блок в памяти и пишем одним куском
            lines = []
            for i in range(current_batch_limit):
                idx = rows_written + i
                tx_id = f"{idx:032x}"
                tx_date = dates_pool[idx % pool_len]
                user_id = (idx * 7) % 200000 + 1
                amount = ((idx * 37) % 499900 + 100) / 100.0
                category = CATEGORIES[idx % cat_len]
                lines.append(f"{tx_id},{tx_date},{user_id},{amount:.2f},{category}\n")

            f.write("".join(lines))
            rows_written += current_batch_limit

            if rows_written % 1_000_000 == 0:
                elapsed = time.time() - start_time
                current_mb = f.tell() / (1024 * 1024)
                rate_mb = current_mb / elapsed if elapsed > 0 else 0
                print(f"Сгенерировано {rows_written:,} строк | Размер: {current_mb:.1f} MB | Скорость: {rate_mb:.1f} MB/сек")

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
