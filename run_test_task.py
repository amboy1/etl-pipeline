import os
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'etl_project.settings')
django.setup()

from pipeline.tasks import test_processing

if __name__ == '__main__':
    print("Отправка тестовой задачи в Celery...")
    result = test_processing.delay(2)
    print(f"Задача отправлена! Task ID: {result.id}")
    print("Статус:", result.status)
