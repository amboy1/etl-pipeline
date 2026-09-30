import os
from celery import Celery

# Устанавливаем переменную окружения для настроек Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'etl_project.settings')

app = Celery('etl_project')

# Читаем конфигурацию из settings.py, используя префикс 'CELERY_'
app.config_from_object('django.conf:settings', namespace='CELERY')

# Автоматически находим tasks.py во всех зарегистрированных приложениях Django
app.autodiscover_tasks()


@app.task(bind=True, ignore_result=True)
def debug_task(self):
    print(f'Request: {self.request!r}')
