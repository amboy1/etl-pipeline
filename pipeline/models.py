import os
from decimal import Decimal
from django.db import models


class ProcessStatus(models.TextChoices):
    PENDING = 'PENDING', 'В очереди'
    SPLITTING = 'SPLITTING', 'Разделение на чанки'
    PROCESSING = 'PROCESSING', 'В обработке'
    COMPLETED = 'COMPLETED', 'Завершено'
    FAILED = 'FAILED', 'Ошибка'


class DataFile(models.Model):
    file = models.FileField(
        upload_to='uploads/%Y/%m/%d/',
        verbose_name='CSV файл'
    )
    status = models.CharField(
        max_length=20,
        choices=ProcessStatus.choices,
        default=ProcessStatus.PENDING,
        db_index=True,
        verbose_name='Статус'
    )
    celery_task_id = models.CharField(
        max_length=255,
        blank=True,
        null=True,
        db_index=True,
        verbose_name='ID задачи Celery'
    )
    
    total_rows = models.BigIntegerField(
        default=0,
        verbose_name='Всего строк'
    )
    processed_rows = models.BigIntegerField(
        default=0,
        verbose_name='Обработано строк'
    )
    
    total_amount = models.DecimalField(
        max_digits=20,
        decimal_places=2,
        default=Decimal('0.00'),
        verbose_name='Общая сумма транзакций'
    )
    category_summary = models.JSONField(
        default=dict,
        blank=True,
        verbose_name='Сводка по категориям'
    )
    error_message = models.TextField(
        blank=True,
        null=True,
        verbose_name='Сообщение об ошибке'
    )

    created_at = models.DateTimeField(auto_now_add=True, verbose_name='Создан')
    updated_at = models.DateTimeField(auto_now=True, verbose_name='Обновлен')
    started_at = models.DateTimeField(null=True, blank=True, verbose_name='Начало обработки')
    finished_at = models.DateTimeField(null=True, blank=True, verbose_name='Завершение обработки')

    class Meta:
        verbose_name = 'Файл данных'
        verbose_name_plural = 'Файлы данных'
        ordering = ['-created_at']

    def __str__(self):
        filename = os.path.basename(self.file.name) if self.file else f"DataFile #{self.pk}"
        return f"{filename} [{self.get_status_display()}]"

    @property
    def progress_percent(self) -> float:
        if self.total_rows > 0:
            return round((self.processed_rows / self.total_rows) * 100, 2)
        return 0.0

    @property
    def duration_seconds(self) -> float | None:
        if self.started_at and self.finished_at:
            return round((self.finished_at - self.started_at).total_seconds(), 2)
        return None
