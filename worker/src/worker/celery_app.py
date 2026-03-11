from celery import Celery

from .config import settings

app = Celery(
    "crucible_worker",
    broker=settings.celery_broker_url,
    backend=settings.celery_result_backend,
    include=[
        "worker.tasks.dispatcher",
        "worker.tasks.executor",
    ],
)

app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    task_track_started=True,
    # Results expire after 24 hours.
    result_expires=86400,
)
