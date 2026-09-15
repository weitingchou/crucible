from celery import Celery

from crucible_lib.queues import DISPATCH_QUEUE, EXECUTE_QUEUE

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
    # Dispatchers and executors run on separate queues, consumed by separate
    # deployments.  A dispatcher holds its slot until its executor finishes, so
    # on a shared pool dispatchers can occupy every slot and starve the
    # executors they wait for.
    task_default_queue=DISPATCH_QUEUE,
    task_routes={
        "worker.tasks.dispatcher.*": {"queue": DISPATCH_QUEUE},
        "worker.tasks.executor.*": {"queue": EXECUTE_QUEUE},
    },
    # Take one message at a time.  The default (4) lets a busy worker reserve
    # tasks it has no free slot for, keeping them from idle workers.
    worker_prefetch_multiplier=1,
    # task_acks_late stays OFF on purpose.  Turning it on without first raising
    # RabbitMQ's consumer_timeout (unset, so the 30-minute default) would have
    # the broker close the channel and redeliver any run longer than 30 min —
    # i.e. every soak test.  Change both together or neither.
    task_acks_late=False,
)
