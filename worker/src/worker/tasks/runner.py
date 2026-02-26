from worker.celery_app import app
from worker.orchestrator.taurus import execute_taurus


@app.task(bind=True, name="worker.tasks.runner.run_worker_task")
def run_worker_task(self, master_result: dict, plan: dict, count: int) -> dict:
    """Spawned by the Celery chain after the master task completes.

    Receives the master's IP from the previous task result and launches
    ``count`` Taurus worker processes that connect back to the master.
    """
    master_ip: str = master_result["master_ip"]

    overrides: dict = {"execution": plan["execution"]}
    overrides["execution"][0].update({"master-host": master_ip})

    for _ in range(count):
        execute_taurus(plan, overrides)

    return {"workers_spawned": count, "master_ip": master_ip}
