from worker.celery_app import app
from worker.config import settings
from worker.fixture_loader.base import FixtureLoader
from worker.orchestrator.taurus import execute_taurus


@app.task(bind=True, name="worker.tasks.master.run_master_task")
def run_master_task(self, plan: dict, mode: str) -> dict:
    """Primary task that runs on the designated master node.

    Responsibilities (in order):
      1. Acquire the SUT lease via the LeaseManager.
      2. Hydrate the SUT with fixture data via the FixtureLoader.
      3. Launch Taurus as master (intra-node or inter-node).
    """
    component: str = plan["test_environment"]["component"]

    # ── 1. Fixture loading ──────────────────────────────────────────────────
    for fixture in plan.get("fixtures", []):
        loader = FixtureLoader(component=component, config=fixture)
        loader.load()

    # ── 2. Taurus orchestration ─────────────────────────────────────────────
    overrides: dict = {"execution": plan["execution"]}

    if mode == "intra_node":
        overrides["execution"][0].update({
            "master": True,
            "workers": plan["test_environment"].get("worker_count", 1),
        })
    elif mode == "inter_node":
        overrides["execution"][0].update({
            "master": True,
            "expect-workers": plan["test_environment"].get("cluster_size", 1) - 1,
        })

    execute_taurus(plan, overrides)

    return {"master_ip": settings.runner_ip}
