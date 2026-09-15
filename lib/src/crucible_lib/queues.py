"""Celery queue names shared by the control plane (sender) and the worker.

Celery routing is resolved *sender-side*, so the control plane must name the
same queue the worker consumes.  Keeping the names here means the two can't
drift apart.

This binds the Python sender and consumer only.  The deployment manifests
(``helm/crucible/templates/deployment-worker.yaml`` and
``infrastructure/docker-compose.yml``) repeat these strings in their ``-Q``
flags, because YAML cannot import them — so renaming a queue here means
renaming it there in the same commit, or the workers consume the old queue
while the control plane publishes to the new one and every run hangs with no
error.

The two queues map to two worker deployments with independent pools:

* ``dispatch`` — ``dispatcher_task``.  One slot is held for the whole run
  (SUT lease + fixture load + completion wait), so these slots are long-lived.
* ``execute``  — ``k6_executor_task``.  Spawns the k6 processes that generate
  load.

They are kept apart because a dispatcher blocks on its executor: sharing one
pool lets dispatchers occupy every slot and starve the executors they are
waiting for, deadlocking the run.
"""

DISPATCH_QUEUE = "dispatch"
EXECUTE_QUEUE = "execute"
