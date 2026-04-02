# Chaos Injector Implementation Plan

Reference: `DESIGN_CONTEXT_CHAOS.md`

## Phase 1: Schema & Validation

**File:** `lib/src/crucible_lib/schemas/test_plan.py`

Add Pydantic models:

| Model | Fields |
|---|---|
| `ChaosTarget` | `env_type` (literal "k8s" \| "ec2"), `namespace` (optional), `selector` (optional dict), `address` (optional) |
| `ChaosSchedule` | `start_after` (str, k6 duration), `duration` (str, k6 duration) |
| `ChaosExperiment` | `name`, `fault_type` (str), `target: ChaosTarget`, `parameters: dict`, `schedule: ChaosSchedule` |
| `ChaosSpec` | `engine` (literal "chaos-mesh"), `experiments: list[ChaosExperiment]` |

Add `chaos_spec: ChaosSpec | None = None` to `TestPlan`.

Validation:
- `env_type=k8s` requires `namespace` + `selector`
- `env_type=ec2` requires `address`
- Warn (not error) if `start_after + duration > hold_for`

This immediately makes `validate_test_plan()` in MCP aware of chaos configs.

---

## Phase 2: Chaos Injector Module

**New directory:** `worker/src/worker/chaos_injector/`

| File | Responsibility |
|---|---|
| `__init__.py` | Re-exports |
| `base.py` | `ChaosEngine` ABC with `inject(experiment) -> str` and `recover(experiment, handle)` |
| `k8s_engine.py` | Creates/deletes Chaos Mesh CRDs via `kubernetes` client |
| `chaosd_engine.py` | POST/DELETE to `chaosd` REST API on target EC2 instances |
| `scheduler.py` | `ChaosScheduler(Thread)` — orchestrates timing: sleep `start_after` -> inject -> sleep `duration` -> recover. Supports `cancel()` for early cleanup. |

Design choice: **Thread inside dispatcher, not a separate Celery task.**
- Chaos must be cancelled if the run aborts (dispatcher holds the SUT lease)
- Recovery is critical — the `finally` block can cancel + join the thread
- Keeps chaos lifecycle tightly coupled to SUT lease ownership

---

## Phase 3: Dispatcher Integration

**File:** `worker/src/worker/tasks/dispatcher.py`

After EXECUTING status is set and k6 is running:

```python
chaos_thread = None
if plan.get("chaos_spec"):
    chaos_thread = ChaosScheduler(plan["chaos_spec"], run_id)
    chaos_thread.start()  # non-blocking
```

In `finally` block (alongside `release_sut_lock`):

```python
if chaos_thread and chaos_thread.is_alive():
    chaos_thread.cancel()  # triggers early recovery
    chaos_thread.join(timeout=30)
```

For inter-node, chaos starts after the START signal (all workers checked in).

`completion_timeout` accounts for `max(start_after + duration)` across experiments.

---

## Phase 4: Dependencies & Deployment

**`worker/pyproject.toml`** — Add:
- `kubernetes >= 29.0.0`
- `requests >= 2.28.0`

**Helm RBAC** — Worker ServiceAccount needs ClusterRole for Chaos Mesh CRDs:
```yaml
rules:
  - apiGroups: ["chaos-mesh.org"]
    resources: ["networkchaos", "podchaos", "stresschaos", "iochaos"]
    verbs: ["create", "delete", "get", "list"]
```

Chaos Mesh itself is assumed pre-installed on the target K8s cluster.

---

## Phase 5: MCP & Observability

Update docstrings on `validate_test_plan`, `submit_test_run`, `trigger_run_by_plan` to document `chaos_spec` block.

No new MCP tools initially — chaos is declared in test plan YAML and executed automatically.

Results enrichment: include chaos timing metadata (inject/recover timestamps) in the results JSON.

---

## Phase 6: Tests

| Test | Scope |
|---|---|
| Unit: schema validation | Valid/invalid chaos_spec combinations |
| Unit: k8s_engine | Mock `kubernetes` client, verify CRD create/delete |
| Unit: chaosd_engine | Mock `requests`, verify POST/DELETE payloads |
| Unit: scheduler | Mock engine, verify timing + cancel behavior |
| Unit: dispatcher | Verify chaos thread lifecycle (start, cancel on failure) |
| Integration/e2e | Requires Chaos Mesh — defer to EKS testing |

---

## Implementation Order

1. Phase 1 (Schema)
2. Phase 2 (Injector module)
3. Phase 3 (Dispatcher integration)
4. Phase 4 (Dependencies + Helm)
5. Phase 5 (MCP docs)
6. Phase 6 (Tests)
