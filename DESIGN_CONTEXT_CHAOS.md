# 1. Architectural Philosophy
Crucible goes beyond basic load generation by incorporating active Resilience Testing (Chaos Engineering). The system uses Chaos Mesh as its primary fault injection engine. This allows Crucible to evaluate how distributed systems (e.g., Doris, Trino, Cassandra) handle extreme data ingestion and analytical queries during catastrophic infrastructure failures.

To support hybrid infrastructure environments, the integration utilizes:
* Kubernetes (K8s): Native Chaos Mesh Custom Resource Definitions (CRDs) applied via the Kubernetes API.
* Direct EC2 / Bare Metal: The `chaosd` agent exposing a REST API on the target instances.

# 2. Component Architecture: The Chaos Injector
The Chaos Injector is a sub-component managed by the Celery Dispatcher Task (the orchestrator).

## 2.1 Execution Lifecycle
The chaos injection is synchronized with the Two-Phase Check-In of the load testing workers:
1. Pre-flight & Fixtures: Workers download fixtures and check into the PostgreSQL Waiting Room.
2. The Starting Gun: The Dispatcher flips the database state to `START`, triggering the k6 workers.
3. Chaos Scheduling: The Dispatcher reads the `chaos_spec` and initiates a non-blocking countdown (e.g., waiting 1 minute for the k6 load to stabilize).
4. Injection: The Dispatcher routes the fault request to either the K8s API or the target's `chaosd` HTTP API.
5. Restoration: Once the chaos duration expires, the Dispatcher actively deletes the CRD or sends a recovery command to `chaosd`, allowing the system to recover while k6 continues to record metrics.

## 2.2 Routing Logic (Python Implementation)

The Dispatcher translates the declarative YAML into the appropriate API calls:

```yaml
import requests
from kubernetes import client, config
def execute_chaos_experiment(chaos_spec: dict, run_id: str):
    target_env = chaos_spec["target"]["env_type"]
    
    if target_env == "k8s":
        # Apply Chaos Mesh CRD
        config.load_incluster_config()
        crd_api = client.CustomObjectsApi()
        manifest = build_chaos_mesh_crd(chaos_spec, run_id)
        crd_api.create_namespaced_custom_object(
            group="chaos-mesh.org", version="v1alpha1",
            namespace=chaos_spec["target"]["namespace"],
            plural=chaos_spec["fault_type"], # e.g., "networkchaos"
            body=manifest
        )

    elif target_env == "ec2":
        # Trigger Chaosd REST API
        target_ip = chaos_spec["target"]["address"]
        payload = build_chaosd_payload(chaos_spec)
        requests.post(f"http://{target_ip}:31767/api/attack/{chaos_spec['fault_type']}", json=payload)
```

# 3. Test Plan YAML Specification (Chaos Extension)
The Crucible YAML schema includes a dedicated `chaos_spec` block. It defines what breaks, where it breaks, and when it breaks.
The `chaos_spec` block is optional. Crucible will skip it if not defined.

## 3.1 Data Dictionary
* `chaos_spec`: Root block for fault injection.
  * `engine`: The engine to execute the chaos. Current only support `chaos-mesh`.
  * `experiments`: List of discrete fault events.
    * `name`: Identifier for the fault.
    * `fault_type`: The Chaos Mesh category (`network_delay`, `pod_kill`, `disk_fill`, `cpu_burn`).
    * `target`: The SUT topology routing.
      * `env_type`: `k8s` or `ec2`.
      * `selector`: K8s labels to target specific pods (e.g., Doris BEs).
      * `address`: IP address (if `env_type` is `ec2`).
    * `parameters`: Fault-specific variables (latency amount, disk percentage).
    * `schedule`: Temporal controls aligned with the k6 `hold_for` duration.
      * `start_after`: Delay injection to allow baseline metrics to form.
      * `duration`: How long the fault persists before automated recovery.

# 4. Valid YAML Example: Doris Network Partition

```yaml
test_metadata:
  run_label: "Doris_BE_Network_Degradation_Test"
test_environment:
  env_type: long-lived
  target_db: crucible_benchmark
  component_spec:
    type: doris
    cluster_info:
      host: doris-fe.doris-cluster.svc.cluster.local:9030
  fixtures:
    - fixture_id: tpch-sf100-lineitem
      table: lineitem

execution:
  executor: k6
  scaling_mode: inter_node
  concurrency: 5000
  ramp_up: 30s
  hold_for: 10m
  workload:
    - workload_id: tpch-complex-joins

chaos_spec:
  engine: "chaos-mesh"
  experiments:
    - name: "degrade-backend-network"
      fault_type: networkchaos
      target:
        env_type: k8s
        namespace: doris-cluster
        selector:
          app.kubernetes.io/component: be
      parameters:
        action: delay
        delay:
          latency: "200ms"
          jitter: "50ms"
      schedule:
        start_after: "1m"
        duration: "3m"
```
