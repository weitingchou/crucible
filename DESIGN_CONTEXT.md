# Project Crucible - Design Context

**Project Codename:** Crucible (Inspired by the concept of "Severe Trial" / 試煉)

---

## 1. System Requirements (V1)

### 1.1 Cloud-Native Architecture & Orchestration
* **Asynchronous Task Management:** An API service paired with a job queue to handle asynchronous task distribution and lifecycle management across a fleet of execution workers.
* **Centralized System Configuration:** Sensitive infrastructure details (e.g., storage credentials, message broker URLs, metadata DB connections) must be securely managed globally on the worker nodes and kept completely hidden from the end-user.

### 1.2 Standardized Testing Abstraction Layer
* **Unified Driver Interface:** A standard abstraction layer capable of supporting multiple testing driver tools (e.g., k6, Locust) without requiring custom script changes for each tool.
* **Process Tree Management:** Robust lifecycle management to safely start, monitor, gracefully terminate, and clean up the underlying testing driver processes.
* **Intelligent Pass/Fail Logic:** Real-time monitoring of test thresholds capable of automatically terminating tests to protect target systems from critical overloads.
* **Normalized Artifact Generation:** The ability to convert proprietary outputs and logs from different testing engines into a standardized format for seamless ingestion and storage.

### 1.3 "Zero-Config" Test Plan Management
* **Intent-Based Configuration:** Users must be able to upload YAML configurations specifying only their testing intent (e.g., concurrency, duration, target database). Environment-specific endpoints and credentials must be silently injected at runtime.
* **Infrastructure Leasing:** A mechanism to securely lock shared databases (via a metadata store) and communicate with a control plane to dynamically provision or attach to testing environments.

### 1.4 Data-Testing-as-a-Service (No-Code Workloads)
* **Cloud-Native Asset Management:** The system must allow users to manage Test Plans, Fixture datasets, and Test Workloads (e.g., SQL queries) via a cloud-native API service.
* **Declarative SQL Testing:** Users must be able to test massive datasets using only standard SQL files, without writing any programming code (like Python or JavaScript).
* **High-Efficiency Execution Engine:** The underlying execution engine must be ultra-lightweight and highly concurrent to guarantee hardware efficiency and prevent the load generator from becoming the bottleneck.
* **Automated Query Parsing:** The system must automatically parse annotated SQL files to execute randomized analytical workloads and track granular, isolated latency metrics for each specific query.

### 1.5 Distributed Workload Scaling
* **Intra-Node Scaling (Vertical):** The system must support spawning multiple local load-generator processes to maximize CPU utilization and bypass concurrency limits on a single high-spec worker node.
* **Inter-Node Scaling (Horizontal):** For massive workloads, the system must orchestrate a distributed master/worker setup to synchronize load generation across multiple worker nodes in the fleet.

### 1.6 Massive Fixture Management & Ingestion
* **Direct-to-Storage Uploads:** The system must broker direct, multipart client uploads to cloud storage for massive (100GB+) datasets to completely bypass the API server's memory and worker disk bottlenecks.
* **Zero-Download Ingestion (MPP Databases):** For systems like Doris or Trino, the worker must pass storage URIs directly to the database, allowing the database cluster to pull the data natively.
* **Streaming Ingestion (Traditional Databases):** For systems like Cassandra, the worker must asynchronously pipe data rows directly from the storage stream into the database to maintain a near-zero local memory footprint.

### 1.7 Centralized Telemetry & Observability
* **Push-Based Metrics:** Ephemeral distributed workers must push real-time load metrics (TPS, latency, errors) to a centralized metrics gateway.
* **Global Aggregation:** Metrics must be labeled by unique run IDs, allowing graphical dashboards to aggregate and visualize data perfectly regardless of the horizontal scale of the test.

---

## 2. High-Level Architecture Design (V1.1)

### 2.1 Component List
* **Control Plane (API Service):** Python (FastAPI)
* **Job Queue (Message Broker):** RabbitMQ
* **Artifact Storage (Object Store):** S3 (or MinIO)
* **Metadata Store (Relational DB):** PostgreSQL
* **Execution Worker (Celery Node):** Python (Celery) + Docker
    * *Sub-component:* **Lease Manager**
    * *Sub-component:* **Fixture Loader**
    * *Sub-component:* **Process Manager (Python Subprocess)**
    * *Sub-component:* **Generic Workload Driver (k6)**
* **Telemetry Gateway:** Prometheus (Direct via remote-write)
* **System Under Test (SUT):** Doris, Trino, Cassandra, etc.

### 2.2 Component Roles & Responsibilities

#### A. Control Plane (API Service)
* **Role:** The entry point for all user interactions.
* **Responsibilities:**
    * **Test Plan Management:** Validates and stores user YAML test plans.
    * **Asset Brokering:** Generates Pre-Signed URLs for S3 Multipart Uploads for massive datasets (100GB+).
    * **Job Dispatch:** Parses the test plan to determine the scaling strategy and submits the appropriate task chain to the Job Queue.

#### B. Execution Worker (Celery Node)
* **Role:** The autonomous runner that executes the test lifecycle.
* **Responsibilities:**
    1.  **Lease Manager:**
        * Manages exclusive access to testing environments.
        * Acquires atomic locks on shared resources (Long-Lived) or provisions ephemeral clusters (Disposable).
    2.  **Fixture Loader:**
        * Automates hydration of the SUT with massive reference datasets.
        * **Zero-Download (MPP):** Issues SQL commands to pull Parquet files directly from S3.
        * **Streaming (NoSQL):** Streams CSV rows from S3 to Cassandra via async drivers (no local disk I/O).
    3.  **Process Manager (Native Subprocess):** 
        * Safely spawns, supervises, and terminates the k6 binary directly using Python's `subprocess` module, bypassing heavy wrappers.
        * Enforces execution order (`prepare` -> `startup` -> `execution` -> `shutdown`).
        * Catches cancellation signals, sending `SIGTERM` to allow k6 to flush final SQL queries to Prometheus, and escalates to `SIGKILL` if the process hangs.
    4.  **Generic Workload Driver (k6):**
        * Custom-compiled `xk6-sql` binary.
        * Parses uploaded SQL files on-the-fly and executes them using highly concurrent Goroutines.

---

## 3. Workload Driver Scaling Design (V1)

### 3.1 Scaling Modes
The system parses `plan.execution.scaling_mode` to determine the execution topology:

* Intra-Node (Vertical): Bypasses the distributed waiting room. A single Celery worker downloads the fixtures once, then loops to spawn multiple local k6 `subprocess` instances to maximize local CPU utilization.
* Inter-Node (Horizontal): Uses a Two-Phase Check-In architecture via PostgreSQL. Multiple Celery workers across the fleet download fixtures independently, enter a "Waiting Room," and wait for a global millisecond-exact `START` signal to prevent staggered execution.

### 3.2 The Dispatcher Task (The Orchestrator)
This task validates capacity and branches the execution logic based on the scaling mode.

```python
import time
from celery import shared_task
from celery.app.control import Inspect
from my_app.database import db_session
from my_app.tasks import k6_executor_task

@shared_task(bind=True)
def dispatcher_task(self, plan: dict, run_id: str):
    mode = plan["execution"].get("scaling_mode", "intra_node")
    cluster_size = plan["test_environment"]["cluster_spec"]["backend_node"].get("count", 1)

    # ---------------------------------------------------------
    # BRANCH A: Vertical Scaling (Single Node, Multi-Process)
    # ---------------------------------------------------------
    if mode == "intra_node":
        # Dispatch a single task instructing one worker to spawn N local processes
        k6_executor_task.delay(plan, run_id, segment_flag="100%", local_instances=cluster_size)
        return {"status": "intra_node_dispatched"}

    # ---------------------------------------------------------
    # BRANCH B: Horizontal Scaling (Multi-Node, Two-Phase Check-In)
    # ---------------------------------------------------------
    i = Inspect(app=self.app)
    active_workers = len(i.active_queues() or {})
    if active_workers < cluster_size:
        db_session.update_run_status(run_id, "FAILED_INSUFFICIENT_CAPACITY")
        return {"error": f"Requested {cluster_size} nodes, only {active_workers} available"}

    db_session.init_waiting_room(run_id, target_count=cluster_size)

    segment_size = 100 / cluster_size
    for idx in range(cluster_size):
        segment_start, segment_end = int(idx * segment_size), int((idx + 1) * segment_size)
        segment_flag = f"{segment_start}%:{segment_end}%"
        k6_executor_task.delay(plan, run_id, segment_flag, local_instances=1)

    # Monitor Waiting Room (5-minute timeout)
    timeout = time.time() + 300
    while time.time() < timeout:
        if db_session.get_ready_count(run_id) == cluster_size:
            db_session.set_start_signal(run_id, "START")
            return {"status": "inter_node_all_started"}
        time.sleep(2)

    db_session.set_start_signal(run_id, "ABORT")
    return {"error": "Worker check-in timeout exceeded"}
```

### 3.2 The k6 Executor Task (The Worker)
This task executes on the worker nodes. It handles fixture downloads and spawns the k6 OS process(es). If in distributed mode, it respects the PostgreSQL waiting room. 

```python
import time
import subprocess
import os
import signal
from celery import shared_task
from my_app.database import db_session

@shared_task(bind=True)
def k6_executor_task(self, plan: dict, run_id: str, segment_flag: str, local_instances: int):
    mode = plan["execution"].get("scaling_mode", "intra_node")

    # 1. Setup & Fixture Download (Heavy Lifting happens exactly once per node)
    download_sql_fixtures(plan["execution"]["workload"])

    env_vars = os.environ.copy()
    env_vars["K6_PROMETHEUS_RW_SERVER_URL"] = "http://prometheus:9090/api/v1/write"

    # 2. Horizontal Synchronization (Only if inter_node)
    if mode == "inter_node":
        db_session.increment_ready_worker(run_id)
        while True:
            signal_state = db_session.get_start_signal(run_id)
            if signal_state == "START":
                break
            elif signal_state == "ABORT":
                cleanup_fixtures()
                return {"status": "aborted_by_dispatcher"}
            time.sleep(0.5)

    # 3. Execution: Launch the k6 OS Process(es)
    processes = []
    # If intra_node, local_instances > 1. If inter_node, local_instances == 1.
    for i in range(local_instances):
        # Dynamically calculate sub-segments if running multiple local instances
        local_segment = calculate_local_segment(segment_flag, i, local_instances)

        env_vars["K6_PROMETHEUS_RW_INJECT_TAGS"] = f"run_id={run_id},segment={local_segment}"

        cmd = [
            "k6", "run", "/worker/drivers/generic_sql_driver.js",
            "--execution-segment", local_segment,
            "--out", "experimental-prometheus-rw",
            "--out", f"csv=/tmp/k6_raw_{run_id}_{i}.csv"
        ]
        processes.append(subprocess.Popen(cmd, env=env_vars))

    # 4. Wait & Teardown
    try:
        # Wait for all processes to finish holding for the specified duration
        for p in processes:
            p.wait(timeout=plan["execution"]["hold_for_seconds"]) 
    except subprocess.TimeoutExpired:
        for p in processes:
            p.send_signal(signal.SIGTERM)
        try:
            for p in processes:
                p.wait(timeout=10)
        except subprocess.TimeoutExpired:
            for p in processes:
                p.kill()

    # Upload all raw artifacts and clean disk
    for i in range(local_instances):
        upload_to_s3(f"/tmp/k6_raw_{run_id}_{i}.csv")
    cleanup_fixtures()

    return {"status": "completed", "mode": mode, "instances_run": local_instances}
```

## 4. Fixture Loader Design (V1)

### 4.1 Architectural Philosophy
To handle massive 100GB+ datasets, the architecture strictly avoids routing heavy data streams through the API server's memory or the Celery worker's local disk.
1. Ingestion: Client -> S3 (Multipart Uploads).
2. Zero-Download Ingestion: S3 -> MPP Database (Doris/Trino) directly.
3. Streaming Ingestion: S3 -> Stream -> Cassandra (via Worker RAM).

### 4.2 API Implementation (Multipart Upload)

```python
BUCKET = "project-crucible-storage"

@app.post("/fixtures/{fixture_id}/{file_name}/multipart/init")
async def init_multipart_upload(fixture_id: str, file_name: str):
    s3_key = f"fixtures/{fixture_id}/{file_name}"
    response = s3.create_multipart_upload(Bucket=BUCKET, Key=s3_key)
    return {"upload_id": response['UploadId'], "key": s3_key}

@app.get("/fixtures/{fixture_id}/{file_name}/multipart/{upload_id}/part/{part_number}")
async def get_presigned_part_url(fixture_id: str, file_name: str, upload_id: str, part_number: int):
    s3_key = f"fixtures/{fixture_id}/{file_name}"
    presigned_url = s3.generate_presigned_url(
        ClientMethod='upload_part',
        Params={'Bucket': BUCKET, 'Key': s3_key, 'UploadId': upload_id, 'PartNumber': part_number},
        ExpiresIn=3600
    )
    return {"presigned_url": presigned_url}
```

### 4.3 Loader Implementation (Strategy Pattern)

```python
class FixtureLoader:
    def load(self):
        # ... S3 client init ...
        if self.component in ["doris", "trino"]:
            self._load_zero_download(files)
        elif self.component == "cassandra":
            self._load_cassandra_streaming(files, s3)

    def _load_zero_download(self, files):
        # Uses S3 Table Value Function (TVF)
        sql = f"""
        INSERT INTO {table} SELECT * FROM S3("uri" = "s3://...", ...);
        """
        cur.execute(sql)

    def _load_cassandra_streaming(self, files, s3_client):
        # Streams bytes from S3, decodes to CSV lines, executes async inserts
        line_generator = (line.decode('utf-8') for line in s3_obj['Body'].iter_lines())
        execute_concurrent_with_args(session, prepared_stmt, csv.reader(line_generator), concurrency=150)
```

## 5. Workload Driver Design Choice (k6 vs. Locust)

### 5.1 Verdict
k6 (Go) is chosen over Locust (Python) for the Generic SQL Driver because of its:

* Goroutine-based Concurrency: Can simulate tens of thousands of VUs with minimal memory.
* Hardware Efficiency: Prevents the load generator from becoming the bottleneck.
* Caveat: Requires a custom binary built with `xk6-sql`.

### 5.2 Generic SQL Driver Implementation (`generic_sql_driver.js`)
This script reads the downloaded SQL file, parses annotations, and executes queries.

```javascript
import sql from 'k6/x/sql';
import { check, fail } from 'k6';
import { Trend } from 'k6/metrics';

// Environment variables injected by Celery Worker
const dbHost = __ENV.DB_HOST;
const sqlFilePath = __ENV.DOWNLOADED_SQL_PATH;

const rawSqlText = open(sqlFilePath);
const queries = parseAnnotatedSql(rawSqlText);

const queryTrends = {};
queries.forEach(q => queryTrends[q.name] = new Trend(`sql_duration_${q.name}`));

const db = sql.open('mysql', `${__ENV.DB_USER}:${__ENV.DB_PASS}@tcp(${dbHost}:${__ENV.DB_PORT})/${__ENV.DB_NAME}`);

export default function () {
    const query = queries[Math.floor(Math.random() * queries.length)];
    const startTime = Date.now();
    try {
        const results = sql.query(db, query.text);
        queryTrends[query.name].add(Date.now() - startTime);
        check(results, {[`${query.name} executed`]: (r) => r !== null});
    } catch (e) {
        // Error handling
    }
}

function parseAnnotatedSql(sqlText) {
    // Logic to split file by "-- @name: QueryName" comments
    // ...
}
```
### 5.3 Direct k6 Execution & Telemetry
By bypassing Taurus and running k6 directly via the OS, we ensure that the custom SQL metrics generated by the script (`sql_duration_QueryName`) are perfectly preserved.
* Real-time Metrics: The worker executes k6 with the `--out experimental-prometheus-rw` flag, streaming the custom `Trend` metrics directly into Prometheus.
* Artifact Archival: The worker also uses `--out csv=/tmp/k6_raw.csv` to generate a raw backup of every data point, which the Celery worker uploads directly to the S3 artifact bucket during the teardown phase.

### 5.4 Workload File Format

Workload files use a plain-text annotation format that is query-language agnostic (SQL, CQL, etc.). The format is validated by `crucible-lib` — both the control plane (on upload via `POST /v1/workloads`) and the worker (after download, before execution) call `validate_workload()` from the shared library.

**Header** — the first non-blank line must declare the workload type:

```
-- @type: sql
```

Supported types: `sql`, `cql`. The type value must be non-empty.

**Query blocks** — each query is preceded by a `-- @name:` annotation. Query names must match `^[A-Za-z_][A-Za-z0-9_]*$`, be unique within the file, and have a non-empty body.

**Example (SQL):**

```sql
-- @type: sql

-- @name: TopSellingProducts
SELECT product_id, SUM(revenue) FROM orders GROUP BY 1 ORDER BY 2 DESC LIMIT 10;

-- @name: DailyActiveUsers
SELECT DATE(created_at), COUNT(DISTINCT user_id) FROM events GROUP BY 1;
```

**Example (CQL):**

```
-- @type: cql

-- @name: GetUserProfile
SELECT * FROM users WHERE user_id = ?;

-- @name: GetRecentOrders
SELECT * FROM orders WHERE user_id = ? ORDER BY created_at DESC LIMIT 20;
```

**Validation rules (enforced in `crucible_lib.schemas.workload`):**

| Rule | Detail |
| :--- | :--- |
| `-- @type:` header required | Must be the first non-blank line. |
| Type value non-empty | The value after `@type:` must not be blank. |
| At least one `-- @name:` block | Files with no named queries are rejected. |
| Unique query names | Duplicate names within a file are an error. |
| Valid name format | Names must match `^[A-Za-z_][A-Za-z0-9_]*$`. |
| Non-empty query bodies | Each named block must contain at least one non-whitespace line. |

Workload files are uploaded via **`POST /v1/workloads`** and stored in S3 at `workloads/{workload_id}` (no file extension). The workload type is carried inside the file via the `-- @type:` header rather than the S3 key, so users reference workloads in the test plan using only the `workload_id`.

## 6. Test Plan YAML Specification (V1 Locked)

### 6.1 Schema Philosophy
The Test Plan schema strictly separates user-managed environments (`cluster_info`) from system-provisioned ones (`cluster_spec`). It also enforces explicit mapping between uploaded fixture artifacts and target database tables.

### 6.2 Data Dictionary

| Field Path | Type | Required | Description |
| :--- | :--- | :---: | :--- |
| **`test_metadata`** | Object | Yes | **Root block for test identification.** |
| `test_metadata.run_label` | String | Yes | Human-readable ID for the test run (e.g., "Local_Doris_Test"). |
| **`test_environment`** | Object | Yes | **Root block for database targeting.** |
| `test_environment.env_type` | String | Yes | Enum: `long-lived` (shared) or `disposable` (ephemeral). |
| `test_environment.component_spec` | Object | Yes | Defines the target engine topology. |
| `component_spec.type` | String | Yes | Target engine (e.g., `doris`, `trino`). |
| `component_spec.cluster_info` | Object | Conditional | **Bring-Your-Own:** Connection details (e.g., `host: localhost:9060`). Mutually exclusive with `cluster_spec`. |
| `component_spec.cluster_spec` | Object | Conditional | **Provision-For-Me:** Specs for fresh cluster (e.g., `version`, `frontend_node.count`). Mutually exclusive with `cluster_info`. |
| `test_environment.target_db` | String | Yes | Target database/keyspace name. |
| `test_environment.fixtures` | Array | No | List of data artifacts to load. |
| `fixtures[].fixture_id` | String | Yes | S3 object prefix for the dataset. |
| `fixtures[].table` | String | Yes | Explicit destination table name. |
| **`execution`** | Object | Yes | **Root block for load generation.** |
| `execution.executor` | String | Yes | Engine: `k6` or `locust`. |
| `execution.scaling_mode` | String | Yes | Enum: `intra_node` (vertical) or `inter_node` (horizontal). |
| `execution.concurrency` | Integer | Yes | Number of VUs. |
| `execution.ramp_up` | String | Yes | Duration (e.g., `30s`). |
| `execution.hold_for` | String | Yes | Duration (e.g., `2m`). |
| `execution.workload` | Array | Yes | List of workload references. |
| `workload[].workload_id` | String | Yes | Identifier of the uploaded workload file (see §5.4). Stored in S3 at `workloads/{workload_id}`. |

### 6.3 Valid YAML Example
```yaml
test_metadata:
  run_label: "Local_Doris_Test"

test_environment:
  env_type: long-lived
  target_db: benchmark_db
  component_spec:
    type: doris
    cluster_info:
      host: localhost:9060
  fixtures:
  - fixture_id: tpch-sf001-customer
    table: customer
  - fixture_id: tpch-sf001-lineitem
    table: lineitem

execution:
  executor: k6
  scaling_mode: intra_node
  concurrency: 5
  ramp_up: 30s
  hold_for: 2m
  workload:
  - workload_id: tpch-test-queries
```
