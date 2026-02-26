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
    * *Sub-component:* **Taurus Orchestrator**
    * *Sub-component:* **Generic Workload Driver (k6)**
* **Telemetry Gateway:** Prometheus Pushgateway
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
    3.  **Taurus Orchestrator:**
        * Manages the process tree of the underlying driver.
        * Enforces lifecycle hooks: `prepare` -> `startup` -> `execution` -> `shutdown`.
        * Monitors worker health (CPU/RAM) and normalizes artifacts.
    4.  **Generic Workload Driver (k6):**
        * Custom-compiled `xk6-sql` binary.
        * Parses uploaded SQL files on-the-fly and executes them using highly concurrent Goroutines.

---

## 3. Workload Driver Scaling Design (V1)

### 3.1 Distributed Execution Flow
The API Service determines the execution strategy by parsing the user's YAML:
* **Intra-Node (Vertical):** A single worker manages multiple local engine instances to maximize CPU utilization.
* **Inter-Node (Horizontal):** A Celery Chain triggers a "Master" task, retrieves its network identity, and spawns multiple "Worker" tasks across the fleet.

### 3.2 Smart Trigger API Implementation
The `/test-runs/{filename}` API dynamically determines the Celery workflow.

```python
BUCKET = "project-crucible-storage"

@app.post("/test-runs/{filename}")
async def trigger_test_run(filename: str):
    plan_content = s3.get_object(Bucket=BUCKET, Key=filename)['Body'].read()
    plan = yaml.safe_load(plan_content)
    mode = plan.get("test_environment", {}).get("scaling_mode", "intra_node")
    
    if mode == "inter_node":
        size = plan["test_environment"].get("cluster_size", 2)
        workflow = chain(
            run_master_task.s(plan, mode="inter_node"), 
            run_worker_task.s(plan, count=size-1)
        )
        result = workflow.delay()
        return {"run_id": result.id, "strategy": "distributed_horizontal"}

    task = run_master_task.delay(plan, mode="intra_node")
    return {"run_id": task.id, "strategy": "distributed_vertical"}
```

### 3.2 Celery Worker Implementation
Workers use Taurus overrides to inject master/worker flags.

```python
@celery_app.task(bind=True)
def run_master_task(self, plan, mode):
    overrides = {"execution": plan["execution"]}
    if mode == "intra_node":
        overrides["execution"][0].update({
            "master": True,
            "workers": plan["test_environment"].get("worker_count", 1)
        })
    elif mode == "inter_node":
        overrides["execution"][0].update({
            "master": True,
            "expect-workers": plan["test_environment"].get("cluster_size", 1) - 1
        })
    execute_taurus(plan, overrides)
    return {"master_ip": os.getenv("RUNNER_IP")}
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
