from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class TestMetadata(BaseModel):
    run_label: str


class ClusterInfo(BaseModel):
    """Bring-Your-Own connection details for a pre-existing cluster."""

    model_config = ConfigDict(extra="allow")

    host: str
    username: str
    password: str


class ComponentSpec(BaseModel):
    type: str
    cluster_info: ClusterInfo | None = None


class FixtureItem(BaseModel):
    fixture_id: str
    table: str


class PrometheusMetric(BaseModel):
    name: str
    query: str


class PrometheusTLS(BaseModel):
    """TLS settings for reaching a Prometheus-compatible endpoint.

    Present so a per-run endpoint issued by a private CA (e.g. a cert-manager
    CA created alongside a disposable VictoriaMetrics instance) can be verified
    without adding that CA to the worker image's system trust store.
    """

    # Forbid extras: a misspelled key here would otherwise be dropped silently
    # and the query would fall back to the system trust store, failing later
    # with a bare "certificate verify failed" that points nowhere near the typo.
    model_config = ConfigDict(extra="forbid")

    ca_bundle_pem: str | None = Field(
        default=None,
        description=(
            "PEM-encoded CA bundle used to verify the endpoint's certificate. "
            "Hostname verification stays on, so the certificate must carry a "
            "SAN matching the host in 'url'. This REPLACES the system trust "
            "store for this source rather than adding to it, so the bundle must "
            "contain every root needed to verify the endpoint's chain. When "
            "unset, the worker's system trust store is used."
        ),
    )


class PrometheusSource(BaseModel):
    name: str
    url: str
    metrics: list[PrometheusMetric] = Field(..., min_length=1)
    resolution: int = Field(default=15, gt=0, description="Minimum step in seconds")
    max_data_points: int = Field(default=500, gt=0, description="Max data points per metric")
    tls: PrometheusTLS | None = None

    @model_validator(mode="after")
    def _ca_bundle_requires_https(self) -> "PrometheusSource":
        """A CA on a plain-HTTP source would be silently ignored.

        requests only consults `verify` for TLS connections, so the query would
        go out in cleartext while the plan says it is verified.  Fail loudly at
        upload time instead.
        """
        if (
            self.tls
            and self.tls.ca_bundle_pem
            and not self.url.lower().startswith("https://")
        ):
            raise ValueError(
                f"tls.ca_bundle_pem is set but url is not https:// (got {self.url!r}); "
                "the CA would be ignored and the query sent in cleartext."
            )
        return self


class Observability(BaseModel):
    prometheus_sources: list[PrometheusSource] = Field(default_factory=list)


class TestEnvironment(BaseModel):
    env_type: Literal["long-lived", "disposable"]
    component_spec: ComponentSpec
    target_db: str
    fixtures: list[FixtureItem] = Field(default_factory=list)
    observability: Observability | None = None

    @model_validator(mode="after")
    def _cluster_info_required_for_long_lived(self) -> "TestEnvironment":
        if self.env_type == "long-lived" and self.component_spec.cluster_info is None:
            raise ValueError(
                "'cluster_info' is required when env_type is 'long-lived'."
            )
        return self


class WorkloadItem(BaseModel):
    workload_id: str


class FailureDetection(BaseModel):
    """SUT failure detection — abort the test early when sustained query errors
    indicate the target database is unresponsive.

    When enabled, the k6 driver tracks a ``query_errors`` Rate metric.  If the
    error rate exceeds ``error_rate_threshold`` after the initial
    ``abort_delay`` window, k6 aborts gracefully and the run is marked
    COMPLETED with whatever partial results were collected.
    """

    enabled: bool = Field(
        default=True,
        description="Set false to disable SUT failure detection entirely.",
    )
    error_rate_threshold: float = Field(
        default=0.5,
        gt=0.0,
        le=1.0,
        description="Maximum allowed query error rate before aborting (0.0–1.0).",
    )
    abort_delay: str = Field(
        default="10s",
        description="Grace period before evaluating the threshold (k6 duration string, e.g. '10s', '30s').",
    )


class ChaosTarget(BaseModel):
    """Routing target for a chaos experiment — K8s pods or EC2/bare-metal host."""

    env_type: Literal["k8s", "ec2"]
    namespace: str | None = None
    selector: dict[str, str] | None = None
    address: str | None = None

    @model_validator(mode="after")
    def _validate_target_fields(self) -> "ChaosTarget":
        if self.env_type == "k8s":
            if not self.namespace or not self.selector:
                raise ValueError(
                    "'namespace' and 'selector' are required when env_type is 'k8s'."
                )
        elif self.env_type == "ec2":
            if not self.address:
                raise ValueError(
                    "'address' is required when env_type is 'ec2'."
                )
        return self


class ChaosSchedule(BaseModel):
    """Temporal controls for a chaos experiment, aligned with k6 hold_for."""

    start_after: str = Field(
        description="Delay before injection, allowing baseline metrics to form (k6 duration string).",
    )
    duration: str = Field(
        description="How long the fault persists before automated recovery (k6 duration string).",
    )


class ChaosExperiment(BaseModel):
    """A single discrete fault event within a chaos spec."""

    name: str
    fault_type: str = Field(
        description="Chaos Mesh resource kind (e.g. 'networkchaos', 'podchaos', 'stresschaos', 'iochaos').",
    )
    target: ChaosTarget
    parameters: dict = Field(default_factory=dict)
    schedule: ChaosSchedule


class ChaosSpec(BaseModel):
    """Root block for fault injection in a test plan."""

    engine: Literal["chaos-mesh"] = Field(
        description="Chaos engine to use. Currently only 'chaos-mesh' is supported.",
    )
    experiments: list[ChaosExperiment] = Field(..., min_length=1)


class ExecutionConfig(BaseModel):
    executor: Literal["k6", "locust"]
    scaling_mode: Literal["intra_node", "inter_node"]
    concurrency: int = Field(..., gt=0)
    ramp_up: str
    hold_for: str
    workload: list[WorkloadItem]
    failure_detection: FailureDetection | None = None


class TestPlan(BaseModel):
    test_metadata: TestMetadata
    test_environment: TestEnvironment
    execution: ExecutionConfig
    chaos_spec: ChaosSpec | None = None
