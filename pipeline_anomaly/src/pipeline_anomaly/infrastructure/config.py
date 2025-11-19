from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml

from pipeline_anomaly.infrastructure.generators.synthetic_generator import SyntheticDatasetConfig


@dataclass(slots=True)
class ClickHouseConfig:
    host: str
    port: int
    username: str
    password: str
    database: str


@dataclass(slots=True)
class IsolationForestConfig:
    contamination: float
    random_state: int


@dataclass(slots=True)
class DBSCANConfig:
    eps: float
    min_samples: int


@dataclass(slots=True)
class AnomalyDetectionConfig:
    window_minutes: int
    zscore_threshold: float
    isolation_forest: IsolationForestConfig
    dbscan: DBSCANConfig
    mad_threshold: float


@dataclass(slots=True)
class AlertingConfig:
    enabled: bool
    threshold_score: float
    sink: str


@dataclass(slots=True)
class AggregationMetricConfig:
    name: str
    type: str
    column: str | None = None
    q: float | None = None
    span: int | None = None


@dataclass(slots=True)
class AggregationConfig:
    window_minutes: int
    metrics: tuple[AggregationMetricConfig, ...]


@dataclass(slots=True)
class PipelineConfig:
    clickhouse: ClickHouseConfig
    dataset: SyntheticDatasetConfig
    aggregation: AggregationConfig
    anomaly_detection: AnomalyDetectionConfig
    alerting: AlertingConfig

    @classmethod
    def load(cls, path: Path) -> "PipelineConfig":
        with path.open("r", encoding="utf-8") as file:
            raw = yaml.safe_load(file)
        aggregation_raw = raw.get("aggregation", {})
        metrics_cfg = tuple(
            AggregationMetricConfig(**metric)
            for metric in aggregation_raw.get("metrics", [])
        )
        return cls(
            clickhouse=ClickHouseConfig(**raw["clickhouse"]),
            dataset=SyntheticDatasetConfig(**raw["dataset"]),
            aggregation=AggregationConfig(
                window_minutes=int(aggregation_raw.get("window_minutes", 60)),
                metrics=metrics_cfg,
            ),
            anomaly_detection=AnomalyDetectionConfig(
                window_minutes=int(raw["anomaly_detection"].get("window_minutes", 60)),
                zscore_threshold=float(raw["anomaly_detection"]["zscore_threshold"]),
                isolation_forest=IsolationForestConfig(**raw["anomaly_detection"]["isolation_forest"]),
                dbscan=DBSCANConfig(**raw["anomaly_detection"]["dbscan"]),
                mad_threshold=float(raw["anomaly_detection"].get("mad", {}).get("threshold", 6.0)),
            ),
            alerting=AlertingConfig(**raw["alerting"]),
        )
