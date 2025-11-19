from __future__ import annotations

from datetime import datetime

import pandas as pd

from pipeline_anomaly.domain.models.aggregate import AggregateCollection
from pipeline_anomaly.domain.models.anomaly import AnomalyReport
from pipeline_anomaly.domain.models.batch import RecordBatch
from pipeline_anomaly.infrastructure.clients.clickhouse import ClickHouseFactory


class ClickHouseRepository:
    def __init__(self, factory: ClickHouseFactory) -> None:
        self._factory = factory

    def ensure_schema(self) -> None:
        ddl_statements = [
            """
            CREATE TABLE IF NOT EXISTS events (
                event_time DateTime,
                entity_id UInt64,
                value Float64,
                attribute Float64
            ) ENGINE = MergeTree ORDER BY (event_time, entity_id)
            """,
            """
            CREATE TABLE IF NOT EXISTS aggregates (
                metric String,
                value Float64,
                window_start DateTime,
                window_end DateTime,
                extra Map(String, Float64)
            ) ENGINE = MergeTree ORDER BY (window_start, metric)
            """,
            """
            CREATE TABLE IF NOT EXISTS anomaly_reports (
                generated_at DateTime,
                window_start DateTime,
                window_end DateTime,
                detector String,
                score Float64,
                severity Float64,
                description String
            ) ENGINE = MergeTree ORDER BY (generated_at, detector)
            """,
        ]
        with self._factory.connect() as client:
            for ddl in ddl_statements:
                client.command(ddl)

    def ingest_batch(self, batch: RecordBatch) -> None:
        with self._factory.connect() as client:
            client.insert_dataframe("events", batch.dataframe)

    def persist_aggregates(self, aggregates: AggregateCollection) -> None:
        payload = aggregates.as_dict()
        with self._factory.connect() as client:
            client.insert_dicts("aggregates", payload)

    def persist_report(self, report: AnomalyReport) -> None:
        rows = [
            {
                "generated_at": report.generated_at,
                "window_start": report.window_start,
                "window_end": report.window_end,
                "detector": anomaly.detector,
                "score": anomaly.score,
                "severity": anomaly.severity,
                "description": anomaly.description,
            }
            for anomaly in report.anomalies
        ]
        with self._factory.connect() as client:
            client.insert_dicts("anomaly_reports", rows)

    def read_latest_window(self, minutes: int | None = None) -> pd.DataFrame:
        with self._factory.connect() as client:
            interval_minutes = minutes or 60 * 24
            query = f"""
            SELECT * FROM events
            WHERE event_time >= now() - INTERVAL {interval_minutes} MINUTE
            ORDER BY event_time
            """
            result = client.query_df(query)
        if result.empty:
            raise RuntimeError("events table is empty")
        return result
