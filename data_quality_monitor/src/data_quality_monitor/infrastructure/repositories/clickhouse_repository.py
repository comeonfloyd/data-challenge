from __future__ import annotations

import pandas as pd

from data_quality_monitor.domain.models.result import QualityReport
from data_quality_monitor.infrastructure.clients.clickhouse import ClickHouseFactory


class ClickHouseRepository:
    def __init__(self, factory: ClickHouseFactory) -> None:
        self._factory = factory

    def ensure_schema(self) -> None:
        ddl_statements = [
            """
            CREATE TABLE IF NOT EXISTS dq_reports (
                table String,
                generated_at DateTime,
                rule String,
                passed UInt8,
                details JSON
            ) ENGINE = MergeTree ORDER BY (generated_at, table)
            """
        ]
        with self._factory.connect() as client:
            for ddl in ddl_statements:
                client.command(ddl)

    def fetch_table(self, table: str) -> pd.DataFrame:
        with self._factory.connect() as client:
            return client.query_df(f"SELECT * FROM {table}")

    def save_report(self, report: QualityReport) -> None:
        rows = report.as_rows()
        with self._factory.connect() as client:
            client.insert_dicts("dq_reports", rows)

    def list_reports(self) -> pd.DataFrame:
        with self._factory.connect() as client:
            return client.query_df("SELECT * FROM dq_reports ORDER BY generated_at DESC LIMIT 1000")
