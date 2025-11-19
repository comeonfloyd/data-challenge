from __future__ import annotations

import pandas as pd

from feature_store_ml.infrastructure.clients.clickhouse import ClickHouseFactory


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
                attribute Float64,
                label UInt8
            ) ENGINE = MergeTree ORDER BY (event_time, entity_id)
            """,
            """
            CREATE TABLE IF NOT EXISTS entity_features (
                event_time DateTime,
                entity_id UInt64,
                label UInt8,
                feature_map Map(String, Float64)
            ) ENGINE = MergeTree ORDER BY (event_time, entity_id)
            """,
            """
            CREATE TABLE IF NOT EXISTS predictions (
                event_time DateTime,
                entity_id UInt64,
                score Float64
            ) ENGINE = MergeTree ORDER BY (event_time, entity_id)
            """,
        ]
        with self._factory.connect() as client:
            for ddl in ddl_statements:
                client.command(ddl)

    def fetch_events(self, lookback_hours: int) -> pd.DataFrame:
        cutoff = lookback_hours or 24
        query = f"""
        SELECT * FROM events
        WHERE event_time >= now() - INTERVAL {cutoff} HOUR
        ORDER BY event_time
        """
        with self._factory.connect() as client:
            frame = client.query_df(query)
        return frame

    def persist_feature_view(self, features: pd.DataFrame) -> None:
        if features.empty:
            return
        rows = []
        for _, row in features.iterrows():
            feature_map = {
                column: float(row[column])
                for column in features.columns
                if column not in {"entity_id", "event_time", "label"}
            }
            rows.append(
                {
                    "entity_id": int(row["entity_id"]),
                    "event_time": row["event_time"],
                    "label": int(row.get("label", 0)),
                    "feature_map": feature_map,
                }
            )
        with self._factory.connect() as client:
            client.insert_dicts("entity_features", rows)

    def load_feature_view(self, lookback_hours: int) -> pd.DataFrame:
        query = f"""
        SELECT entity_id, event_time, label, feature_map
        FROM entity_features
        WHERE event_time >= now() - INTERVAL {lookback_hours} HOUR
        ORDER BY event_time
        """
        with self._factory.connect() as client:
            frame = client.query_df(query)
        return self._expand_feature_map(frame)

    def fetch_inference_candidates(self, batch_size: int) -> pd.DataFrame:
        query = f"""
        SELECT entity_id, event_time, label, feature_map
        FROM entity_features
        ORDER BY event_time DESC
        LIMIT {batch_size}
        """
        with self._factory.connect() as client:
            frame = client.query_df(query)
        return self._expand_feature_map(frame)

    def persist_predictions(self, payload: pd.DataFrame) -> None:
        if payload.empty:
            return
        rows = payload.to_dict(orient="records")
        with self._factory.connect() as client:
            client.insert_dicts("predictions", rows)

    def list_reports(self) -> pd.DataFrame:
        with self._factory.connect() as client:
            return client.query_df("SELECT * FROM predictions ORDER BY event_time DESC LIMIT 1000")

    @staticmethod
    def _expand_feature_map(frame: pd.DataFrame) -> pd.DataFrame:
        if frame.empty:
            return frame
        feature_map = frame.pop("feature_map")
        features = pd.DataFrame(feature_map.tolist()).fillna(0.0)
        return pd.concat([frame.reset_index(drop=True), features], axis=1)
