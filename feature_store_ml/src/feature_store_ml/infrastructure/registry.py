from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from feature_store_ml.infrastructure.config import FeatureConfig


@dataclass(slots=True)
class FeatureRegistry:
    """Построитель агрегированных фичей по тайм-слотам."""

    bucket_minutes: int

    @classmethod
    def from_config(cls, config: FeatureConfig) -> "FeatureRegistry":
        return cls(bucket_minutes=config.bucket_minutes)

    @property
    def feature_names(self) -> tuple[str, ...]:
        return (
            "value_mean",
            "value_std",
            "value_count",
            "value_p05",
            "value_p95",
            "value_iqr",
            "attribute_mean",
            "value_trend",
            "value_volatility",
        )

    def compute(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        if dataframe.empty:
            columns = ["entity_id", "event_time", "label", *self.feature_names]
            return pd.DataFrame(columns=columns)

        frame = dataframe.copy()
        frame["bucket"] = frame["event_time"].dt.floor(f"{self.bucket_minutes}min")
        grouped = frame.groupby(["entity_id", "bucket"], sort=True)

        stats = grouped["value"].agg(
            value_mean="mean",
            value_std="std",
            value_count="count",
        )
        stats["value_std"] = stats["value_std"].fillna(0.0)

        quantiles = grouped["value"].quantile([0.05, 0.95]).unstack()
        quantiles.columns = ["value_p05", "value_p95"]
        stats = stats.join(quantiles)
        stats["value_iqr"] = stats["value_p95"] - stats["value_p05"]

        attribute = grouped["attribute"].agg(attribute_mean="mean")
        stats = stats.join(attribute)

        stats = stats.reset_index().rename(columns={"bucket": "event_time"})
        stats = stats.sort_values(["entity_id", "event_time"]).reset_index(drop=True)

        stats["value_trend"] = stats.groupby("entity_id")["value_mean"].diff().fillna(0.0)
        stats["value_volatility"] = (
            stats.groupby("entity_id")["value_mean"]
            .rolling(window=3, min_periods=1)
            .std()
            .reset_index(level=0, drop=True)
            .fillna(0.0)
        )

        target = grouped["label"].max().reset_index().rename(columns={"bucket": "event_time", "label": "label"})
        stats = stats.merge(target, on=["entity_id", "event_time"], how="left")
        stats["label"] = stats["label"].fillna(0).astype(int)

        return stats[["entity_id", "event_time", "label", *self.feature_names]]
