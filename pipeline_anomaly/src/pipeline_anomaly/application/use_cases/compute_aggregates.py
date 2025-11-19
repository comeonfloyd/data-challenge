from __future__ import annotations

from datetime import datetime

import pandas as pd

from pipeline_anomaly.domain.models.aggregate import Aggregate, AggregateCollection
from pipeline_anomaly.domain.services.interfaces import ClickHouseWriter
from pipeline_anomaly.infrastructure.config import AggregationConfig, AggregationMetricConfig


class ComputeAggregates:
    DEFAULT_METRICS: tuple[AggregationMetricConfig, ...] = (
        AggregationMetricConfig(name="count", type="count"),
        AggregationMetricConfig(name="mean_value", type="mean", column="value"),
        AggregationMetricConfig(name="std_value", type="std", column="value"),
    )

    def __init__(self, writer: ClickHouseWriter, config: AggregationConfig) -> None:
        self._writer = writer
        self._config = config

    def execute(self) -> AggregateCollection:
        dataframe = self._writer.read_latest_window(minutes=self._config.window_minutes)
        window_start = dataframe["event_time"].min()
        window_end = dataframe["event_time"].max()

        aggregates = [
            self._materialize(metric_cfg, dataframe, window_start, window_end)
            for metric_cfg in (self._config.metrics or self.DEFAULT_METRICS)
        ]

        collection = AggregateCollection(aggregates=tuple(aggregates))
        self._writer.persist_aggregates(collection)
        return collection

    def _materialize(
        self,
        cfg: AggregationMetricConfig,
        dataframe: pd.DataFrame,
        window_start: datetime,
        window_end: datetime,
    ) -> Aggregate:
        column = cfg.column or "value"
        if column not in dataframe.columns:
            raise KeyError(f"column {column} missing in dataframe")
        series = dataframe[column]
        metric_type = cfg.type.lower()
        if metric_type == "count":
            value = float(len(series))
            extra = None
        elif metric_type == "mean":
            value = float(series.mean())
            extra = None
        elif metric_type == "std":
            value = float(series.std() or 0.0)
            extra = None
        elif metric_type == "median":
            value = float(series.median())
            extra = None
        elif metric_type == "min":
            value = float(series.min())
            extra = None
        elif metric_type == "max":
            value = float(series.max())
            extra = None
        elif metric_type == "quantile":
            q = cfg.q or 0.5
            value = float(series.quantile(q))
            extra = {"quantile": q}
        elif metric_type == "iqr":
            q75 = float(series.quantile(0.75))
            q25 = float(series.quantile(0.25))
            value = q75 - q25
            extra = {"q75": q75, "q25": q25}
        elif metric_type == "ewma_trend":
            span = cfg.span or 12
            ewma = series.ewm(span=span, adjust=False).mean()
            last_value = float(series.iloc[-1])
            trend = last_value - float(ewma.iloc[-1])
            value = trend
            extra = {"ewma": float(ewma.iloc[-1]), "last_value": last_value, "span": float(span)}
        else:
            raise ValueError(f"unknown metric type {cfg.type}")

        return Aggregate(metric=cfg.name, value=value, window_start=window_start, window_end=window_end, extra=extra)
