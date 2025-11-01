from __future__ import annotations

from datetime import datetime

import pandas as pd

from pipeline_anomaly.domain.models.aggregate import Aggregate, AggregateCollection
from pipeline_anomaly.domain.services.interfaces import ClickHouseWriter


class ComputeAggregates:
    def __init__(self, writer: ClickHouseWriter) -> None:
        self._writer = writer

    def execute(self) -> AggregateCollection:
        dataframe = self._writer.read_latest_window()
        window_start = dataframe["event_time"].min()
        window_end = dataframe["event_time"].max()

        aggregates = AggregateCollection(
            aggregates=(
                Aggregate("count", float(len(dataframe)), window_start, window_end),
                Aggregate("mean_value", float(dataframe["value"].mean()), window_start, window_end),
                Aggregate("std_value", float(dataframe["value"].std()), window_start, window_end),
            )
        )

        self._writer.persist_aggregates(aggregates)
        return aggregates
