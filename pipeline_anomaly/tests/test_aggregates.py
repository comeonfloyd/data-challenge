import pandas as pd
from unittest.mock import Mock

from pipeline_anomaly.application.use_cases.compute_aggregates import ComputeAggregates
from pipeline_anomaly.infrastructure.config import AggregationConfig, AggregationMetricConfig


def test_compute_aggregates_supports_custom_metrics():
    dataframe = pd.DataFrame(
        {
            "event_time": pd.date_range("2024-01-01", periods=5, freq="h"),
            "value": [1.0, 2.0, 3.0, 10.0, 5.0],
        }
    )
    writer = Mock()
    writer.read_latest_window.return_value = dataframe

    config = AggregationConfig(
        window_minutes=120,
        metrics=(
            AggregationMetricConfig(name="iqr_value", type="iqr", column="value"),
            AggregationMetricConfig(name="ewma_trend", type="ewma_trend", column="value", span=3),
        ),
    )

    aggregator = ComputeAggregates(writer=writer, config=config)
    collection = aggregator.execute()

    assert len(collection.aggregates) == 2
    assert {aggregate.metric for aggregate in collection.aggregates} == {"iqr_value", "ewma_trend"}
    writer.read_latest_window.assert_called_with(minutes=120)
