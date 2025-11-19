from __future__ import annotations

import pandas as pd

from pipeline_anomaly.infrastructure.detectors.rolling_mad import RollingMADDetector


def test_rolling_mad_flags_single_spike() -> None:
    timestamps = pd.date_range("2024-01-01", periods=100, freq="s")
    values = [100.0] * 100
    values[50] = 1000.0
    dataframe = pd.DataFrame({"event_time": timestamps, "value": values})

    detector = RollingMADDetector(window=20, threshold=4.0, min_periods=10)
    result = detector.fit_predict(dataframe)

    assert result.loc[50] == 1
    assert result.sum() == 1
