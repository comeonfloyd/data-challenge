from __future__ import annotations

import pandas as pd

from pipeline_anomaly.infrastructure.detectors.base import PandasDetector


class RollingMADDetector(PandasDetector):
    """Robust detector based on rolling median absolute deviation."""

    def __init__(self, window: int, threshold: float, min_periods: int | None = None) -> None:
        super().__init__(name="rolling_mad")
        self._window = window
        self._threshold = threshold
        self._min_periods = min_periods or max(1, window // 2)

    def fit_predict(self, dataframe: pd.DataFrame) -> pd.Series:
        ordered = dataframe.sort_values("event_time")
        series = ordered["value"].reset_index(drop=True)
        rolling_median = series.rolling(window=self._window, min_periods=self._min_periods).median()
        deviation = (series - rolling_median).abs()
        mad = deviation.rolling(window=self._window, min_periods=self._min_periods).median()
        mad = mad.bfill().ffill().fillna(deviation.median())
        mad = mad.clip(lower=1e-9)
        robust_z = 0.6745 * deviation / mad
        predictions = (robust_z > self._threshold).astype(int)
        predictions.index = ordered.index
        return predictions.sort_index()

