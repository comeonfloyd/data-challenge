from __future__ import annotations

import pandas as pd

from pipeline_anomaly.infrastructure.detectors.base import PandasDetector


class MedianAbsoluteDeviationDetector(PandasDetector):
    """Robust anomaly detector based on the modified Z-score."""

    def __init__(self, threshold: float) -> None:
        super().__init__(name="mad")
        self._threshold = threshold

    def fit_predict(self, dataframe: pd.DataFrame) -> pd.Series:
        series = dataframe["value"]
        median = series.median()
        deviation = (series - median).abs()
        mad = deviation.median()
        if mad == 0:
            # fall back to standard deviation to avoid division by zero
            mad = float(series.std() or 1.0)
        modified_z = 0.6745 * deviation / mad
        return (modified_z > self._threshold).astype(int)
