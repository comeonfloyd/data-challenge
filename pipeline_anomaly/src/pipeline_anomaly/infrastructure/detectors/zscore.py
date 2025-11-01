from __future__ import annotations

import pandas as pd

from pipeline_anomaly.infrastructure.detectors.base import PandasDetector


class ZScoreDetector(PandasDetector):
    def __init__(self, threshold: float) -> None:
        super().__init__(name="zscore")
        self._threshold = threshold

    def fit_predict(self, dataframe: pd.DataFrame) -> pd.Series:
        mean = dataframe["value"].mean()
        std = dataframe["value"].std()
        z_scores = (dataframe["value"] - mean) / std
        return (z_scores.abs() > self._threshold).astype(int)
