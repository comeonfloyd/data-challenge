from __future__ import annotations

import pandas as pd
from sklearn.cluster import DBSCAN

from pipeline_anomaly.infrastructure.detectors.base import PandasDetector


class DBSCANDetector(PandasDetector):
    def __init__(self, eps: float, min_samples: int) -> None:
        super().__init__(name="dbscan")
        self._model = DBSCAN(eps=eps, min_samples=min_samples)

    def fit_predict(self, dataframe: pd.DataFrame) -> pd.Series:
        features = dataframe[["value", "attribute"]]
        predictions = self._model.fit_predict(features)
        return pd.Series((predictions == -1).astype(int))
