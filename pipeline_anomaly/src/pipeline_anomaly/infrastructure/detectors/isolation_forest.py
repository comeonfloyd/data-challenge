from __future__ import annotations

import pandas as pd
from sklearn.ensemble import IsolationForest

from pipeline_anomaly.infrastructure.detectors.base import PandasDetector


class IsolationForestDetector(PandasDetector):
    def __init__(self, contamination: float, random_state: int) -> None:
        super().__init__(name="isolation_forest")
        self._model = IsolationForest(contamination=contamination, random_state=random_state)

    def fit_predict(self, dataframe: pd.DataFrame) -> pd.Series:
        features = dataframe[["value", "attribute"]]
        predictions = self._model.fit_predict(features)
        return pd.Series((predictions == -1).astype(int))
