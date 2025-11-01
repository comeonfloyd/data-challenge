from __future__ import annotations

import pandas as pd

from pipeline_anomaly.domain.services.interfaces import AnomalyDetector


class PandasDetector:
    def __init__(self, name: str) -> None:
        self.name = name

    def severity(self, scores: pd.Series) -> float:
        if scores.empty:
            return 0.0
        positives = (scores > 0).sum()
        return positives / len(scores)
