from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(slots=True)
class FeatureDataset:
    """Матрица признаков и таргет для обучения/инференса."""

    features: pd.DataFrame
    target: pd.Series

    def as_lgbm(self) -> tuple[pd.DataFrame, pd.Series]:
        return self.features, self.target
