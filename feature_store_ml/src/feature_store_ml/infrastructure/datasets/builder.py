from __future__ import annotations

import pandas as pd

from feature_store_ml.domain.models.dataset import FeatureDataset
from feature_store_ml.infrastructure.registry import FeatureRegistry


class DatasetBuilder:
    def __init__(self, registry: FeatureRegistry) -> None:
        self._registry = registry

    def build(self, frame: pd.DataFrame) -> FeatureDataset:
        feature_columns = self._registry.feature_names
        missing = [column for column in feature_columns if column not in frame.columns]
        if missing:
            raise KeyError(f"missing feature columns: {missing}")
        features = frame[feature_columns].fillna(0.0)
        target = frame.get("label")
        if target is None:
            target = pd.Series(0, index=frame.index)
        target = (target > 0).astype(int)
        return FeatureDataset(features=features, target=target)
