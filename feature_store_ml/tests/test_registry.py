import pandas as pd

from feature_store_ml.infrastructure.config import FeatureConfig
from feature_store_ml.infrastructure.registry import FeatureRegistry


def test_registry_computes_features():
    data = pd.DataFrame(
        {
            "entity_id": [1, 1, 1, 2],
            "event_time": pd.date_range("2024-01-01", periods=4, freq="30min"),
            "value": [1.0, 2.0, 5.0, 3.0],
            "attribute": [0.1, 0.2, 0.2, 0.3],
            "label": [0, 0, 1, 0],
        }
    )
    registry = FeatureRegistry.from_config(FeatureConfig(lookback_hours=24, bucket_minutes=60, min_records=10))
    features = registry.compute(data)
    assert set(registry.feature_names).issubset(features.columns)
    assert "value_trend" in features.columns
