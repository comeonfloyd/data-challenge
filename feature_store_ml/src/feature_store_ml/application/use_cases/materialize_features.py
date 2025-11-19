from __future__ import annotations

import pandas as pd
from loguru import logger

from feature_store_ml.infrastructure.registry import FeatureRegistry
from feature_store_ml.infrastructure.repositories.clickhouse_repository import ClickHouseRepository


class MaterializeFeatures:
    def __init__(self, repository: ClickHouseRepository, registry: FeatureRegistry) -> None:
        self._repository = repository
        self._registry = registry

    def execute(self, lookback_hours: int, min_records: int) -> pd.DataFrame:
        self._repository.ensure_schema()
        events = self._repository.fetch_events(lookback_hours)
        if len(events) < min_records:
            raise ValueError(f"not enough rows to materialize features: {len(events)} < {min_records}")
        logger.info("building features for %d rows", len(events))
        feature_view = self._registry.compute(events)
        logger.info("persisting %d feature rows", len(feature_view))
        self._repository.persist_feature_view(feature_view)
        return feature_view
