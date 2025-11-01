from __future__ import annotations

from loguru import logger

from feature_store_ml.infrastructure.registry import FeatureRegistry
from feature_store_ml.infrastructure.repositories.clickhouse_repository import ClickHouseRepository


class MaterializeFeatures:
    def __init__(self, repository: ClickHouseRepository, registry: FeatureRegistry) -> None:
        self._repository = repository
        self._registry = registry

    def execute(self, lookback_hours: int) -> None:
        self._repository.ensure_schema()
        raw = self._repository.read_training_window(lookback_hours)
        if raw.empty:
            logger.warning("no raw events for lookback window")
            return
        features = self._registry.compute(raw)
        self._repository.write_feature_batch(features)
        logger.info("materialized %d rows", len(features))
