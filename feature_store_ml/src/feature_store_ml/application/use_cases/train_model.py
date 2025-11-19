from __future__ import annotations

from loguru import logger

from feature_store_ml.domain.models.model_artifact import ModelArtifact
from feature_store_ml.infrastructure.datasets.builder import DatasetBuilder
from feature_store_ml.infrastructure.modeling.trainer import LightGBMTrainer
from feature_store_ml.infrastructure.repositories.clickhouse_repository import ClickHouseRepository


class TrainModel:
    def __init__(
        self,
        repository: ClickHouseRepository,
        dataset_builder: DatasetBuilder,
        trainer: LightGBMTrainer,
    ) -> None:
        self._repository = repository
        self._dataset_builder = dataset_builder
        self._trainer = trainer

    def execute(self, lookback_hours: int) -> ModelArtifact:
        feature_view = self._repository.load_feature_view(lookback_hours)
        if feature_view.empty:
            raise RuntimeError("feature view is empty, run materialization first")
        dataset = self._dataset_builder.build(feature_view)
        logger.info("training model on %d rows", len(dataset.features))
        return self._trainer.train(dataset)
