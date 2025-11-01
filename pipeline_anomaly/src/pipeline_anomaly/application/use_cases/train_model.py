from __future__ import annotations

from loguru import logger

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

    def execute(self, lookback_hours: int) -> None:
        frame = self._repository.read_training_window(lookback_hours)
        if frame.empty:
            raise RuntimeError("training window is empty")
        dataset = self._dataset_builder.build(frame)
        artifact = self._trainer.train(dataset)
        logger.info("model persisted to %s", artifact.model_path)
