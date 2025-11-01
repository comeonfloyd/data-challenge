from __future__ import annotations

import pandas as pd
from loguru import logger

from feature_store_ml.infrastructure.modeling.trainer import LightGBMTrainer
from feature_store_ml.infrastructure.repositories.clickhouse_repository import ClickHouseRepository


class ServePredictions:
    def __init__(self, repository: ClickHouseRepository, trainer: LightGBMTrainer, threshold: float) -> None:
        self._repository = repository
        self._trainer = trainer
        self._threshold = threshold

    def execute(self) -> None:
        frame = self._repository.read_feature_store()
        if frame.empty:
            logger.warning("feature store empty, nothing to serve")
            return
        model, metadata = self._trainer.load()
        feature_names = metadata["features"]
        scores = model.predict(frame[feature_names])
        payload = pd.DataFrame(
            {
                "event_time": frame["event_time"],
                "entity_id": frame["entity_id"],
                "score": scores,
                "label": (scores >= self._threshold).astype(int),
            }
        )
        self._repository.write_predictions(payload)
        logger.info("served %d predictions", len(payload))
