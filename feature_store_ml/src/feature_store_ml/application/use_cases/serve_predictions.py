from __future__ import annotations

import pandas as pd
from loguru import logger

from feature_store_ml.infrastructure.modeling.trainer import LightGBMTrainer
from feature_store_ml.infrastructure.repositories.clickhouse_repository import ClickHouseRepository


class ServePredictions:
    def __init__(
        self,
        repository: ClickHouseRepository,
        trainer: LightGBMTrainer,
        batch_size: int,
        threshold: float,
    ) -> None:
        self._repository = repository
        self._trainer = trainer
        self._batch_size = batch_size
        self._threshold = threshold

    def execute(self) -> pd.DataFrame:
        feature_view = self._repository.fetch_inference_candidates(self._batch_size)
        if feature_view.empty:
            raise RuntimeError("feature store is empty, run materialization first")
        model, metadata = self._trainer.load()
        feature_names = metadata.get("features", [])
        features = feature_view[feature_names]
        scores = model.predict(features)
        logger.info("generated %d predictions", len(scores))
        payload = feature_view[["entity_id", "event_time"]].copy()
        payload["score"] = scores
        payload["prediction"] = (payload["score"] >= self._threshold).astype(int)
        self._repository.persist_predictions(payload[["event_time", "entity_id", "score"]])
        return payload
