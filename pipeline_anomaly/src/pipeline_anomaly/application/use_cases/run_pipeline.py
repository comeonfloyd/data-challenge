from __future__ import annotations

from loguru import logger

from pipeline_anomaly.domain.services.interfaces import AlertSink
from pipeline_anomaly.application.use_cases.compute_aggregates import ComputeAggregates
from pipeline_anomaly.application.use_cases.detect_anomalies import DetectAnomalies
from pipeline_anomaly.application.use_cases.load_dataset import LoadSyntheticDataset


class RunPipeline:
    def __init__(
        self,
        loader: LoadSyntheticDataset,
        aggregator: ComputeAggregates,
        detector: DetectAnomalies,
        alert_sink: AlertSink,
        alerts_enabled: bool,
    ) -> None:
        self._loader = loader
        self._aggregator = aggregator
        self._detector = detector
        self._alert_sink = alert_sink
        self._alerts_enabled = alerts_enabled

    def execute(self) -> None:
        logger.info("starting pipeline")
        self._loader.execute()
        aggregates = self._aggregator.execute()
        logger.info("aggregates persisted: {}", aggregates.as_dict())
        report = self._detector.execute()
        logger.info("anomaly report generated: {}", report)
        if self._alerts_enabled and self._detector.is_alert(report):
            logger.warning("alert threshold exceeded")
            self._alert_sink.send(report)
        logger.info("pipeline finished")
