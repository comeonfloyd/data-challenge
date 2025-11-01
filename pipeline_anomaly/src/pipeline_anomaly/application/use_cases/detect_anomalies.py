from __future__ import annotations

from datetime import datetime

import pandas as pd

from pipeline_anomaly.domain.models.anomaly import Anomaly, AnomalyReport
from pipeline_anomaly.domain.services.interfaces import AnomalyDetector, ClickHouseWriter


class DetectAnomalies:
    def __init__(self, writer: ClickHouseWriter, detectors: list[AnomalyDetector], threshold: float) -> None:
        self._writer = writer
        self._detectors = detectors
        self._threshold = threshold

    def execute(self) -> AnomalyReport:
        dataframe = self._writer.read_latest_window()
        window_start = dataframe["event_time"].min()
        window_end = dataframe["event_time"].max()

        anomalies: list[Anomaly] = []
        for detector in self._detectors:
            scores = detector.fit_predict(dataframe)
            severity = detector.severity(scores)
            anomalies.append(
                Anomaly(
                    detector=detector.name,
                    score=float(scores.mean()),
                    severity=float(severity),
                    description=f"{detector.name} severity={severity:.3f}",
                )
            )

        report = AnomalyReport(
            generated_at=datetime.utcnow(),
            window_start=window_start,
            window_end=window_end,
            anomalies=tuple(anomalies),
        )
        self._writer.persist_report(report)
        return report

    def is_alert(self, report: AnomalyReport) -> bool:
        return report.highest_severity() >= self._threshold
