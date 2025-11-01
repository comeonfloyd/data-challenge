from __future__ import annotations

import json

from loguru import logger

from pipeline_anomaly.domain.models.anomaly import AnomalyReport


class StdOutAlertSink:
    def send(self, report: AnomalyReport) -> None:
        payload = {
            "generated_at": report.generated_at.isoformat(),
            "window_start": report.window_start.isoformat(),
            "window_end": report.window_end.isoformat(),
            "anomalies": [
                {
                    "detector": anomaly.detector,
                    "score": anomaly.score,
                    "severity": anomaly.severity,
                    "description": anomaly.description,
                }
                for anomaly in report.anomalies
            ],
        }
        logger.error("ANOMALY ALERT: {}", json.dumps(payload, ensure_ascii=False))
