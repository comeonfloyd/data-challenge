from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Sequence

from pipeline_anomaly.domain.models.anomaly import Anomaly


@dataclass(slots=True)
class WeightedAnomalyEnsemble:
    """Aggregates detector outputs into a synthetic ensemble signal."""

    weights: Mapping[str, float]
    min_detectors: int = 2

    def combine(self, anomalies: Sequence[Anomaly]) -> Anomaly | None:
        """Return a synthetic anomaly when enough detectors agree."""

        if not anomalies:
            return None

        positive = [item for item in anomalies if item.severity > 0]
        if len(positive) < self.min_detectors:
            return None

        weighted_severity, weighted_score, total_weight = 0.0, 0.0, 0.0
        for anomaly in positive:
            weight = self.weights.get(anomaly.detector, 1.0)
            weighted_severity += weight * anomaly.severity
            weighted_score += weight * anomaly.score
            total_weight += weight

        if total_weight == 0:
            return None

        severity = weighted_severity / total_weight
        score = weighted_score / total_weight
        description = (
            "ensemble agreement: "
            f"{len(positive)}/{self.min_detectors} detectors severity={severity:.3f}"
        )
        return Anomaly(detector="ensemble", score=score, severity=severity, description=description)

