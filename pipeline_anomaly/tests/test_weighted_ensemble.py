from __future__ import annotations

import math

from pipeline_anomaly.application.services.ensemble import WeightedAnomalyEnsemble
from pipeline_anomaly.domain.models.anomaly import Anomaly


def test_ensemble_requires_min_detectors() -> None:
    ensemble = WeightedAnomalyEnsemble(weights={"a": 1.0, "b": 1.0}, min_detectors=2)
    anomalies = (
        Anomaly(detector="a", score=0.5, severity=0.3, description=""),
        Anomaly(detector="b", score=0.4, severity=0.0, description=""),
    )

    assert ensemble.combine(anomalies) is None


def test_ensemble_combines_weighted_scores() -> None:
    ensemble = WeightedAnomalyEnsemble(weights={"a": 1.0, "b": 2.0}, min_detectors=2)
    anomalies = (
        Anomaly(detector="a", score=0.5, severity=0.3, description=""),
        Anomaly(detector="b", score=0.8, severity=0.9, description=""),
    )

    result = ensemble.combine(anomalies)

    assert result is not None
    expected_severity = (0.3 * 1.0 + 0.9 * 2.0) / 3.0
    assert math.isclose(result.severity, expected_severity, rel_tol=1e-6)
