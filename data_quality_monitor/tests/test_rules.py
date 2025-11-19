import pandas as pd

from data_quality_monitor.domain.models.rule import Expectation, TableRule
from data_quality_monitor.infrastructure.rules import engine


def test_completeness_passes():
    frame = pd.DataFrame({"value": [1, 2, 3]})
    rule = TableRule(table="t", expectations=(Expectation("completeness", {"column": "value", "threshold": 1.0}),))
    report = engine.evaluate(rule, frame)
    assert report.results[0].passed


def test_freshness_detects_stale_data():
    frame = pd.DataFrame({"event_time": pd.to_datetime(["2023-01-01T00:00:00Z"])})
    expectation = Expectation("freshness", {"column": "event_time", "max_minutes": 10})
    rule = TableRule(table="t", expectations=(expectation,))
    report = engine.evaluate(rule, frame)
    assert not report.results[0].passed
