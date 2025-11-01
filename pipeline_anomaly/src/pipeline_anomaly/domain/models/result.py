from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(slots=True)
class RuleResult:
    rule: str
    passed: bool
    details: dict[str, object]


@dataclass(slots=True)
class QualityReport:
    table: str
    generated_at: datetime
    results: tuple[RuleResult, ...]

    def as_dicts(self) -> list[dict[str, object]]:
        rows: list[dict[str, object]] = []
        for result in self.results:
            rows.append(
                {
                    "table": self.table,
                    "generated_at": self.generated_at,
                    "rule": result.rule,
                    "passed": int(result.passed),
                    "details": result.details,
                }
            )
        return rows
