from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Mapping


@dataclass(frozen=True, slots=True)
class RuleResult:
    rule: str
    passed: bool
    details: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class QualityReport:
    table: str
    generated_at: datetime
    results: tuple[RuleResult, ...]

    def has_failures(self) -> bool:
        return any(not result.passed for result in self.results)

    def as_rows(self) -> list[dict[str, object]]:
        return [
            {
                "table": self.table,
                "generated_at": self.generated_at,
                "rule": result.rule,
                "passed": result.passed,
                "details": result.details,
            }
            for result in self.results
        ]
