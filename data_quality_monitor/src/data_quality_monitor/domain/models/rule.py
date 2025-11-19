from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True, slots=True)
class Expectation:
    type: str
    params: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class TableRule:
    table: str
    expectations: tuple[Expectation, ...]
