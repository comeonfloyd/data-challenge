from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import pandas as pd


@dataclass(slots=True)
class Feature:
    name: str
    builder: Callable[[pd.DataFrame], pd.Series]

    def compute(self, frame: pd.DataFrame) -> pd.Series:
        return self.builder(frame)
