from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import pandas as pd


@dataclass(frozen=True, slots=True)
class RecordBatch:
    """Пакет строк для загрузки в ClickHouse."""

    dataframe: pd.DataFrame

    def __iter__(self) -> Iterable[pd.Series]:
        return (row for _, row in self.dataframe.iterrows())

    @property
    def size(self) -> int:
        return len(self.dataframe)
