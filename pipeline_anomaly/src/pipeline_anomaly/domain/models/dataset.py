from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(slots=True)
class FeatureDataset:
    features: pd.DataFrame
    target: pd.Series
