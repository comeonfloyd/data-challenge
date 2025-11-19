from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class ModelArtifact:
    model_path: Path
    feature_names: tuple[str, ...]
