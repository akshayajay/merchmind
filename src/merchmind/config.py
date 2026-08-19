from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class DataPaths:
    """Resolved Bronze, Silver, Gold and reporting paths."""

    root: Path

    @property
    def bronze(self) -> Path:
        return self.root / "bronze"

    @property
    def silver(self) -> Path:
        return self.root / "silver"

    @property
    def gold(self) -> Path:
        return self.root / "gold"

    @property
    def reports(self) -> Path:
        return self.root / "reports"

    def create(self) -> DataPaths:
        for path in (self.root, self.bronze, self.silver, self.gold, self.reports):
            path.mkdir(parents=True, exist_ok=True)
        return self


def default_data_dir() -> Path:
    return Path(os.getenv("MERCHMIND_DATA_DIR", "data")).expanduser().resolve()
