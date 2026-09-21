"""雷达评分配置：观察窗口、权重、降级系数均可由 JSON 覆盖。"""

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class RadarConfig:
    windows: dict[str, float] = field(default_factory=lambda: {"heat_hours": 2.0, "velocity_hours": 2.0, "late_hours": 1.0})
    source_weights: dict[str, float] = field(default_factory=lambda: {"platform": 1.0, "search": 0.8, "editor": 0.55})
    default_source_weight: float = 0.5
    source_confidence: dict[str, float] = field(default_factory=lambda: {"platform": 0.7, "search": 0.9, "editor": 0.7})
    metric_weights: dict[str, float] = field(default_factory=lambda: {"searches": 0.002, "posts": 0.05, "mentions": 1.0})
    score_weights: dict[str, float] = field(default_factory=lambda: {"heat": 0.38, "velocity": 0.27, "audience_fit": 0.25, "competition_density": 0.1})
    thresholds: dict[str, float] = field(default_factory=lambda: {"min_confidence": 0.55, "min_distinct_sources": 2})
    uncategorized_fit: float = 0.3
    downgrade_multipliers: dict[str, float] = field(default_factory=lambda: {"low_confidence": 0.7, "insufficient_sources": 0.85, "uncategorized": 0.9, "cooling": 0.8})
    feedback_adjustments: dict[str, float] = field(default_factory=lambda: {"follow": 0.1, "shelve": -0.15, "misjudge": -0.5})
    velocity_newcomer_rate: float = 5.0
    heat_saturation: float = 10.0
    velocity_smoothing: float = 1.0

    @classmethod
    def load(cls, path: str | Path | None) -> "RadarConfig":
        if path is None:
            return cls()
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        base = cls()
        merged: dict[str, Any] = {}
        for key in base.__dataclass_fields__:
            value = getattr(base, key)
            override = raw.get(key)
            if isinstance(value, dict) and isinstance(override, dict):
                merged[key] = {**value, **override}
            else:
                merged[key] = override if key in raw else value
        return cls(**merged)

    def canonical_json(self) -> str:
        return json.dumps(
            {key: getattr(self, key) for key in self.__dataclass_fields__},
            ensure_ascii=False,
            sort_keys=True,
        )
