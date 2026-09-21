"""分类词表与雷达参数加载。

配置为纯 JSON，便于策划侧修改；缺失文件时回退到内置默认值，
但调用方通常应显式传入仓库内的 config/*.json 路径。
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_TAXONOMY = REPO_ROOT / "config" / "taxonomy.json"
DEFAULT_RADAR = REPO_ROOT / "config" / "radar.json"


@dataclass(frozen=True)
class Category:
    name: str
    keywords: tuple[str, ...]


@dataclass(frozen=True)
class Taxonomy:
    categories: tuple[Category, ...]
    audience_affinity: dict[str, float]

    def match(self, topic: str) -> tuple[Category, ...]:
        """返回与题面命中关键词的全部分类（可多归属）。"""
        return tuple(c for c in self.categories if any(k in topic for k in c.keywords))

    def affinity(self, topic: str) -> tuple[float, tuple[Category, ...]]:
        cats = self.match(topic)
        if not cats:
            return 0.0, ()
        return max(self.audience_affinity.get(c.name, 0.5) for c in cats), cats

    @classmethod
    def load(cls, path: str | Path = DEFAULT_TAXONOMY) -> "Taxonomy":
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        cats = tuple(Category(c["name"], tuple(c["keywords"])) for c in raw["categories"])
        return cls(cats, dict(raw.get("audience_affinity", {})))


@dataclass(frozen=True)
class SourceProfile:
    reliability: float
    kind: str
    default_confidence: float


@dataclass(frozen=True)
class RadarConfig:
    sources: dict[str, SourceProfile]
    unknown_source: SourceProfile
    demand_metrics: frozenset[str]
    supply_metrics: frozenset[str]
    window_hours: float
    prior_windows: int
    weights: dict[str, float]
    min_confidence: float
    thin_evidence_events: int
    thin_evidence_sources: int
    late_lag_seconds: float
    penalties: dict[str, float]
    feedback_adjustments: dict[str, float]

    def profile(self, source: str) -> SourceProfile:
        return self.sources.get(source, self.unknown_source)

    def is_known_source(self, source: str) -> bool:
        return source in self.sources

    @classmethod
    def load(cls, path: str | Path = DEFAULT_RADAR) -> "RadarConfig":
        raw: dict[str, Any] = json.loads(Path(path).read_text(encoding="utf-8"))
        th = raw["thresholds"]
        return cls(
            sources={k: SourceProfile(float(v["reliability"]), str(v["kind"]), float(v.get("default_confidence", 1.0))) for k, v in raw["sources"].items()},
            unknown_source=SourceProfile(float(raw["unknown_source"]["reliability"]), str(raw["unknown_source"]["kind"]), float(raw["unknown_source"].get("default_confidence", 0.5))),
            demand_metrics=frozenset(raw["metrics"]["demand"]),
            supply_metrics=frozenset(raw["metrics"]["supply"]),
            window_hours=float(raw["window_hours"]),
            prior_windows=int(raw.get("prior_windows", 1)),
            weights={k: float(v) for k, v in raw["weights"].items()},
            min_confidence=float(th["min_confidence"]),
            thin_evidence_events=int(th["thin_evidence_events"]),
            thin_evidence_sources=int(th["thin_evidence_sources"]),
            late_lag_seconds=float(th["late_lag_seconds"]),
            penalties={k: float(v) for k, v in raw["penalties"].items()},
            feedback_adjustments={k: float(v) for k, v in raw["feedback_adjustments"].items()},
        )
