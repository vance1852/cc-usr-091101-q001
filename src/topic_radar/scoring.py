"""候选题材评分。

四个维度均在 0..1，加权后乘降级系数：

- 热度 heat：观察窗内需求侧指标的对数体量，榜内横向归一；
- 增速 momentum：近窗与等长前窗的需求速率对比，自身时序对比，
  不依赖其它题材，因此跨版本可比；
- 机会 opportunity：供给侧指标（在榜作品数等）越密、机会越低；
  完全没有供给证据时给中性分并显式标注；
- 受众契合 audience：分类词表命中本工作室受众偏好的最高档。

降级（来源未知、置信不足、证据稀薄、信号滞后）只乘系数并记录原因，
不静默丢弃题材。
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from .config import Category, RadarConfig, Taxonomy
from .ingest import DedupResult

RECENT = "recent"
PRIOR = "prior"
OLDER = "older"


@dataclass(frozen=True)
class Evidence:
    event_id: str
    source: str
    known_source: bool
    occurred_at: datetime
    received_at: datetime
    lag_seconds: float
    bucket: str
    demand: float
    supply: float
    confidence: float
    reliability: float
    duplicate_arrivals: int
    metrics_conflict: bool
    metrics: dict[str, float] = field(default_factory=dict)
    attributes: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ScoreBreakdown:
    heat: float
    momentum: float
    opportunity: float
    audience: float
    base_score: float
    confidence: float
    penalty_multiplier: float
    feedback_adjustment: float
    final_score: float
    raw_demand_recent: float
    raw_demand_prior: float
    raw_supply_recent: float
    degradation: tuple[str, ...]
    categories: tuple[str, ...]


@dataclass(frozen=True)
class Candidate:
    topic: str
    breakdown: ScoreBreakdown
    evidence: tuple[Evidence, ...]

    @property
    def rank_score(self) -> float:
        return self.breakdown.final_score


def _bucket(occurred: datetime, cutoff: datetime, window: timedelta, prior_windows: int) -> str:
    if cutoff - window < occurred <= cutoff:
        return RECENT
    if cutoff - window * (prior_windows + 1) < occurred <= cutoff - window:
        return PRIOR
    return OLDER


def _demand_and_supply(metrics: dict[str, float], cfg: RadarConfig) -> tuple[float, float]:
    demand = sum(v for k, v in metrics.items() if k in cfg.demand_metrics)
    supply = sum(v for k, v in metrics.items() if k in cfg.supply_metrics)
    return demand, supply


def _event_confidence(signal_metrics_conflict: bool, attrs: dict[str, Any], source: str, cfg: RadarConfig) -> float:
    profile = cfg.profile(source)
    raw = attrs.get("confidence", profile.default_confidence)
    try:
        value = float(raw)
    except (TypeError, ValueError):
        value = profile.default_confidence
    return max(0.0, min(1.0, value * profile.reliability))


def build_candidates(
    deduped: list[DedupResult],
    cutoff: datetime,
    cfg: RadarConfig,
    taxonomy: Taxonomy,
    feedback_adjustment: dict[str, float] | None = None,
) -> list[Candidate]:
    """以 cutoff 封存一版候选榜。

    只有 ``received_at <= cutoff`` 的信号参与，晚到记录自动落到后续版本；
    信号落在近期还是前窗由 ``occurred_at`` 决定——两轨时间在此分离。
    """
    feedback_adjustment = feedback_adjustment or {}
    window = timedelta(hours=cfg.window_hours)

    # 题材 -> 证据
    per_topic: dict[str, list[Evidence]] = {}
    for result in deduped:
        s = result.signal
        if s.received_at > cutoff:
            continue  # 晚到数据：不影响当前版本
        demand, supply = _demand_and_supply(s.metrics, cfg)
        conf = _event_confidence(result.metrics_conflict, s.attributes, s.source, cfg)
        ev = Evidence(
            event_id=s.event_id,
            source=s.source,
            known_source=cfg.is_known_source(s.source),
            occurred_at=s.occurred_at,
            received_at=s.received_at,
            lag_seconds=(s.received_at - s.occurred_at).total_seconds(),
            bucket=_bucket(s.occurred_at, cutoff, window, cfg.prior_windows),
            demand=demand,
            supply=supply,
            confidence=conf,
            reliability=cfg.profile(s.source).reliability,
            duplicate_arrivals=result.duplicate_count,
            metrics_conflict=result.metrics_conflict,
            metrics=dict(s.metrics),
            attributes=dict(s.attributes),
        )
        per_topic.setdefault(s.topic, []).append(ev)

    if not per_topic:
        return []

    # 横向归一基准
    max_heat_raw = 0.0
    max_supply = 0.0
    for evs in per_topic.values():
        recent = [e for e in evs if e.bucket == RECENT]
        max_heat_raw = max(max_heat_raw, sum(e.demand for e in recent))
        max_supply = max(max_supply, sum(e.supply for e in recent))

    weights = cfg.weights
    w_sum = sum(weights.values()) or 1.0

    candidates: list[Candidate] = []
    for topic, evs in per_topic.items():
        recent = [e for e in evs if e.bucket == RECENT]
        prior = [e for e in evs if e.bucket == PRIOR]
        demand_recent = sum(e.demand for e in recent)
        demand_prior = sum(e.demand for e in prior)
        supply_recent = sum(e.supply for e in recent)

        heat = math.log1p(demand_recent) / math.log1p(max_heat_raw) if max_heat_raw > 0 else 0.0

        rate_recent = demand_recent / window.total_seconds()
        rate_prior = demand_prior / (window.total_seconds() * cfg.prior_windows)
        if rate_recent == 0 and rate_prior == 0:
            momentum = 0.5
        elif rate_prior == 0:
            momentum = 1.0
        else:
            growth = (rate_recent - rate_prior) / (rate_recent + rate_prior)
            momentum = (growth + 1) / 2

        if supply_recent <= 0:
            opportunity = 0.5
        elif max_supply <= 0:
            opportunity = 0.5
        else:
            opportunity = 1 - math.log1p(supply_recent) / math.log1p(max_supply)

        affinity, cats = taxonomy.affinity(topic)
        audience = affinity

        base = (
            weights["heat"] * heat
            + weights["momentum"] * momentum
            + weights["opportunity"] * opportunity
            + weights["audience"] * audience
        ) / w_sum * 100

        # ---- 置信度与降级 ----
        total_w = sum(max(e.demand, 1.0) for e in evs)
        confidence = sum(e.confidence * max(e.demand, 1.0) for e in evs) / total_w

        degradation: list[str] = []
        multiplier = 1.0
        known_events = sum(1 for e in evs if e.known_source)
        if known_events * 2 < len(evs):
            degradation.append("来源未知占多数，按未知来源可靠性降级")
            multiplier *= cfg.penalties["unknown_source"]
        if confidence < cfg.min_confidence:
            degradation.append(f"综合置信度 {confidence:.2f} 低于阈值 {cfg.min_confidence:.2f}")
            multiplier *= cfg.penalties["low_confidence"]
        if len(evs) < cfg.thin_evidence_events or len({e.source for e in evs}) < cfg.thin_evidence_sources:
            degradation.append(
                f"证据稀薄（{len(evs)} 条信号 / {len({e.source for e in evs})} 个来源）"
            )
            multiplier *= cfg.penalties["thin_evidence"]
        if any(e.lag_seconds > cfg.late_lag_seconds for e in evs):
            degradation.append("存在发生后超过滞后阈值才接收到的信号")
            multiplier *= cfg.penalties["late_lag"]
        if supply_recent <= 0:
            degradation.append("竞争密度数据缺失，机会分按中性 0.5 计")
        if any(e.metrics_conflict for e in evs):
            degradation.append("同源重复投递的指标不一致，已采用最早到达版本")
        if not recent:
            degradation.append("观察窗内无新信号，仅由窗外记录支撑")

        adjustment = feedback_adjustment.get(topic, 0.0)
        final_score = max(0.0, min(100.0, base * multiplier + adjustment))

        breakdown = ScoreBreakdown(
            heat=round(heat, 4),
            momentum=round(momentum, 4),
            opportunity=round(opportunity, 4),
            audience=round(audience, 4),
            base_score=round(base, 4),
            confidence=round(confidence, 4),
            penalty_multiplier=round(multiplier, 4),
            feedback_adjustment=round(adjustment, 4),
            final_score=round(final_score, 4),
            raw_demand_recent=round(demand_recent, 4),
            raw_demand_prior=round(demand_prior, 4),
            raw_supply_recent=round(supply_recent, 4),
            degradation=tuple(degradation),
            categories=tuple(c.name for c in cats),
        )
        ordered_ev = tuple(sorted(evs, key=lambda e: (e.occurred_at, e.received_at, e.source)))
        candidates.append(Candidate(topic, breakdown, ordered_ev))

    candidates.sort(key=lambda c: (-c.rank_score, c.topic))
    return candidates
