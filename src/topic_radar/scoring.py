"""选题机会评分引擎（纯函数，无 IO）。

输入某个截止时刻 ``as_of`` 下"已经收到"的信号与"已经做出"的决策，
输出带完整分项与证据的候选榜单。版本快照与实时预览共用这条路径，
因此保存后的榜单与当时预览必然一致。

时间语义
========
* 信号可见条件：``received_at <= as_of`` 且 ``occurred_at <= as_of``。
  发生在更早、但 ``as_of`` 之后才送到的信号属于"晚到数据"，不参与本版本，
  只在快照的 ``excluded_late`` 中留痕，等待下一个版本纳入。
* 热度/增速窗口全部以 ``occurred_at``（事件发生时间）切分。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from .config import RadarConfig
from .contracts import SignalEnvelope
from .taxonomy import Taxonomy

VALID_ACTIONS = ("follow", "shelve", "misjudge")


@dataclass(frozen=True)
class _SignalView:
    envelope: SignalEnvelope
    contribution: float
    signal_confidence: float
    source_weight: float
    late: bool
    known_source: bool
    has_metrics: bool


def _signal_contribution(envelope: SignalEnvelope, config: RadarConfig) -> tuple[float, bool, bool]:
    """返回 (热度贡献, 来源是否已知, 是否有有效指标)。"""
    known_source = envelope.source in config.source_weights
    source_weight = config.source_weights.get(envelope.source, config.default_source_weight)
    value = 0.0
    for metric, amount in envelope.metrics.items():
        value += config.metric_weights.get(metric, 0.0) * amount
    return source_weight * value, known_source, value > 0.0


def _confidence(envelope: SignalEnvelope, config: RadarConfig) -> float:
    source_conf = config.source_confidence.get(envelope.source, config.thresholds["min_confidence"])
    attr_conf = envelope.attributes.get("confidence")
    if isinstance(attr_conf, (int, float)):
        source_conf *= max(0.0, min(1.0, float(attr_conf)))
    return max(0.0, min(1.0, source_conf))


def _evidence(view: _SignalView, in_heat: bool, in_velocity_now: bool, in_velocity_prev: bool) -> dict[str, Any]:
    e = view.envelope
    windows = []
    if in_heat:
        windows.append("heat")
    if in_velocity_now:
        windows.append("velocity_now")
    if in_velocity_prev:
        windows.append("velocity_prev")
    return {
        "event_id": e.event_id,
        "source": e.source,
        "occurred_at": e.occurred_at.isoformat(),
        "received_at": e.received_at.isoformat(),
        "metrics": e.metrics,
        "contribution": round(view.contribution, 6),
        "confidence": round(view.signal_confidence, 4),
        "late_arrival": view.late,
        "windows": windows,
    }


def _latest_decisions(decisions: list[dict[str, Any]], as_of: datetime) -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for d in decisions:
        decided = d["decided_at"] if isinstance(d["decided_at"], datetime) else datetime.fromisoformat(d["decided_at"])
        if decided > as_of:
            continue  # 历史版本不允许被未来的人工决策影响
        if d["action"] not in VALID_ACTIONS:
            continue
        current = latest.get(d["topic"])
        if current is None or decided >= (current["decided_at"] if isinstance(current["decided_at"], datetime)
                                          else datetime.fromisoformat(current["decided_at"])):
            latest[d["topic"]] = {**d, "decided_at": decided}
    return latest


def build_radar(as_of: datetime, signals: list[SignalEnvelope], decisions: list[dict[str, Any]],
                config: RadarConfig, taxonomy: Taxonomy) -> dict[str, Any]:
    heat_hours = config.windows["heat_hours"]
    vel_hours = config.windows["velocity_hours"]
    late_hours = config.windows["late_hours"]

    heat_start = as_of - timedelta(hours=heat_hours)
    vel_start = as_of - timedelta(hours=vel_hours)
    prev_start = as_of - timedelta(hours=2 * vel_hours)

    visible: list[_SignalView] = []
    excluded_late: list[dict[str, Any]] = []
    excluded_future: list[dict[str, Any]] = []
    for e in signals:
        if e.received_at > as_of:
            excluded_late.append({
                "event_id": e.event_id, "source": e.source, "topic": e.topic,
                "occurred_at": e.occurred_at.isoformat(), "received_at": e.received_at.isoformat(),
                "reason": "received_after_cutoff",
            })
            continue
        if e.occurred_at > as_of:
            excluded_future.append({
                "event_id": e.event_id, "source": e.source, "topic": e.topic,
                "occurred_at": e.occurred_at.isoformat(), "received_at": e.received_at.isoformat(),
                "reason": "occurred_after_cutoff",
            })
            continue
        contribution, known_source, has_metrics = _signal_contribution(e, config)
        visible.append(_SignalView(
            envelope=e,
            contribution=contribution,
            signal_confidence=_confidence(e, config),
            source_weight=config.source_weights.get(e.source, config.default_source_weight),
            late=(e.received_at - e.occurred_at) > timedelta(hours=late_hours),
            known_source=known_source and bool(e.source.strip()),
            has_metrics=has_metrics,
        ))

    # 按题材分组（确定性顺序）。
    grouped: dict[str, list[_SignalView]] = {}
    for view in sorted(visible, key=lambda v: (v.envelope.received_at, v.envelope.source, v.envelope.event_id)):
        grouped.setdefault(view.envelope.topic.strip(), []).append(view)

    # 竞争密度：先算每个题材当前热度窗口内是否活跃、属于哪个分类。
    topic_category: dict[str, str | None] = {}
    topic_active: dict[str, bool] = {}
    for topic, views in grouped.items():
        topic_category[topic] = taxonomy.classify(topic).category_name
        topic_active[topic] = any(v.envelope.occurred_at > heat_start for v in views)
    active_by_category: dict[str, list[str]] = {}
    for topic, active in topic_active.items():
        if active:
            key = topic_category[topic] or "__uncategorized__"
            active_by_category.setdefault(key, []).append(topic)

    latest = _latest_decisions(decisions, as_of)
    candidates: list[dict[str, Any]] = []

    for topic, views in grouped.items():
        classification = taxonomy.classify(topic)
        heat_raw = sum(v.contribution for v in views if v.envelope.occurred_at > heat_start)
        vel_now = sum(v.contribution for v in views if vel_start < v.envelope.occurred_at <= as_of)
        vel_prev = sum(v.contribution for v in views if prev_start < v.envelope.occurred_at <= vel_start)

        if vel_prev > 0:
            velocity_ratio = (vel_now - vel_prev) / vel_prev
            newcomer = False
        elif vel_now > 0:
            velocity_ratio = config.velocity_newcomer_rate
            newcomer = True
        else:
            velocity_ratio = 0.0
            newcomer = False

        heat_score = heat_raw / (heat_raw + config.heat_saturation) if heat_raw > 0 else 0.0
        velocity_score = max(0.0, velocity_ratio) / (1.0 + abs(velocity_ratio)) if velocity_ratio > 0 else 0.0

        peers = active_by_category.get(topic_category[topic] or "__uncategorized__", [])
        competitors = sorted(t for t in peers if t != topic)
        density_score = len(competitors) / (len(competitors) + 2.0)

        # 置信度：按热度贡献加权；全部无指标时退化为等权。
        weight_total = sum(max(v.contribution, 0.0) for v in views)
        if weight_total > 0:
            confidence = sum(v.signal_confidence * max(v.contribution, 0.0) for v in views) / weight_total
        else:
            confidence = sum(v.signal_confidence for v in views) / len(views)
        distinct_sources = sorted({v.envelope.source for v in views})

        flags: list[str] = []
        if confidence < config.thresholds["min_confidence"]:
            flags.append("low_confidence")
        if len(distinct_sources) < config.thresholds["min_distinct_sources"]:
            flags.append("insufficient_sources")
        if not classification.categorized:
            flags.append("uncategorized")
        if not topic_active[topic]:
            flags.append("cooling")
        if any(not v.known_source for v in views):
            flags.append("unknown_source")
        if any(not v.has_metrics for v in views):
            flags.append("evidence_without_metrics")
        if any(v.late for v in views):
            flags.append("contains_late_arrival")
        if newcomer:
            flags.append("newcomer")

        downgrade = 1.0
        for flag, multiplier in config.downgrade_multipliers.items():
            if flag in flags:
                downgrade *= multiplier

        w = config.score_weights
        positive = (
            w.get("heat", 0) * heat_score
            + w.get("velocity", 0) * velocity_score
            + w.get("audience_fit", 0) * classification.audience_fit
        )
        base_score = positive - w.get("competition_density", 0) * density_score
        score = base_score * downgrade

        decision = latest.get(topic)
        feedback_action = decision["action"] if decision else None
        if decision is not None:
            score += config.feedback_adjustments.get(decision["action"], 0.0)
        final_score = max(0.0, score)

        evidence = [
            _evidence(
                v,
                in_heat=v.envelope.occurred_at > heat_start,
                in_velocity_now=vel_start < v.envelope.occurred_at <= as_of,
                in_velocity_prev=prev_start < v.envelope.occurred_at <= vel_start,
            )
            for v in views
        ]
        signals_in_window = sum(1 for e in evidence if "heat" in e["windows"])

        reasons = _reasons(
            heat_raw=heat_raw, heat_hours=heat_hours, vel_now=vel_now, vel_prev=vel_prev,
            newcomer=newcomer, distinct_sources=distinct_sources, classification=classification,
            competitors=competitors, confidence=confidence, flags=flags,
            feedback_action=feedback_action, signals_in_window=signals_in_window,
        )

        candidates.append({
            "topic": topic,
            "category": classification.category_name,
            "matched_keywords": list(classification.matched_keywords),
            "audience_fit": round(classification.audience_fit, 4),
            "heat": {"raw": round(heat_raw, 6), "score": round(heat_score, 6), "window_hours": heat_hours},
            "velocity": {
                "raw_ratio": round(velocity_ratio, 6),
                "score": round(velocity_score, 6),
                "window_hours": vel_hours,
                "current_window": round(vel_now, 6),
                "previous_window": round(vel_prev, 6),
                "newcomer": newcomer,
            },
            "competition": {
                "density_score": round(density_score, 6),
                "active_competitors": len(competitors),
                "competitor_topics": competitors,
            },
            "confidence": round(confidence, 4),
            "distinct_sources": distinct_sources,
            "flags": flags,
            "downgrade_multiplier": round(downgrade, 4),
            "feedback": None if decision is None else {
                "action": decision["action"],
                "reason": decision.get("reason"),
                "editor": decision.get("editor"),
                "decided_at": decision["decided_at"].isoformat()
                if isinstance(decision["decided_at"], datetime) else decision["decided_at"],
                "adjustment": config.feedback_adjustments.get(decision["action"], 0.0),
            },
            "signals_in_heat_window": signals_in_window,
            "score_components": {
                "heat": round(w.get("heat", 0) * heat_score, 6),
                "velocity": round(w.get("velocity", 0) * velocity_score, 6),
                "audience_fit": round(w.get("audience_fit", 0) * classification.audience_fit, 6),
                "competition_density": round(-w.get("competition_density", 0) * density_score, 6),
                "downgrade_multiplier": round(downgrade, 4),
                "feedback_adjustment": 0.0
                if decision is None else round(config.feedback_adjustments.get(decision["action"], 0.0), 6),
            },
            "final_score": round(final_score, 6),
            "why_rising": reasons,
            "evidence": evidence,
        })

    candidates.sort(key=lambda c: (-c["final_score"], c["topic"]))
    for rank, c in enumerate(candidates, start=1):
        c["rank"] = rank

    return {
        "as_of": as_of.isoformat(),
        "window_semantics": {
            "visibility": "received_at <= as_of 且 occurred_at <= as_of",
            "heat_window": f"(as_of-{heat_hours}h, as_of]，按 occurred_at 切分",
            "velocity_window": f"对比 (as_of-{2 * vel_hours}h, as_of-{vel_hours}h] 与 (as_of-{vel_hours}h, as_of]",
        },
        "totals": {
            "visible_signals": len(visible),
            "candidates": len(candidates),
            "excluded_late": len(excluded_late),
            "excluded_future_event": len(excluded_future),
        },
        "candidates": candidates,
        "excluded_late": excluded_late,
        "excluded_future_event": excluded_future,
    }


def _reasons(*, heat_raw, heat_hours, vel_now, vel_prev, newcomer, distinct_sources,
             classification, competitors, confidence, flags, feedback_action,
             signals_in_window) -> list[str]:
    reasons: list[str] = []
    if newcomer:
        reasons.append(f"题材在最近 {heat_hours} 小时窗口内首次出现，按新题材增速估计")
    elif vel_prev > 0:
        pct = round((vel_now - vel_prev) / vel_prev * 100, 1)
        if pct >= 0:
            reasons.append(f"热度贡献环比上一窗口增长 {pct}%（{round(vel_prev, 2)} → {round(vel_now, 2)}）")
        else:
            reasons.append(f"热度贡献环比上一窗口下降 {abs(pct)}%（{round(vel_prev, 2)} → {round(vel_now, 2)}）")
    reasons.append(f"近 {heat_hours} 小时热度贡献 {round(heat_raw, 2)}，窗口内证据 {signals_in_window} 条")
    reasons.append(f"证据来自 {len(distinct_sources)} 个来源：{'、'.join(distinct_sources)}")
    if classification.categorized:
        reasons.append(
            f"命中分类「{classification.category_name}」（关键词：{'、'.join(classification.matched_keywords)}），"
            f"受众契合度 {classification.audience_fit}"
        )
    if competitors:
        reasons.append(f"同分类活跃竞品 {len(competitors)} 个：{'、'.join(competitors)}")
    else:
        reasons.append("同分类暂无活跃竞品，竞争密度低")
    reasons.append(f"综合置信度 {round(confidence, 2)}")
    flag_text = {
        "low_confidence": "置信度低于阈值，已降级",
        "insufficient_sources": "独立来源数不足，已降级",
        "uncategorized": "未命中分类词表，按未分类契合度处理并降级",
        "cooling": "热度窗口内无新信号，题材正在冷却，已降级",
        "unknown_source": "存在配置外来源，按默认来源权重处理并降级提示",
        "evidence_without_metrics": "部分证据缺少可计分指标",
        "contains_late_arrival": "证据中含延迟送达信号",
        "newcomer": None,
    }
    for flag in flags:
        text = flag_text.get(flag)
        if text:
            reasons.append(f"【降级】{text}")
    if feedback_action == "follow":
        reasons.append("编辑标记跟进，排序获得人工加权")
    elif feedback_action == "shelve":
        reasons.append("编辑标记搁置，排序被人工下调")
    elif feedback_action == "misjudge":
        reasons.append("编辑标记误判，排序被显著下调")
    return reasons


def diff_versions(old: dict[str, Any], new: dict[str, Any]) -> dict[str, Any]:
    """相邻版本对比：名次、分数、证据集合与降级标记的变化。"""
    old_map = {c["topic"]: c for c in old["snapshot"]["candidates"]}
    new_map = {c["topic"]: c for c in new["snapshot"]["candidates"]}
    changes = []
    for topic in sorted(set(old_map) | set(new_map)):
        before, after = old_map.get(topic), new_map.get(topic)
        if before is None:
            changes.append({"topic": topic, "change": "appeared", "rank_after": after["rank"],
                            "score_after": after["final_score"]})
            continue
        if after is None:
            changes.append({"topic": topic, "change": "disappeared", "rank_before": before["rank"],
                            "score_before": before["final_score"]})
            continue
        before_ids = {e["event_id"] for e in before["evidence"]}
        after_ids = {e["event_id"] for e in after["evidence"]}
        added = [e for e in after["evidence"] if e["event_id"] in after_ids - before_ids]
        removed = [e for e in before["evidence"] if e["event_id"] in before_ids - after_ids]
        flags_before, flags_after = set(before["flags"]), set(after["flags"])
        changes.append({
            "topic": topic,
            "change": "changed" if before["rank"] != after["rank"] or before["final_score"] != after["final_score"]
            or added or removed or flags_before != flags_after else "unchanged",
            "rank_before": before["rank"],
            "rank_after": after["rank"],
            "rank_delta": before["rank"] - after["rank"],
            "score_before": before["final_score"],
            "score_after": after["final_score"],
            "score_delta": round(after["final_score"] - before["final_score"], 6),
            "evidence_added": [{"event_id": e["event_id"], "source": e["source"]} for e in added],
            "evidence_removed": [{"event_id": e["event_id"], "source": e["source"]} for e in removed],
            "flags_added": sorted(flags_after - flags_before),
            "flags_removed": sorted(flags_before - flags_after),
        })
    return {
        "from_version": old["version_id"],
        "from_as_of": old["as_of"],
        "to_version": new["version_id"],
        "to_as_of": new["as_of"],
        "changes": changes,
        "summary": {
            "appeared": sum(1 for c in changes if c["change"] == "appeared"),
            "disappeared": sum(1 for c in changes if c["change"] == "disappeared"),
            "changed": sum(1 for c in changes if c["change"] == "changed"),
            "unchanged": sum(1 for c in changes if c["change"] == "unchanged"),
        },
    }
