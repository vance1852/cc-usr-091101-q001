"""选题雷达应用服务：版本封存、反馈生效与版本间对比。

时间纪律同样适用于人工反馈：只有 ``created_at <= 版本 cutoff`` 且标注的
生效版本早于当前版本的反馈才参与该版本排序。保存后版本即冻结，
之后新增的反馈只影响后续版本。
"""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any

from .config import RadarConfig, Taxonomy
from .ingest import ingest
from .scoring import build_candidates
from .store import RadarStore

VALID_DECISIONS = ("follow_up", "shelve", "misjudge")


def parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("时间必须包含 UTC 偏移")
    return parsed


class RadarService:
    def __init__(self, store: RadarStore, cfg: RadarConfig, taxonomy: Taxonomy):
        self.store = store
        self.cfg = cfg
        self.taxonomy = taxonomy

    # ---------- 采集 ----------

    def ingest_rows(self, rows: list[dict]) -> dict[str, Any]:
        report = ingest(rows)
        new_arrivals, duplicate_arrivals = self.store.persist_deduped(report.accepted)
        return {
            "accepted_events": len(report.accepted),
            "new_arrivals": new_arrivals,
            "duplicate_arrivals_skipped": duplicate_arrivals,
            "rejected_rows": [
                {"row_index": idx, "excerpt": excerpt, "error": err}
                for idx, excerpt, err in report.rejected
            ],
        }

    def ingest_file(self, path: str | Path) -> dict[str, Any]:
        return self.ingest_rows(json.loads(Path(path).read_text(encoding="utf-8")))

    # ---------- 反馈 ----------

    def add_feedback(
        self,
        topic: str,
        decision: str,
        reason: str = "",
        editor: str = "",
        applied_from_version: int | None = None,
        created_at: datetime | None = None,
    ) -> int:
        if decision not in VALID_DECISIONS:
            raise ValueError(f"decision 必须是 {VALID_DECISIONS} 之一")
        if applied_from_version is not None:
            if self.store.get_version_meta(int(applied_from_version)) is None:
                raise ValueError("applied_from_version 指向不存在的版本")
        return self.store.add_feedback(
            topic=topic,
            decision=decision,
            reason=reason,
            editor=editor,
            applied_from_version=applied_from_version,
            created_at=created_at.isoformat() if created_at else None,
        )

    def _feedback_adjustments(self, cutoff: datetime, version_id: int) -> dict[str, float]:
        """每个题材取 cutoff 前最近一条有效反馈作为调整分。

        最近一条代表编辑的最新判断（先跟进后搁置，以搁置为准）；
        历史反馈仍完整保留在反馈表中，可在候选详情里追溯。
        """
        latest: dict[str, tuple[int, str]] = {}
        for row in self.store.list_feedback():
            created = parse_time(row["created_at"])
            if created > cutoff:
                continue
            applies_from = row["applied_from_version"]
            if applies_from is not None and int(applies_from) >= version_id:
                continue
            latest[row["topic"]] = (row["feedback_id"], row["decision"])
        return {
            topic: self.cfg.feedback_adjustments.get(decision, 0.0)
            for topic, (_id, decision) in latest.items()
        }

    # ---------- 版本 ----------

    def save_version(self, cutoff: datetime, note: str = "") -> dict[str, Any]:
        cutoff_iso = cutoff.isoformat()
        if self.store.version_exists(cutoff_iso):
            raise ValueError(f"cutoff={cutoff_iso} 的榜单已保存，版本不可改写")

        deduped = self.store.load_deduped()
        # 先建版本行拿到 version_id，反馈生效边界需要它；失败整体回滚。
        version_id = self.store.insert_version_row(
            cutoff_iso,
            self.cfg.window_hours,
            json.dumps(
                {"radar": asdict(self.cfg), "taxonomy": asdict(self.taxonomy)},
                ensure_ascii=False,
                sort_keys=True,
                default=_json_default,
            ),
            note,
        )
        try:
            adjustments = self._feedback_adjustments(cutoff, version_id)
            candidates = build_candidates(deduped, cutoff, self.cfg, self.taxonomy, adjustments)
            for rank, c in enumerate(candidates, start=1):
                self.store.insert_candidate_snapshot(
                    version_id,
                    rank,
                    c.topic,
                    asdict(c.breakdown),
                    [asdict(e) for e in c.evidence],
                )
            self.store.conn.commit()
        except Exception:
            self.store.conn.rollback()
            raise
        return {"version_id": version_id, "cutoff_at": cutoff_iso, "candidates": len(candidates)}

    def list_versions(self) -> list[dict[str, Any]]:
        return [dict(r) for r in self.store.list_versions()]

    def get_board(self, version_id: int) -> dict[str, Any]:
        meta = self.store.get_version_meta(version_id)
        if meta is None:
            raise KeyError(f"版本 {version_id} 不存在")
        return {
            "version_id": version_id,
            "cutoff_at": meta["cutoff_at"],
            "window_hours": meta["window_hours"],
            "created_at": meta["created_at"],
            "note": meta["note"],
            "candidates": self.store.load_candidates(version_id),
        }

    # ---------- 候选详情与可解释性 ----------

    def _evidence_detail(self, version_id: int, topic: str) -> list[dict[str, Any]]:
        details: list[dict[str, Any]] = []
        for ref in self.store.load_evidence_refs(version_id, topic):
            winner = self.store.conn.execute(
                "SELECT w.winner_received_at, w.arrival_count, w.metrics_conflict "
                "FROM dedup_winner w WHERE w.event_id=? AND w.source=?",
                (ref["event_id"], ref["source"]),
            ).fetchone()
            row = self.store.conn.execute(
                "SELECT * FROM signal_arrival WHERE event_id=? AND source=? AND received_at=?",
                (ref["event_id"], ref["source"], winner["winner_received_at"]),
            ).fetchone()
            metrics = json.loads(row["metrics_json"])
            attrs = json.loads(row["attributes_json"])
            occurred = parse_time(row["occurred_at"])
            received = parse_time(row["received_at"])
            details.append(
                {
                    "event_id": row["event_id"],
                    "source": row["source"],
                    "bucket": ref["bucket"],
                    "occurred_at": row["occurred_at"],
                    "received_at": row["received_at"],
                    "lag_seconds": (received - occurred).total_seconds(),
                    "metrics": metrics,
                    "attributes": attrs,
                    "duplicate_arrivals": winner["arrival_count"] - 1,
                    "metrics_conflict": bool(winner["metrics_conflict"]),
                }
            )
        details.sort(key=lambda d: (d["occurred_at"], d["received_at"], d["source"]))
        return details

    @staticmethod
    def _diff(base: dict[str, Any] | None, target: dict[str, Any] | None) -> dict[str, Any] | None:
        """两个版本中同一题材快照的差异。"""
        if base is None or target is None:
            return None
        b, t = base["breakdown"], target["breakdown"]
        return {
            "rank_change": base["rank"] - target["rank"],  # 正数=排名上升
            "score_change": round(t["final_score"] - b["final_score"], 4),
            "dimension_changes": {
                k: round(t[k] - b[k], 4)
                for k in ("heat", "momentum", "opportunity", "audience")
            },
            "degradation_added": [d for d in t["degradation"] if d not in b["degradation"]],
            "degradation_cleared": [d for d in b["degradation"] if d not in t["degradation"]],
            "feedback_adjustment_change": round(
                t["feedback_adjustment"] - b["feedback_adjustment"], 4
            ),
        }

    def get_candidate(self, version_id: int, topic: str) -> dict[str, Any]:
        board = self.get_board(version_id)
        snap = next((c for c in board["candidates"] if c["topic"] == topic), None)
        if snap is None:
            raise KeyError(f"版本 {version_id} 中没有题材 {topic}")

        versions = self.store.list_versions()
        ids = [v["version_id"] for v in versions]
        idx = ids.index(version_id)
        prev_id = ids[idx - 1] if idx > 0 else None
        next_id = ids[idx + 1] if idx + 1 < len(ids) else None

        def _snapshot(vid: int) -> dict[str, Any] | None:
            return next(
                (c for c in self.store.load_candidates(vid) if c["topic"] == topic), None
            )

        evidence_ids = {
            (e["event_id"], e["source"])
            for e in self.store.load_evidence_refs(version_id, topic)
        }
        prev_evidence_ids = (
            {(e["event_id"], e["source"]) for e in self.store.load_evidence_refs(prev_id, topic)}
            if prev_id
            else set()
        )
        evidence_changelog = {
            "added_vs_previous": [
                {"event_id": e, "source": s} for e, s in sorted(evidence_ids - prev_evidence_ids)
            ],
            "removed_vs_previous": [
                {"event_id": e, "source": s} for e, s in sorted(prev_evidence_ids - evidence_ids)
            ],
        }

        return {
            "topic": topic,
            "version_id": version_id,
            "rank": snap["rank"],
            "why_rising": _narrative(snap["breakdown"]),
            "breakdown": snap["breakdown"],
            "evidence": self._evidence_detail(version_id, topic),
            "evidence_changelog": evidence_changelog,
            "feedback_history": [dict(r) for r in self.store.list_feedback(topic)],
            "diff_previous_version": (
                {
                    "version_id": prev_id,
                    **(self._diff(_snapshot(prev_id), snap) or {}),
                }
                if prev_id and _snapshot(prev_id)
                else None
            ),
            "diff_next_version": (
                {
                    "version_id": next_id,
                    **(self._diff(snap, _snapshot(next_id)) or {}),
                }
                if next_id and _snapshot(next_id)
                else None
            ),
        }


def _narrative(b: dict[str, Any]) -> list[str]:
    """把分数拆解翻译成策划可读的上升/下降理由。"""
    lines = []
    if b["momentum"] >= 0.65:
        lines.append(f"增速强劲（增速分 {b['momentum']:.2f}），近窗需求速率明显高于前窗")
    elif b["momentum"] <= 0.35:
        lines.append(f"增速放缓（增速分 {b['momentum']:.2f}），近窗需求速率低于前窗")
    if b["heat"] >= 0.66:
        lines.append(f"窗内热度处于榜内高位（热度分 {b['heat']:.2f}，需求总量 {b['raw_demand_recent']:.0f}）")
    if b["opportunity"] >= 0.66:
        lines.append("竞争密度低，供给侧拥挤度小，机会窗口敞开")
    elif b["opportunity"] <= 0.34:
        lines.append(f"竞争密度高（在窗供给 {b['raw_supply_recent']:.0f}），需差异化切入")
    if b["audience"] >= 0.8:
        lines.append(f"与本工作室受众高度契合（{', '.join(b['categories']) or '未分类'}）")
    if b["feedback_adjustment"]:
        direction = "加分" if b["feedback_adjustment"] > 0 else "扣分"
        lines.append(f"编辑历史判断{direction} {b['feedback_adjustment']:+.1f}（仅影响本版排序，不改历史）")
    if b["degradation"]:
        lines.append("降级因素：" + "；".join(b["degradation"]))
    return lines


def _json_default(obj: Any) -> Any:
    if isinstance(obj, frozenset):
        return sorted(obj)
    raise TypeError(f"不可序列化的类型: {type(obj)!r}")
