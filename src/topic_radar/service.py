"""雷达服务：把持久化、评分引擎、词表编排成用例。

* :meth:`preview` 只看不存；:meth:`save_version` 把同一计算结果整盘冻结。
* 版本号由 ``as_of + 配置 + 快照`` 内容寻址，重复保存同一时刻直接报冲突，
  晚到数据只能产生新的 ``as_of`` 版本。
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any

from .config import RadarConfig
from .contracts import SignalEnvelope
from .repository import Repository
from .scoring import build_radar, diff_versions
from .taxonomy import Taxonomy
from .timeutil import now_utc, parse_time


def canonical_dumps(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


class RadarService:
    def __init__(self, repository: Repository, config: RadarConfig, taxonomy: Taxonomy):
        self.repo = repository
        self.config = config
        self.taxonomy = taxonomy

    # -- 摄入 --------------------------------------------------------------

    def ingest_rows(self, rows: list[dict[str, Any]]) -> dict[str, Any]:
        results = []
        errors = []
        for index, raw in enumerate(rows):
            try:
                envelope = SignalEnvelope.from_dict(raw)
            except (ValueError, TypeError) as exc:
                errors.append({"index": index, "error": str(exc)})
                continue
            results.append(self.repo.upsert_signal(envelope, raw))
        status_counts: dict[str, int] = {}
        for r in results:
            status_counts[r["status"]] = status_counts.get(r["status"], 0) + 1
        return {"ingested": len(results), "errors": errors, "status_counts": status_counts, "results": results}

    # -- 计算 --------------------------------------------------------------

    def _snapshot(self, as_of: datetime) -> dict[str, Any]:
        signals = [s.to_envelope() for s in self.repo.list_signals()]
        decisions = self.repo.list_decisions()
        radar = build_radar(as_of, signals, decisions, self.config, self.taxonomy)
        taxonomy_snapshot = [
            {
                "name": c.name,
                "audience_fit": c.audience_fit,
                "keywords": list(c.keywords),
                "note": c.note,
            }
            for c in self.taxonomy._categories
        ]
        snapshot = {
            "as_of": radar["as_of"],
            "config": json.loads(self.config.canonical_json()),
            "taxonomy": taxonomy_snapshot,
            **radar,
        }
        return snapshot

    def preview(self, as_of: datetime | None = None) -> dict[str, Any]:
        """实时预览（不固化）：永远基于当前库状态计算。"""
        return self._snapshot(as_of or now_utc())

    def save_version(self, as_of: datetime | None = None, label: str | None = None) -> dict[str, Any]:
        as_of = as_of or now_utc()
        snapshot = self._snapshot(as_of)
        version_id = "v-" + hashlib.sha256(
            canonical_dumps({"as_of": snapshot["as_of"], "snapshot": snapshot}).encode("utf-8")
        ).hexdigest()[:12]
        self.repo.save_version(
            version_id=version_id,
            as_of=as_of,
            label=label,
            config_json=self.config.canonical_json(),
            snapshot_json=canonical_dumps(snapshot),
        )
        return {"version_id": version_id, "as_of": snapshot["as_of"], "label": label,
                "candidates": len(snapshot["candidates"])}

    # -- 读取 --------------------------------------------------------------

    def list_versions(self) -> list[dict[str, Any]]:
        return self.repo.list_versions()

    def get_version(self, version_id: str) -> dict[str, Any] | None:
        return self.repo.get_version(version_id)

    def latest_version(self) -> dict[str, Any] | None:
        return self.repo.latest_version()

    def diff_versions(self, version_id: str, from_version_id: str | None = None) -> dict[str, Any]:
        new = self.repo.get_version(version_id)
        if new is None:
            raise KeyError(version_id)
        versions = self.repo.list_versions()
        idx = next((i for i, v in enumerate(versions) if v["version_id"] == version_id), None)
        if from_version_id is None:
            if idx == 0:
                return {"from_version": None, "to_version": version_id,
                        "changes": [], "summary": {"appeared": 0, "disappeared": 0, "changed": 0, "unchanged": 0},
                        "note": "该版本之前没有已保存版本，无可对比的相邻版本"}
            old = self.repo.get_version(versions[idx - 1]["version_id"])
        else:
            old = self.repo.get_version(from_version_id)
            if old is None:
                raise KeyError(from_version_id)
        return diff_versions(old, new)

    def candidate_detail(self, topic: str) -> dict[str, Any]:
        """候选的完整身世：为何上升、用了哪些证据、相邻版本变化、人工决策。"""
        versions = self.repo.list_versions()
        timeline: list[dict[str, Any]] = []
        per_version: list[tuple[dict[str, Any], dict[str, Any] | None]] = []
        for meta in versions:
            record = self.repo.get_version(meta["version_id"])
            candidate = next((c for c in record["snapshot"]["candidates"] if c["topic"] == topic), None)
            if candidate is not None:
                timeline.append({
                    "version_id": record["version_id"],
                    "as_of": record["as_of"],
                    "label": record["label"],
                    "rank": candidate["rank"],
                    "final_score": candidate["final_score"],
                    "flags": candidate["flags"],
                })
                per_version.append((record, candidate))

        changes: list[dict[str, Any]] = []
        for (old_record, _), (new_record, new_candidate) in zip(per_version, per_version[1:]):
            full_diff = diff_versions(old_record, new_record)
            entry = next((c for c in full_diff["changes"] if c["topic"] == topic), None)
            if entry and entry["change"] != "unchanged":
                changes.append(entry)

        decisions = [d for d in self.repo.list_decisions() if d["topic"] == topic]
        latest_record, latest_candidate = per_version[-1] if per_version else (None, None)
        return {
            "topic": topic,
            "present_in_versions": len(timeline),
            "latest_version": latest_record["version_id"] if latest_record else None,
            "latest": latest_candidate,
            "timeline": timeline,
            "adjacent_changes": changes,
            "decisions": decisions,
        }

    # -- 人工决策 -----------------------------------------------------------

    def record_decision(self, topic: str, action: str, reason: str | None = None,
                        editor: str | None = None, decided_at: datetime | None = None,
                        version_id: str | None = None) -> dict[str, Any]:
        from .scoring import VALID_ACTIONS

        if action not in VALID_ACTIONS:
            raise ValueError(f"action 必须是 {VALID_ACTIONS} 之一")
        if not reason or not reason.strip():
            raise ValueError("决策必须保留理由（reason）")
        return self.repo.append_decision(
            topic=topic, action=action, reason=reason.strip(), editor=editor,
            decided_at=decided_at or now_utc(), version_id=version_id,
        )

    def duplicates(self) -> list[dict[str, Any]]:
        return self.repo.list_duplicates()
