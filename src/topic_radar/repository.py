"""SQLite 持久化：原始信号、同源去重台账、冻结版本、追加式人工决策。

设计约束：
* canonical 信号按 ``(source, event_id)`` 唯一，迟到的重复投递进 ``duplicates``；
  取 ``received_at`` 最早（再以内容哈希兜底）的副本为正本，与摄入顺序无关，
  因此重启或重新跑乱序样例后去重结果一致。
* 版本快照整盘 JSON 落库，只增不改，保存后任何新信号、新决策都无法改写它。
* 决策只追加，不更新、不删除，历史排序读取决策时按 ``as_of`` 截断时间。
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from datetime import datetime
from pathlib import Path
from typing import Any

from .contracts import SignalEnvelope
from .timeutil import now_utc, parse_time, to_utc_iso

_SCHEMA = """
CREATE TABLE IF NOT EXISTS signals (
    source        TEXT NOT NULL,
    event_id      TEXT NOT NULL,
    topic         TEXT NOT NULL,
    occurred_at   TEXT NOT NULL,
    received_at   TEXT NOT NULL,
    metrics_json  TEXT NOT NULL,
    attrs_json    TEXT NOT NULL,
    raw_json      TEXT NOT NULL,
    first_seen_at TEXT NOT NULL,
    PRIMARY KEY (source, event_id)
);
CREATE TABLE IF NOT EXISTS duplicates (
    source        TEXT NOT NULL,
    event_id      TEXT NOT NULL,
    seq           INTEGER NOT NULL,
    received_at   TEXT NOT NULL,
    note          TEXT NOT NULL,
    raw_json      TEXT NOT NULL,
    first_seen_at TEXT NOT NULL,
    PRIMARY KEY (source, event_id, seq)
);
CREATE TABLE IF NOT EXISTS versions (
    version_id    TEXT PRIMARY KEY,
    as_of         TEXT NOT NULL UNIQUE,
    created_at    TEXT NOT NULL,
    label         TEXT,
    config_json   TEXT NOT NULL,
    snapshot_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS decisions (
    topic      TEXT NOT NULL,
    action     TEXT NOT NULL,
    reason     TEXT,
    editor     TEXT,
    decided_at TEXT NOT NULL,
    version_id TEXT,
    PRIMARY KEY (topic, decided_at)
);
"""


def content_hash(envelope: SignalEnvelope) -> str:
    payload = json.dumps(
        {
            "occurred_at": to_utc_iso(envelope.occurred_at),
            "metrics": envelope.metrics,
            "attributes": envelope.attributes,
            "topic": envelope.topic,
        },
        sort_keys=True,
        ensure_ascii=False,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


class StoredSignal:
    def __init__(self, row: sqlite3.Row):
        self.source = row["source"]
        self.event_id = row["event_id"]
        self.topic = row["topic"]
        self.occurred_at = parse_time(row["occurred_at"])
        self.received_at = parse_time(row["received_at"])
        self.metrics = json.loads(row["metrics_json"])
        self.attributes = json.loads(row["attrs_json"])
        self.first_seen_at = parse_time(row["first_seen_at"])

    def to_envelope(self) -> SignalEnvelope:
        return SignalEnvelope(
            event_id=self.event_id,
            source=self.source,
            topic=self.topic,
            occurred_at=self.occurred_at,
            received_at=self.received_at,
            metrics=dict(self.metrics),
            attributes=dict(self.attributes),
        )


class VersionExistsError(ValueError):
    pass


class Repository:
    def __init__(self, path: str | Path):
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(self.path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # -- 摄入 / 去重 -------------------------------------------------------

    def upsert_signal(self, envelope: SignalEnvelope, raw: dict[str, Any]) -> dict[str, Any]:
        """合并同源重复，返回本次摄入结果说明。"""
        with self._lock:
            cur = self._conn.execute(
                "SELECT * FROM signals WHERE source=? AND event_id=?",
                (envelope.source, envelope.event_id),
            )
            existing = cur.fetchone()
            now_iso = to_utc_iso(now_utc())
            raw_text = json.dumps(raw, ensure_ascii=False, sort_keys=True)
            if existing is None:
                self._conn.execute(
                    "INSERT INTO signals VALUES (?,?,?,?,?,?,?,?,?)",
                    (
                        envelope.source,
                        envelope.event_id,
                        envelope.topic,
                        to_utc_iso(envelope.occurred_at),
                        to_utc_iso(envelope.received_at),
                        json.dumps(envelope.metrics, ensure_ascii=False, sort_keys=True),
                        json.dumps(envelope.attributes, ensure_ascii=False, sort_keys=True),
                        raw_text,
                        now_iso,
                    ),
                )
                self._conn.commit()
                return {"status": "inserted", "source": envelope.source, "event_id": envelope.event_id}

            incoming_key = (envelope.received_at, content_hash(envelope))
            existing_key = (parse_time(existing["received_at"]), content_hash(StoredSignal(existing).to_envelope()))
            self._record_duplicate(envelope, raw_text, now_iso, "redelivery")
            result = "duplicate"
            if incoming_key < existing_key:
                # 乱序到达：更早 received_at 的副本应成为正本，旧正本降级留痕。
                self._conn.execute(
                    "UPDATE signals SET topic=?, occurred_at=?, received_at=?, metrics_json=?, attrs_json=?, raw_json=? "
                    "WHERE source=? AND event_id=?",
                    (
                        envelope.topic,
                        to_utc_iso(envelope.occurred_at),
                        to_utc_iso(envelope.received_at),
                        json.dumps(envelope.metrics, ensure_ascii=False, sort_keys=True),
                        json.dumps(envelope.attributes, ensure_ascii=False, sort_keys=True),
                        raw_text,
                        envelope.source,
                        envelope.event_id,
                    ),
                )
                old_raw = existing["raw_json"]
                self._record_duplicate(
                    StoredSignal(existing).to_envelope(), old_raw, now_iso, "superseded-canonical"
                )
                result = "superseded"
            self._conn.commit()
            return {"status": result, "source": envelope.source, "event_id": envelope.event_id}

    def _record_duplicate(self, envelope: SignalEnvelope, raw_text: str, seen_iso: str, note: str) -> None:
        seq_row = self._conn.execute(
            "SELECT COALESCE(MAX(seq), 0) + 1 AS next_seq FROM duplicates WHERE source=? AND event_id=?",
            (envelope.source, envelope.event_id),
        ).fetchone()
        self._conn.execute(
            "INSERT OR REPLACE INTO duplicates VALUES (?,?,?,?,?,?,?)",
            (
                envelope.source,
                envelope.event_id,
                seq_row["next_seq"],
                to_utc_iso(envelope.received_at),
                note,
                raw_text,
                seen_iso,
            ),
        )

    def list_signals(self) -> list[StoredSignal]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM signals ORDER BY received_at, source, event_id"
            ).fetchall()
            return [StoredSignal(r) for r in rows]

    def list_duplicates(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT source, event_id, seq, received_at, note, first_seen_at "
                "FROM duplicates ORDER BY source, event_id, seq"
            ).fetchall()
            return [dict(r) for r in rows]

    # -- 版本 --------------------------------------------------------------

    def save_version(self, version_id: str, as_of: datetime, label: str | None,
                     config_json: str, snapshot_json: str) -> None:
        with self._lock:
            try:
                self._conn.execute(
                    "INSERT INTO versions (version_id, as_of, created_at, label, config_json, snapshot_json) "
                    "VALUES (?,?,?,?,?,?)",
                    (version_id, to_utc_iso(as_of), to_utc_iso(now_utc()), label, config_json, snapshot_json),
                )
                self._conn.commit()
            except sqlite3.IntegrityError as exc:
                raise VersionExistsError(f"as_of={to_utc_iso(as_of)} 的榜单版本已存在，不可改写") from exc

    def get_version(self, version_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute("SELECT * FROM versions WHERE version_id=?", (version_id,)).fetchone()
            if row is None:
                return None
            return {
                "version_id": row["version_id"],
                "as_of": row["as_of"],
                "created_at": row["created_at"],
                "label": row["label"],
                "config": json.loads(row["config_json"]),
                "snapshot": json.loads(row["snapshot_json"]),
            }

    def get_version_by_as_of(self, as_of: datetime) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM versions WHERE as_of=?", (to_utc_iso(as_of),)
            ).fetchone()
            if row is None:
                return None
            return self.get_version(row["version_id"])

    def list_versions(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT version_id, as_of, created_at, label FROM versions ORDER BY as_of"
            ).fetchall()
            return [dict(r) for r in rows]

    def latest_version(self) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT version_id FROM versions ORDER BY as_of DESC LIMIT 1"
            ).fetchone()
            return self.get_version(row["version_id"]) if row else None

    # -- 人工决策 -----------------------------------------------------------

    def append_decision(self, topic: str, action: str, reason: str | None,
                        editor: str | None, decided_at: datetime,
                        version_id: str | None) -> dict[str, Any]:
        with self._lock:
            self._conn.execute(
                "INSERT INTO decisions VALUES (?,?,?,?,?,?)",
                (topic, action, reason, editor, to_utc_iso(decided_at), version_id),
            )
            self._conn.commit()
            return {
                "topic": topic,
                "action": action,
                "reason": reason,
                "editor": editor,
                "decided_at": to_utc_iso(decided_at),
                "version_id": version_id,
            }

    def list_decisions(self, as_of: datetime | None = None) -> list[dict[str, Any]]:
        with self._lock:
            if as_of is None:
                rows = self._conn.execute(
                    "SELECT * FROM decisions ORDER BY decided_at"
                ).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT * FROM decisions WHERE decided_at<=? ORDER BY decided_at",
                    (to_utc_iso(as_of),),
                ).fetchall()
            return [dict(r) for r in rows]
