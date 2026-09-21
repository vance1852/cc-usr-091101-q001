"""SQLite 持久化。

两类表，两种生命周期：

* 只追加/封存表（signal_arrival、dedup_winner、board_version、
  board_candidate、candidate_evidence）：由触发器禁止 UPDATE/DELETE，
  保存的榜单与去重结果在服务重启后逐字节一致；
* 反馈表（editor_feedback）：同样只追加，人工决策永不被覆盖或删除，
  其作用是修改 *未来* 版本的排序分，旧版本快照不受影响。
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from .ingest import DedupResult

SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS signal_arrival (
    event_id      TEXT NOT NULL,
    source        TEXT NOT NULL,
    topic         TEXT NOT NULL,
    occurred_at   TEXT NOT NULL,
    received_at   TEXT NOT NULL,
    metrics_json  TEXT NOT NULL,
    attributes_json TEXT NOT NULL,
    PRIMARY KEY (event_id, source, received_at)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS dedup_winner (
    event_id           TEXT NOT NULL,
    source             TEXT NOT NULL,
    winner_received_at TEXT NOT NULL,
    arrival_count      INTEGER NOT NULL,
    metrics_conflict   INTEGER NOT NULL,
    PRIMARY KEY (event_id, source)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS board_version (
    version_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    cutoff_at    TEXT NOT NULL UNIQUE,
    window_hours REAL NOT NULL,
    created_at   TEXT NOT NULL,
    config_json  TEXT NOT NULL,
    note         TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS board_candidate (
    version_id   INTEGER NOT NULL REFERENCES board_version(version_id),
    rank         INTEGER NOT NULL,
    topic        TEXT NOT NULL,
    breakdown_json TEXT NOT NULL,
    PRIMARY KEY (version_id, topic)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS candidate_evidence (
    version_id INTEGER NOT NULL,
    topic      TEXT NOT NULL,
    event_id   TEXT NOT NULL,
    source     TEXT NOT NULL,
    bucket     TEXT NOT NULL,
    PRIMARY KEY (version_id, topic, event_id, source)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS editor_feedback (
    feedback_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    topic        TEXT NOT NULL,
    decision     TEXT NOT NULL CHECK (decision IN ('follow_up', 'shelve', 'misjudge')),
    reason       TEXT NOT NULL DEFAULT '',
    editor       TEXT NOT NULL DEFAULT '',
    created_at   TEXT NOT NULL,
    applied_from_version INTEGER
);

CREATE INDEX IF NOT EXISTS idx_feedback_topic ON editor_feedback(topic, feedback_id);

-- 不可篡改护栏：任何对封存数据的改写/删除都直接失败。
CREATE TRIGGER IF NOT EXISTS trg_board_version_frozen
BEFORE UPDATE ON board_version BEGIN
    SELECT RAISE(ABORT, '榜单版本一经保存即不可改写');
END;
CREATE TRIGGER IF NOT EXISTS trg_board_version_no_delete
BEFORE DELETE ON board_version BEGIN
    SELECT RAISE(ABORT, '榜单版本一经保存即不可删除');
END;
CREATE TRIGGER IF NOT EXISTS trg_candidate_frozen
BEFORE UPDATE ON board_candidate BEGIN
    SELECT RAISE(ABORT, '候选快照不可改写');
END;
CREATE TRIGGER IF NOT EXISTS trg_candidate_no_delete
BEFORE DELETE ON board_candidate BEGIN
    SELECT RAISE(ABORT, '候选快照不可删除');
END;
CREATE TRIGGER IF NOT EXISTS trg_evidence_frozen
BEFORE UPDATE ON candidate_evidence BEGIN
    SELECT RAISE(ABORT, '证据快照不可改写');
END;
CREATE TRIGGER IF NOT EXISTS trg_evidence_no_delete
BEFORE DELETE ON candidate_evidence BEGIN
    SELECT RAISE(ABORT, '证据快照不可删除');
END;
CREATE TRIGGER IF NOT EXISTS trg_arrival_no_mutate
BEFORE UPDATE ON signal_arrival BEGIN
    SELECT RAISE(ABORT, '原始到达记录不可改写');
END;
CREATE TRIGGER IF NOT EXISTS trg_winner_no_mutate
BEFORE UPDATE ON dedup_winner BEGIN
    SELECT RAISE(ABORT, '去重结论不可改写');
END;
CREATE TRIGGER IF NOT EXISTS trg_feedback_no_mutate
BEFORE UPDATE ON editor_feedback BEGIN
    SELECT RAISE(ABORT, '编辑反馈只可追加');
END;
CREATE TRIGGER IF NOT EXISTS trg_feedback_no_delete
BEFORE DELETE ON editor_feedback BEGIN
    SELECT RAISE(ABORT, '编辑反馈不可删除');
END;
"""


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class RadarStore:
    def __init__(self, path: str | Path = "radar.db"):
        self.path = str(path)
        # API 以线程方式处理并发请求；WAL + busy_timeout 保证单写入者下安全等待。
        self.conn = sqlite3.connect(self.path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA busy_timeout=5000")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.executescript(SCHEMA)
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> "RadarStore":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ---------- 信号与去重 ----------

    def persist_deduped(self, results: Iterable[DedupResult]) -> tuple[int, int]:
        """写入全部原始到达与去重结论。返回 (新增到达数, 跳过的重复到达数)。

        去重结论首次落库后即冻结：即使再次喂入同一 event_id 的另一组投递，
        也不会改写既有 winner（重启后与首次入库完全一致）。
        """
        new_arrivals = 0
        duplicate_arrivals = 0
        for result in results:
            for arrival in result.arrivals:
                cur = self.conn.execute(
                    "INSERT OR IGNORE INTO signal_arrival "
                    "(event_id, source, topic, occurred_at, received_at, metrics_json, attributes_json) "
                    "VALUES (?,?,?,?,?,?,?)",
                    (
                        arrival.event_id,
                        arrival.source,
                        arrival.topic,
                        arrival.occurred_at.isoformat(),
                        arrival.received_at.isoformat(),
                        json.dumps(arrival.metrics, ensure_ascii=False, sort_keys=True),
                        json.dumps(arrival.attributes, ensure_ascii=False, sort_keys=True),
                    ),
                )
                if cur.rowcount == 1:
                    new_arrivals += 1
                else:
                    duplicate_arrivals += 1
            frozen = self.conn.execute(
                "SELECT 1 FROM dedup_winner WHERE event_id=? AND source=?",
                (result.signal.event_id, result.signal.source),
            ).fetchone()
            if frozen:
                continue
            self.conn.execute(
                "INSERT INTO dedup_winner (event_id, source, winner_received_at, arrival_count, metrics_conflict) "
                "VALUES (?,?,?,?,?)",
                (
                    result.signal.event_id,
                    result.signal.source,
                    result.signal.received_at.isoformat(),
                    len(result.arrivals),
                    1 if result.metrics_conflict else 0,
                ),
            )
        self.conn.commit()
        return new_arrivals, duplicate_arrivals

    def load_deduped(self) -> list[DedupResult]:
        """从冻结记录重建去重结果，供评分使用。

        winner 以 ``dedup_winner`` 冻结结论为准；同一 received_at 平手时
        复用 ingest 的规范 JSON 字典序，保证重建结果与首次入库一致。
        """
        from .contracts import SignalEnvelope
        from .ingest import _canonical

        winners = {
            (r["event_id"], r["source"]): r
            for r in self.conn.execute("SELECT * FROM dedup_winner")
        }
        grouped: dict[tuple[str, str], list[SignalEnvelope]] = {}
        for row in self.conn.execute("SELECT * FROM signal_arrival"):
            sig = SignalEnvelope(
                event_id=row["event_id"],
                source=row["source"],
                topic=row["topic"],
                occurred_at=datetime.fromisoformat(row["occurred_at"]),
                received_at=datetime.fromisoformat(row["received_at"]),
                metrics=json.loads(row["metrics_json"]),
                attributes=json.loads(row["attributes_json"]),
            )
            grouped.setdefault((row["event_id"], row["source"]), []).append(sig)

        results: list[DedupResult] = []
        for key, arrivals in grouped.items():
            w = winners[key]
            winner_received = w["winner_received_at"]
            ordered = sorted(arrivals, key=lambda s: (s.received_at, _canonical(s)))
            winner = next(
                s for s in ordered if s.received_at.isoformat() == winner_received
            )
            results.append(DedupResult(winner, tuple(ordered), bool(w["metrics_conflict"])))
        results.sort(key=lambda r: (r.signal.occurred_at, r.signal.source, r.signal.event_id))
        return results

    def arrival_count(self) -> int:
        return self.conn.execute("SELECT COUNT(*) FROM signal_arrival").fetchone()[0]

    # ---------- 版本封存 ----------

    def version_exists(self, cutoff_iso: str) -> bool:
        return bool(
            self.conn.execute("SELECT 1 FROM board_version WHERE cutoff_at=?", (cutoff_iso,)).fetchone()
        )

    def insert_version_row(self, cutoff_iso: str, window_hours: float, config_json: str, note: str = "") -> int:
        cur = self.conn.execute(
            "INSERT INTO board_version (cutoff_at, window_hours, created_at, config_json, note) "
            "VALUES (?,?,?,?,?)",
            (cutoff_iso, window_hours, utc_now_iso(), config_json, note),
        )
        return int(cur.lastrowid)

    def insert_candidate_snapshot(self, version_id: int, rank: int, topic: str, breakdown: dict[str, Any], evidence: list[dict[str, Any]]) -> None:
        self.conn.execute(
            "INSERT INTO board_candidate (version_id, rank, topic, breakdown_json) VALUES (?,?,?,?)",
            (version_id, rank, topic, json.dumps(breakdown, ensure_ascii=False, sort_keys=True)),
        )
        for ev in evidence:
            self.conn.execute(
                "INSERT INTO candidate_evidence (version_id, topic, event_id, source, bucket) "
                "VALUES (?,?,?,?,?)",
                (version_id, topic, ev["event_id"], ev["source"], ev["bucket"]),
            )

    def list_versions(self) -> list[sqlite3.Row]:
        return list(
            self.conn.execute(
                "SELECT version_id, cutoff_at, window_hours, created_at, note "
                "FROM board_version ORDER BY version_id"
            )
        )

    def get_version_meta(self, version_id: int) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM board_version WHERE version_id=?", (version_id,)
        ).fetchone()

    def load_candidates(self, version_id: int) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT rank, topic, breakdown_json FROM board_candidate WHERE version_id=? ORDER BY rank",
            (version_id,),
        )
        return [
            {"rank": r["rank"], "topic": r["topic"], "breakdown": json.loads(r["breakdown_json"])}
            for r in rows
        ]

    def load_evidence_refs(self, version_id: int, topic: str) -> list[sqlite3.Row]:
        return list(
            self.conn.execute(
                "SELECT event_id, source, bucket FROM candidate_evidence "
                "WHERE version_id=? AND topic=? ORDER BY event_id",
                (version_id, topic),
            )
        )

    def get_arrival(self, event_id: str, source: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM signal_arrival WHERE event_id=? AND source=? "
            "ORDER BY received_at LIMIT 1",
            (event_id, source),
        ).fetchone()

    # ---------- 编辑反馈 ----------

    def add_feedback(
        self,
        topic: str,
        decision: str,
        reason: str,
        editor: str,
        applied_from_version: int | None,
        created_at: str | None = None,
    ) -> int:
        cur = self.conn.execute(
            "INSERT INTO editor_feedback (topic, decision, reason, editor, created_at, applied_from_version) "
            "VALUES (?,?,?,?,?,?)",
            (topic, decision, reason, editor, created_at or utc_now_iso(), applied_from_version),
        )
        self.conn.commit()
        return int(cur.lastrowid)

    def list_feedback(self, topic: str | None = None) -> list[sqlite3.Row]:
        if topic is None:
            return list(self.conn.execute("SELECT * FROM editor_feedback ORDER BY feedback_id"))
        return list(
            self.conn.execute(
                "SELECT * FROM editor_feedback WHERE topic=? ORDER BY feedback_id", (topic,)
            )
        )
