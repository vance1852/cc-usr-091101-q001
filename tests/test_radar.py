import json
import os
import sqlite3
import tempfile
import threading
import unittest
import urllib.request
from datetime import datetime, timedelta, timezone
from http.server import ThreadingHTTPServer
from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from topic_radar.api import RadarAPI, _make_handler  # noqa: E402
from topic_radar.config import RadarConfig, Taxonomy  # noqa: E402
from topic_radar.ingest import ingest  # noqa: E402
from topic_radar.scoring import RECENT, PRIOR  # noqa: E402
from topic_radar.service import RadarService  # noqa: E402
from topic_radar.store import RadarStore  # noqa: E402

CST = timezone(timedelta(hours=8))


def t(value: str) -> datetime:
    return datetime.fromisoformat(value)


def env(eid, source, topic, occurred, received, metrics=None, **attrs):
    row = {
        "event_id": eid,
        "source": source,
        "topic": topic,
        "occurred_at": occurred,
        "received_at": received,
        "metrics": metrics or {"searches": 100},
    }
    row.update(attrs)
    return row


class ServiceCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "radar.db")
        self.store = RadarStore(self.db)
        self.service = RadarService(self.store, RadarConfig.load(), Taxonomy.load())

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def board(self, version_id):
        return {c["topic"]: c for c in self.service.get_board(version_id)["candidates"]}


class DedupTest(unittest.TestCase):
    def test_same_event_merged_earliest_arrival_wins(self):
        rows = [
            env("s1", "search", "逆袭短篇", "2026-09-10T13:05:00+08:00", "2026-09-10T13:07:10+08:00"),
            env("s1", "search", "逆袭短篇", "2026-09-10T13:05:00+08:00", "2026-09-10T13:05:04+08:00"),
        ]
        report = ingest(rows)
        self.assertEqual(len(report.accepted), 1)
        winner = report.accepted[0]
        self.assertEqual(winner.signal.received_at, t("2026-09-10T13:05:04+08:00"))
        self.assertEqual(winner.duplicate_count, 1)
        self.assertEqual(len(winner.arrivals), 2)

    def test_dedup_order_independent(self):
        a = env("s1", "search", "逆袭短篇", "2026-09-10T13:05:00+08:00", "2026-09-10T13:07:10+08:00")
        b = env("s1", "search", "逆袭短篇", "2026-09-10T13:05:00+08:00", "2026-09-10T13:05:04+08:00")
        r1 = ingest([a, b]).accepted[0].signal
        r2 = ingest([b, a]).accepted[0].signal
        self.assertEqual(r1, r2)

    def test_different_sources_are_distinct_events(self):
        rows = [
            env("s1", "search", "逆袭短篇", "2026-09-10T13:05:00+08:00", "2026-09-10T13:05:04+08:00"),
            env("s1", "hotlist", "逆袭短篇", "2026-09-10T13:05:00+08:00", "2026-09-10T13:05:04+08:00", {"heat": 10}),
        ]
        self.assertEqual(len(ingest(rows).accepted), 2)

    def test_metrics_conflict_flagged_bad_rows_rejected(self):
        rows = [
            env("s1", "hotlist", "悬疑", "2026-09-10T15:00:00+08:00", "2026-09-10T15:00:30+08:00", {"heat": 9000}),
            env("s1", "hotlist", "悬疑", "2026-09-10T15:00:00+08:00", "2026-09-10T15:00:40+08:00", {"heat": 9050}),
            {"event_id": "bad", "source": "search", "received_at": "2026-09-10T15:00:00+08:00", "metrics": {}},
        ]
        report = ingest(rows)
        self.assertTrue(report.accepted[0].metrics_conflict)
        self.assertEqual(len(report.rejected), 1)
        self.assertEqual(report.rejected[0][0], 2)


class ScoringTest(ServiceCase):
    def test_received_time_governs_version_occurred_time_governs_bucket(self):
        cutoff = t("2026-09-10T19:00:00+08:00")
        rows = [
            # 近窗：occurred 13:30（6h 窗内）
            env("a1", "search", "逆袭短篇", "2026-09-10T13:30:00+08:00", "2026-09-10T13:31:00+08:00", {"searches": 5000}),
            # 前窗：occurred 08:00，到达早
            env("a0", "search", "逆袭短篇", "2026-09-10T08:00:00+08:00", "2026-09-10T08:01:00+08:00", {"searches": 1000}),
            # 晚到：occurred 14:00，但 20:00 才收到——不能进入 19:00 封存的版本
            env("a2", "search", "逆袭短篇", "2026-09-10T14:00:00+08:00", "2026-09-10T20:00:00+08:00", {"searches": 9000}),
        ]
        self.service.ingest_rows(rows)
        v1 = self.service.save_version(cutoff)["version_id"]
        cand = self.board(v1)["逆袭短篇"]
        self.assertAlmostEqual(cand["breakdown"]["raw_demand_recent"], 5000.0)
        self.assertAlmostEqual(cand["breakdown"]["raw_demand_prior"], 1000.0)
        buckets = {e["bucket"] for e in self.service.get_candidate(v1, "逆袭短篇")["evidence"]}
        self.assertNotIn("a2", [e["event_id"] for e in self.service.get_candidate(v1, "逆袭短篇")["evidence"]])
        self.assertEqual(buckets, {RECENT, PRIOR})
        self.assertGreater(cand["breakdown"]["momentum"], 0.5)  # 5000/6h vs 1000/6h

    def test_late_data_changes_only_later_version(self):
        cutoff1 = t("2026-09-10T19:00:00+08:00")
        cutoff2 = t("2026-09-10T21:00:00+08:00")
        self.service.ingest_rows([
            env("a1", "search", "逆袭短篇", "2026-09-10T13:30:00+08:00", "2026-09-10T13:31:00+08:00", {"searches": 1000}),
        ])
        v1 = self.service.save_version(cutoff1)["version_id"]
        snapshot_v1 = json.dumps(self.board(v1), sort_keys=True)
        # 后来的数据
        self.service.ingest_rows([
            env("a2", "search", "逆袭短篇", "2026-09-10T18:00:00+08:00", "2026-09-10T20:30:00+08:00", {"searches": 9000}),
        ])
        v2 = self.service.save_version(cutoff2)["version_id"]
        self.assertEqual(snapshot_v1, json.dumps(self.board(v1), sort_keys=True))
        self.assertGreater(self.board(v2)["逆袭短篇"]["breakdown"]["raw_demand_recent"], 1000)

    def test_unknown_source_and_low_confidence_degraded(self):
        cutoff = t("2026-09-10T19:00:00+08:00")
        self.service.ingest_rows([
            env("u1", "mystery_feed", "冷门题材", "2026-09-10T15:00:00+08:00", "2026-09-10T15:01:00+08:00", {"heat": 9000}, confidence=0.2),
        ])
        vid = self.service.save_version(cutoff)["version_id"]
        b = self.board(vid)["冷门题材"]["breakdown"]
        self.assertLess(b["penalty_multiplier"], 1.0)
        joined = "；".join(b["degradation"])
        self.assertIn("未知", joined)
        self.assertIn("置信度", joined)
        self.assertIn("证据稀薄", joined)

    def test_competition_density_lowers_score_and_missing_supply_is_neutral(self):
        cutoff = t("2026-09-10T19:00:00+08:00")
        self.service.ingest_rows([
            env("h1", "hotlist", "甜宠日常", "2026-09-10T15:00:00+08:00", "2026-09-10T15:01:00+08:00", {"heat": 8000, "works": 1000}),
            env("h2", "hotlist", "悬疑凶案", "2026-09-10T15:00:00+08:00", "2026-09-10T15:01:00+08:00", {"heat": 8000, "works": 1}),
        ])
        vid = self.service.save_version(cutoff)["version_id"]
        board = self.board(vid)
        self.assertGreater(
            board["悬疑凶案"]["breakdown"]["opportunity"],
            board["甜宠日常"]["breakdown"]["opportunity"],
        )

    def test_late_lag_degradation(self):
        cutoff = t("2026-09-10T19:00:00+08:00")
        self.service.ingest_rows([
            env("l1", "hotlist", "非遗悬疑", "2026-09-10T15:00:00+08:00", "2026-09-10T18:30:00+08:00", {"heat": 5000}),
            env("l2", "search", "非遗悬疑", "2026-09-10T16:00:00+08:00", "2026-09-10T16:01:00+08:00", {"searches": 3000}),
        ])
        vid = self.service.save_version(cutoff)["version_id"]
        detail = self.service.get_candidate(vid, "非遗悬疑")
        self.assertTrue(any(e["lag_seconds"] > 7200 for e in detail["evidence"]))
        self.assertIn("滞后", "；".join(self.board(vid)["非遗悬疑"]["breakdown"]["degradation"]))


class VersionImmutabilityTest(ServiceCase):
    def test_same_cutoff_cannot_be_saved_twice(self):
        cutoff = t("2026-09-10T19:00:00+08:00")
        self.service.ingest_rows([
            env("a1", "search", "逆袭短篇", "2026-09-10T13:30:00+08:00", "2026-09-10T13:31:00+08:00"),
        ])
        self.service.save_version(cutoff)
        with self.assertRaises(ValueError):
            self.service.save_version(cutoff)

    def test_sql_triggers_block_tampering(self):
        cutoff = t("2026-09-10T19:00:00+08:00")
        self.service.ingest_rows([
            env("a1", "search", "逆袭短篇", "2026-09-10T13:30:00+08:00", "2026-09-10T13:31:00+08:00"),
        ])
        self.service.save_version(cutoff)
        self.service.add_feedback("x", "shelve", "占位")
        for sql in (
            "UPDATE board_version SET note='x'",
            "DELETE FROM board_version",
            "UPDATE board_candidate SET rank=99",
            "DELETE FROM board_candidate",
            "UPDATE signal_arrival SET topic='x'",
            "UPDATE editor_feedback SET reason='x'",
        ):
            with self.assertRaises(sqlite3.Error):
                self.store.conn.execute(sql)
                self.store.conn.commit()

    def test_feedback_is_append_only_and_affects_only_future_versions(self):
        cutoff1 = t("2026-09-10T19:00:00+08:00")
        cutoff2 = t("2026-09-10T21:00:00+08:00")
        self.service.ingest_rows([
            env("a1", "search", "逆袭短篇", "2026-09-10T13:30:00+08:00", "2026-09-10T13:31:00+08:00", {"searches": 5000}),
            env("b1", "search", "甜宠日常", "2026-09-10T13:40:00+08:00", "2026-09-10T13:41:00+08:00", {"searches": 5000}),
        ])
        v1 = self.service.save_version(cutoff1)["version_id"]
        v1_before = json.dumps(self.board(v1), sort_keys=True)

        # v1 之后标记：逆袭误判（历史 v1 分数不得变化）
        self.service.add_feedback(
            "逆袭短篇", "misjudge", "剧本会判定与储备不符",
            editor="策划A", created_at=t("2026-09-10T19:30:00+08:00"),
        )
        v2 = self.service.save_version(cutoff2)["version_id"]
        self.assertEqual(v1_before, json.dumps(self.board(v1), sort_keys=True))
        adj_v2 = self.board(v2)["逆袭短篇"]["breakdown"]["feedback_adjustment"]
        self.assertLess(adj_v2, 0)
        self.assertEqual(self.board(v2)["甜宠日常"]["breakdown"]["feedback_adjustment"], 0)

        # 反馈历史完整保留且出现在候选详情
        detail = self.service.get_candidate(v2, "逆袭短篇")
        self.assertEqual(len(detail["feedback_history"]), 1)
        self.assertEqual(detail["feedback_history"][0]["reason"], "剧本会判定与储备不符")

        with self.assertRaises(sqlite3.Error):
            self.store.conn.execute("DELETE FROM editor_feedback")


class DiffTest(ServiceCase):
    def test_adjacent_version_diff_and_evidence_changelog(self):
        self.service.ingest_rows([
            env("a1", "search", "逆袭短篇", "2026-09-10T13:30:00+08:00", "2026-09-10T13:31:00+08:00", {"searches": 1000}),
        ])
        v1 = self.service.save_version(t("2026-09-10T19:00:00+08:00"))["version_id"]
        self.service.ingest_rows([
            env("a2", "hotlist", "逆袭短篇", "2026-09-10T18:00:00+08:00", "2026-09-10T20:30:00+08:00", {"heat": 9900}),
        ])
        v2 = self.service.save_version(t("2026-09-10T21:00:00+08:00"))["version_id"]
        detail = self.service.get_candidate(v2, "逆袭短篇")
        diff = detail["diff_previous_version"]
        self.assertEqual(diff["version_id"], v1)
        added = {(e["event_id"], e["source"]) for e in detail["evidence_changelog"]["added_vs_previous"]}
        self.assertIn(("a2", "hotlist"), added)
        self.assertTrue(any("上升" in line or "热度" in line or "降级" in line for line in detail["why_rising"]))


class RestartConsistencyTest(unittest.TestCase):
    def test_boards_and_dedup_survive_restart(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, "radar.db")
            fixture = json.loads(
                (Path(__file__).parents[1] / "fixtures" / "sample_feed.json").read_text(encoding="utf-8")
            )
            store = RadarStore(db)
            svc = RadarService(store, RadarConfig.load(), Taxonomy.load())
            svc.ingest_rows(fixture)
            svc.add_feedback("非遗悬疑", "follow_up", "工作室有漆器工艺顾问",
                             editor="主编", created_at=t("2026-09-10T18:00:00+08:00"))
            v1 = svc.save_version(t("2026-09-10T19:00:00+08:00"), note="午间复盘版")["version_id"]
            board_before = json.dumps(svc.get_board(v1), sort_keys=True, ensure_ascii=False)
            dedup_before = sorted(
                (r.signal.event_id, r.signal.source, r.signal.received_at.isoformat(), len(r.arrivals))
                for r in store.load_deduped()
            )
            arrivals_before = store.arrival_count()
            store.close()

            # 重启：新进程视角
            store2 = RadarStore(db)
            svc2 = RadarService(store2, RadarConfig.load(), Taxonomy.load())
            self.assertEqual(board_before, json.dumps(svc2.get_board(v1), sort_keys=True, ensure_ascii=False))
            dedup_after = sorted(
                (r.signal.event_id, r.signal.source, r.signal.received_at.isoformat(), len(r.arrivals))
                for r in store2.load_deduped()
            )
            self.assertEqual(dedup_before, dedup_after)

            # 重复喂入同一批数据：无新增到达，去重结论不被覆盖
            report = svc2.ingest_rows(fixture)
            self.assertEqual(report["new_arrivals"], 0)
            self.assertEqual(store2.arrival_count(), arrivals_before)
            self.assertEqual(len(svc2.store.list_feedback("非遗悬疑")), 1)
            store2.close()


class APITest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "radar.db")
        self.api = RadarAPI(self.db)
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(self.api))
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.api.close()
        self.tmp.cleanup()

    def _call(self, method, path, payload=None):
        data = json.dumps(payload).encode() if payload is not None else None
        req = urllib.request.Request(
            f"http://127.0.0.1:{self.port}{path}", data=data, method=method,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read().decode())

    def test_end_to_end_api(self):
        rows = [
            env("a1", "search", "非遗悬疑", "2026-09-10T13:30:00+08:00", "2026-09-10T13:31:00+08:00", {"searches": 5000}),
            env("a2", "hotlist", "非遗悬疑", "2026-09-10T18:00:00+08:00", "2026-09-10T18:01:00+08:00", {"heat": 9000}),
        ]
        status, ingest_resp = self._call("POST", "/api/signals", rows)
        self.assertEqual(status, 200)
        self.assertEqual(ingest_resp["accepted_events"], 2)

        status, saved = self._call("POST", "/api/versions", {"cutoff": "2026-09-10T19:00:00+08:00", "note": "api 测试"})
        self.assertEqual(status, 201)
        vid = saved["version_id"]

        _, board = self._call("GET", f"/api/versions/{vid}")
        self.assertEqual(board["candidates"][0]["topic"], "非遗悬疑")

        encoded = urllib.request.quote("非遗悬疑")
        _, detail = self._call("GET", f"/api/versions/{vid}/candidates/{encoded}")
        self.assertEqual(len(detail["evidence"]), 2)
        self.assertTrue(detail["why_rising"])

        status, fb = self._call("POST", "/api/feedback", {
            "topic": "非遗悬疑", "decision": "follow_up", "reason": "可对接非遗顾问",
        })
        self.assertEqual(status, 201)
        _, history = self._call("GET", f"/api/feedback?topic={encoded}")
        self.assertEqual(len(history["feedback"]), 1)


if __name__ == "__main__":
    unittest.main()
