import json
import sys
import tempfile
import threading
import unittest
import urllib.parse
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from topic_radar.api import make_handler
from topic_radar.config import RadarConfig
from topic_radar.contracts import SignalEnvelope
from topic_radar.repository import Repository, VersionExistsError
from topic_radar.scoring import build_radar
from topic_radar.service import RadarService, canonical_dumps
from topic_radar.taxonomy import Taxonomy
from topic_radar.timeutil import parse_time

ROOT = Path(__file__).parents[1]
FIXTURES = ROOT / "fixtures"
T15 = parse_time("2026-09-10T15:00:00+08:00")
T17 = parse_time("2026-09-10T17:00:00+08:00")


def make_service(db_path: str | Path) -> RadarService:
    config = RadarConfig.load(FIXTURES / "radar_config.json")
    taxonomy = Taxonomy.load(FIXTURES / "taxonomy.json", config.uncategorized_fit)
    return RadarService(Repository(db_path), config, taxonomy)


def envelope(event_id, source, topic, occurred, received, metrics, **attrs):
    return SignalEnvelope.from_dict({
        "event_id": event_id, "source": source, "topic": topic,
        "occurred_at": occurred, "received_at": received,
        "metrics": metrics, **attrs,
    })


class ServiceTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = str(Path(self.tmp.name) / "radar.db")
        self.service = make_service(self.db)
        self.rows = json.loads((FIXTURES / "signals.json").read_text(encoding="utf-8"))

    def tearDown(self):
        self.service.repo.close()
        self.tmp.cleanup()

    def test_dedup_keeps_earliest_received_and_is_order_independent(self):
        result = self.service.ingest_rows(self.rows)
        # 9 行中 sig-101 重复投递一次：8 个正本 + 1 条重复
        self.assertEqual(result["status_counts"], {"inserted": 8, "duplicate": 1})
        signals = self.service.repo.list_signals()
        canonical = next(s for s in signals if (s.source, s.event_id) == ("search", "sig-101"))
        self.assertEqual(canonical.received_at.isoformat(), "2026-09-10T05:05:04+00:00")
        dupes = self.service.duplicates()
        self.assertEqual([(d["event_id"], d["note"]) for d in dupes], [("sig-101", "redelivery")])

        # 乱序摄入：先到 received 更晚的副本，再补更早的 -> 正本应被替换
        with tempfile.TemporaryDirectory() as tmp2:
            svc2 = make_service(str(Path(tmp2) / "r.db"))
            rows_reversed = [self.rows[1], self.rows[0]]
            svc2.ingest_rows(rows_reversed)
            canon = next(s for s in svc2.repo.list_signals() if s.event_id == "sig-101")
            self.assertEqual(canon.received_at, parse_time("2026-09-10T13:05:04+08:00"))
            notes = [(d["note"]) for d in svc2.duplicates()]
            self.assertIn("superseded-canonical", notes)
            svc2.repo.close()

        # 重复整批重放不应再产生新正本
        again = self.service.ingest_rows(self.rows)
        self.assertEqual(again["status_counts"], {"duplicate": 9})
        self.assertEqual(len(self.service.repo.list_signals()), 8)

    def test_visibility_distinguishes_event_and_reception_time(self):
        self.service.ingest_rows(self.rows)
        radar = self.service.preview(T15)
        topics = {c["topic"] for c in radar["candidates"]}
        # sig-104 14:30 发生、16:31 才收到：15:00 的榜单对它一无所知
        self.assertNotIn("闪婚甜宠", topics)
        late = {e["event_id"] for e in radar["excluded_late"]}
        self.assertIn("sig-104", late)
        self.assertEqual(radar["totals"]["visible_signals"], 7)

        radar17 = self.service.preview(T17)
        self.assertIn("闪婚甜宠", {c["topic"] for c in radar17["candidates"]})
        self.assertEqual(radar17["totals"]["excluded_late"], 0)

    def test_scoring_components_and_downgrades(self):
        self.service.ingest_rows(self.rows)
        radar = self.service.preview(T15)
        by_topic = {c["topic"]: c for c in radar["candidates"]}

        rise = by_topic["逆袭短篇"]
        self.assertEqual(rise["rank"], 1)
        self.assertGreater(rise["heat"]["raw"], 0)
        self.assertTrue(rise["velocity"]["newcomer"])
        self.assertEqual(rise["category"], "逆袭爽感")
        self.assertEqual(set(rise["distinct_sources"]), {"search", "platform"})
        self.assertNotIn("low_confidence", rise["flags"])
        self.assertTrue(any("首次出现" in r for r in rise["why_rising"]))
        self.assertEqual({e["event_id"] for e in rise["evidence"]},
                         {"sig-101", "sig-102", "sig-103"})

        feiyi = by_topic["非遗悬疑"]
        self.assertIn("cooling", feiyi["flags"])  # 近 2 小时窗口无新事件
        self.assertIn("【降级】", "".join(feiyi["why_rising"]))

        rss = by_topic["热血江湖录"]
        self.assertIn("unknown_source", rss["flags"])
        self.assertIn("insufficient_sources", rss["flags"])
        self.assertLess(rss["downgrade_multiplier"], 1.0)

        # 打脸逆袭短剧 与 逆袭短篇 同分类 -> 互为竞品
        dali = by_topic["打脸逆袭短剧"]
        self.assertEqual(dali["competition"]["competitor_topics"], ["逆袭短篇"])
        self.assertEqual(rise["competition"]["competitor_topics"], ["打脸逆袭短剧"])
        self.assertGreater(dali["competition"]["density_score"], 0.0)

    def test_version_is_immutable_and_late_data_only_makes_new_version(self):
        self.service.ingest_rows(self.rows)
        v1 = self.service.save_version(T15, label="午间策划会")
        snapshot_before = canonical_dumps(self.service.get_version(v1["version_id"])["snapshot"])

        # 策划保存后再补晚到数据与人工决策
        late_row = {"event_id": "sig-900", "source": "platform", "topic": "闪婚甜宠",
                    "occurred_at": "2026-09-10T16:40:00+08:00",
                    "received_at": "2026-09-10T16:41:00+08:00",
                    "metrics": {"heat": 50000}}
        self.service.ingest_rows([late_row])
        self.service.record_decision("热血江湖录", "misjudge", "编辑核实为旧闻翻炒", editor="阿闻")
        v2 = self.service.save_version(T17, label="傍晚复盘")

        snapshot_after = canonical_dumps(self.service.get_version(v1["version_id"])["snapshot"])
        self.assertEqual(snapshot_before, snapshot_after, "已保存版本被晚到数据或决策改写")

        # 同一 as_of 不可二次保存
        with self.assertRaises(VersionExistsError):
            self.service.save_version(T15, label="想覆盖午间版")

        # v2 包含晚到题材与决策影响，v1 不包含
        v2_topics = {c["topic"] for c in self.service.get_version(v2["version_id"])["snapshot"]["candidates"]}
        self.assertIn("闪婚甜宠", v2_topics)
        v1_topics = {c["topic"] for c in self.service.get_version(v1["version_id"])["snapshot"]["candidates"]}
        self.assertNotIn("闪婚甜宠", v1_topics)

        # 决策需要理由
        with self.assertRaises(ValueError):
            self.service.record_decision("逆袭短篇", "follow", "  ")

    def test_feedback_changes_future_but_not_history_and_is_time_bounded(self):
        signals = [
            envelope("a1", "platform", "武侠新番",
                     "2026-09-10T11:30:00+08:00", "2026-09-10T11:31:00+08:00", {"heat": 2000}),
            envelope("a2", "search", "武侠新番",
                     "2026-09-10T14:30:00+08:00", "2026-09-10T14:31:00+08:00", {"searches": 3000}),
        ]

        def score(decisions):
            radar = build_radar(T15, signals, decisions, self.service.config, self.service.taxonomy)
            return next(c for c in radar["candidates"] if c["topic"] == "武侠新番")

        baseline = score([])["final_score"]

        # 决策时间早于 as_of：影响排序但不碰任何历史分数文件
        decided = {"topic": "武侠新番", "action": "follow", "reason": "与制作能力匹配",
                   "editor": "策划组长", "decided_at": parse_time("2026-09-10T14:00:00+08:00")}
        followed = score([decided])
        self.assertGreater(followed["final_score"], baseline)
        self.assertEqual(followed["feedback"]["action"], "follow")
        self.assertAlmostEqual(
            followed["final_score"],
            baseline + self.service.config.feedback_adjustments["follow"], places=6)

        # 决策时间晚于 as_of：历史时刻重算必须当作该决策尚不存在
        future = {**decided, "decided_at": parse_time("2026-09-10T16:00:00+08:00")}
        unaffected = score([future])
        self.assertIsNone(unaffected["feedback"])
        self.assertAlmostEqual(unaffected["final_score"], baseline, places=6)

        # 落盘保存两个版本：决策只进新版本，旧版本快照字节不变
        self.service.ingest_rows([
            {"event_id": "a1", "source": "platform", "topic": "武侠新番",
             "occurred_at": "2026-09-10T11:30:00+08:00", "received_at": "2026-09-10T11:31:00+08:00",
             "metrics": {"heat": 2000}},
            {"event_id": "a2", "source": "search", "topic": "武侠新番",
             "occurred_at": "2026-09-10T14:30:00+08:00", "received_at": "2026-09-10T14:31:00+08:00",
             "metrics": {"searches": 3000}},
        ])
        v1 = self.service.save_version(T15)
        frozen = canonical_dumps(self.service.get_version(v1["version_id"])["snapshot"])
        self.service.record_decision("武侠新番", "follow", "与制作能力匹配",
                                     decided_at=parse_time("2026-09-10T16:30:00+08:00"))
        v2 = self.service.save_version(T17)
        self.assertEqual(canonical_dumps(self.service.get_version(v1["version_id"])["snapshot"]), frozen)
        self.assertEqual(
            next(c for c in self.service.get_version(v2["version_id"])["snapshot"]["candidates"]
                 if c["topic"] == "武侠新番")["feedback"]["action"], "follow")

    def test_restart_preserves_versions_decisions_and_dedup(self):
        self.service.ingest_rows(self.rows)
        v1 = self.service.save_version(T15, label="午间策划会")
        self.service.record_decision("非遗悬疑", "shelve", "改编成本过高")
        frozen = canonical_dumps(self.service.get_version(v1["version_id"])["snapshot"])
        dup_count = len(self.service.duplicates())
        self.service.repo.close()

        restarted = make_service(self.db)
        self.assertEqual(canonical_dumps(restarted.get_version(v1["version_id"])["snapshot"]), frozen)
        self.assertEqual(len(restarted.duplicates()), dup_count)
        self.assertEqual(len(restarted.repo.list_signals()), 8)
        d = restarted.repo.list_decisions()[0]
        self.assertEqual((d["topic"], d["action"], d["reason"]), ("非遗悬疑", "shelve", "改编成本过高"))
        # 重启后重复重放仍然全部识别为重复
        self.assertEqual(restarted.ingest_rows(self.rows)["status_counts"], {"duplicate": 9})
        restarted.repo.close()

    def test_content_addressed_version_is_stable_across_fresh_builds(self):
        self.service.ingest_rows(self.rows)
        v1 = self.service.save_version(T15)
        self.service.repo.close()
        with tempfile.TemporaryDirectory() as tmp2:
            other = make_service(str(Path(tmp2) / "r.db"))
            other.ingest_rows(self.rows)
            rebuilt = other.save_version(T15)
            self.assertEqual(rebuilt["version_id"], v1["version_id"])
            other.repo.close()

    def test_candidate_detail_and_adjacent_diff(self):
        self.service.ingest_rows(self.rows)
        v1 = self.service.save_version(T15, label="午间")
        self.service.record_decision("热血江湖录", "follow", "美术风格契合")
        late_row = {"event_id": "sig-900", "source": "platform", "topic": "闪婚甜宠",
                    "occurred_at": "2026-09-10T16:40:00+08:00",
                    "received_at": "2026-09-10T16:41:00+08:00", "metrics": {"heat": 50000}}
        self.service.ingest_rows([late_row])
        v2 = self.service.save_version(T17, label="傍晚")

        detail = self.service.candidate_detail("闪婚甜宠")
        self.assertEqual([t["version_id"] for t in detail["timeline"]], [v2["version_id"]])
        self.assertTrue(any(r["change"] == "appeared" for r in detail["adjacent_changes"]) or
                        detail["present_in_versions"] == 1)
        self.assertIn("sig-900", {e["event_id"] for e in detail["latest"]["evidence"]})
        # sig-104 是延迟超过两小时送到的证据，应显式留痕，且本窗口环比上一窗口有增长
        self.assertIn("contains_late_arrival", detail["latest"]["flags"])
        self.assertTrue(any("环比上一窗口增长" in r for r in detail["latest"]["why_rising"]))

        diff = self.service.diff_versions(v2["version_id"])
        self.assertEqual(diff["from_version"], v1["version_id"])
        appeared = {c["topic"] for c in diff["changes"] if c["change"] == "appeared"}
        self.assertIn("闪婚甜宠", appeared)
        follow_change = next(c for c in diff["changes"] if c["topic"] == "热血江湖录")
        self.assertNotEqual(follow_change["score_delta"], 0.0)

        detail_rss = self.service.candidate_detail("热血江湖录")
        self.assertEqual(detail_rss["decisions"][0]["action"], "follow")
        self.assertTrue(detail_rss["decisions"][0]["reason"])

    def test_invalid_envelopes_are_reported_not_crashing(self):
        bad = [
            {"event_id": "x", "source": "platform", "topic": "无题"},  # 缺字段
            {"event_id": "y", "source": "platform", "topic": "无时区",
             "occurred_at": "2026-09-10T12:00:00", "received_at": "2026-09-10T12:01:00+08:00",
             "metrics": {}},
        ]
        result = self.service.ingest_rows(bad)
        self.assertEqual(result["ingested"], 0)
        self.assertEqual(len(result["errors"]), 2)


class HttpApiTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.service = make_service(str(Path(cls.tmp.name) / "api.db"))
        server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(cls.service))
        cls.server = server
        cls.port = server.server_address[1]
        cls.thread = threading.Thread(target=server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.service.repo.close()
        cls.tmp.cleanup()

    def _request(self, method: str, path: str, body=None):
        url = f"http://127.0.0.1:{self.port}{path}"
        data = json.dumps(body).encode("utf-8") if body is not None else None
        req = urllib.request.Request(url, data=data, method=method,
                                     headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req) as resp:
                return resp.status, json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read().decode("utf-8"))

    def test_full_flow_over_http(self):
        status, body = self._request("GET", "/health")
        self.assertEqual((status, body["status"]), (200, "ok"))

        rows = json.loads((FIXTURES / "signals.json").read_text(encoding="utf-8"))
        status, body = self._request("POST", "/ingest", rows)
        self.assertEqual(status, 200)
        self.assertEqual(body["status_counts"]["inserted"], 8)

        status, body = self._request("GET", f"/radar/preview?as_of=2026-09-10T15:00:00%2B08:00")
        self.assertEqual(status, 200)
        self.assertEqual(body["totals"]["visible_signals"], 7)

        status, v1 = self._request("POST", "/versions", {"as_of": "2026-09-10T15:00:00+08:00", "label": "午间"})
        self.assertEqual(status, 201)
        status, _ = self._request("POST", "/versions", {"as_of": "2026-09-10T15:00:00+08:00"})
        self.assertEqual(status, 400)  # 同时刻版本已存在，拒绝改写

        status, body = self._request("POST", f"/candidates/{urllib.parse.quote('逆袭短篇')}/decisions",
                                     {"action": "follow", "reason": "制作排期有空档", "editor": "阿策"})
        self.assertEqual(status, 201)
        status, body = self._request("POST", f"/candidates/{urllib.parse.quote('逆袭短篇')}/decisions",
                                     {"action": "follow"})
        self.assertEqual(status, 400)  # 缺理由

        status, v2 = self._request("POST", "/versions", {"as_of": "2026-09-10T17:00:00+08:00"})
        self.assertEqual(status, 201)

        status, body = self._request("GET", f"/versions/{v2['version_id']}/diff")
        self.assertEqual(status, 200)
        self.assertEqual(body["from_version"], v1["version_id"])

        status, body = self._request("GET", f"/candidates/{urllib.parse.quote('逆袭短篇')}")
        self.assertEqual(status, 200)
        self.assertEqual(len(body["timeline"]), 2)
        self.assertTrue(body["latest"]["why_rising"])
        self.assertTrue(body["decisions"])

        status, body = self._request("GET", "/duplicates")
        self.assertEqual(status, 200)
        self.assertTrue(any(d["event_id"] == "sig-101" for d in body["duplicates"]))

        status, body = self._request("GET", f"/versions/{urllib.parse.quote('不存在')}")
        self.assertEqual(status, 404)


if __name__ == "__main__":
    unittest.main()
