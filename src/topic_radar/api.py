"""选题雷达 HTTP API（标准库实现，无第三方依赖）。

路由：

* ``POST /api/signals``             上报/导入一批信号信封（JSON 数组）
* ``GET  /api/versions``            已保存版本列表
* ``POST /api/versions``            按 ``{"cutoff": "...", "note": "..."}`` 封存新版本
* ``GET  /api/versions/<id>``       某版本候选榜
* ``GET  /api/versions/<id>/candidates/<topic>`` 候选可解释详情
* ``POST /api/feedback``            追加编辑决策
* ``GET  /api/feedback?topic=...``  反馈历史

启动：``python -m topic_radar.api --db radar.db --host 127.0.0.1 --port 8000``
"""

from __future__ import annotations

import json
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

from .config import RadarConfig, Taxonomy
from .service import VALID_DECISIONS, RadarService, parse_time
from .store import RadarStore


class RadarAPI:
    def __init__(self, db_path: str = "radar.db", radar_config: str | None = None, taxonomy_config: str | None = None):
        self.store = RadarStore(db_path)
        cfg = RadarConfig.load(radar_config) if radar_config else RadarConfig.load()
        tax = Taxonomy.load(taxonomy_config) if taxonomy_config else Taxonomy.load()
        self.service = RadarService(self.store, cfg, tax)

    def close(self) -> None:
        self.store.close()


def _make_handler(api: RadarAPI) -> type[BaseHTTPRequestHandler]:
    service = api.service

    class Handler(BaseHTTPRequestHandler):
        server_version = "TopicRadar/0.1"

        def log_message(self, fmt: str, *args: Any) -> None:  # 安静一点
            return

        def _send(self, status: int, payload: Any) -> None:
            body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _read_json(self) -> Any:
            length = int(self.headers.get("Content-Length", 0))
            if length == 0:
                return {}
            raw = self.rfile.read(length)
            return json.loads(raw.decode("utf-8"))

        def do_GET(self) -> None:  # noqa: N802
            self._route("GET")

        def do_POST(self) -> None:  # noqa: N802
            self._route("POST")

        def _route(self, method: str) -> None:
            parsed = urlparse(self.path)
            path = parsed.path
            query = {k: v[-1] for k, v in parse_qs(parsed.query).items()}
            try:
                if method == "POST" and path == "/api/signals":
                    rows = self._read_json()
                    if not isinstance(rows, list):
                        self._send(400, {"error": "请求体必须是信号信封数组"})
                        return
                    self._send(200, service.ingest_rows(rows))
                    return

                if method == "GET" and path == "/api/versions":
                    self._send(200, {"versions": service.list_versions()})
                    return

                if method == "POST" and path == "/api/versions":
                    body = self._read_json()
                    cutoff = parse_time(str(body["cutoff"]))
                    self._send(201, service.save_version(cutoff, str(body.get("note", ""))))
                    return

                m = re.fullmatch(r"/api/versions/(\d+)", path)
                if method == "GET" and m:
                    self._send(200, service.get_board(int(m.group(1))))
                    return

                m = re.fullmatch(r"/api/versions/(\d+)/candidates/([^/]+)", path)
                if method == "GET" and m:
                    topic = unquote(m.group(2))
                    self._send(200, service.get_candidate(int(m.group(1)), topic))
                    return

                if method == "POST" and path == "/api/feedback":
                    body = self._read_json()
                    required = {"topic", "decision"}
                    missing = required - body.keys()
                    if missing:
                        self._send(400, {"error": f"缺少字段: {sorted(missing)}"})
                        return
                    if body["decision"] not in VALID_DECISIONS:
                        self._send(400, {"error": f"decision 必须是 {VALID_DECISIONS}"})
                        return
                    created = parse_time(body["created_at"]) if body.get("created_at") else None
                    fid = service.add_feedback(
                        topic=str(body["topic"]),
                        decision=str(body["decision"]),
                        reason=str(body.get("reason", "")),
                        editor=str(body.get("editor", "")),
                        applied_from_version=body.get("applied_from_version"),
                        created_at=created,
                    )
                    self._send(201, {"feedback_id": fid})
                    return

                if method == "GET" and path == "/api/feedback":
                    self._send(200, {"feedback": [dict(r) for r in service.store.list_feedback(query.get("topic"))]})
                    return

                self._send(404, {"error": "未找到路由"})
            except KeyError as exc:
                self._send(404, {"error": str(exc)})
            except (ValueError, TypeError, json.JSONDecodeError) as exc:
                self._send(400, {"error": str(exc)})
            except Exception as exc:  # 防御：不要让连接挂死
                self._send(500, {"error": f"{type(exc).__name__}: {exc}"})

    return Handler


def serve(db_path: str = "radar.db", host: str = "127.0.0.1", port: int = 8000):
    api = RadarAPI(db_path)
    httpd = ThreadingHTTPServer((host, port), _make_handler(api))
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
        api.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="启动选题雷达 API")
    parser.add_argument("--db", default="radar.db")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()
    serve(args.db, args.host, args.port)
