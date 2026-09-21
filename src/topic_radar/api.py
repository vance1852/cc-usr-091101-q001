"""选题雷达 HTTP API（标准库 http.server）。

路由
----
GET  /health
GET  /radar/preview?as_of=...
GET  /versions                       已保存版本列表
POST /versions                       固化当前榜单 {"as_of": "...", "label": "..."}
GET  /versions/{id}                  版本完整快照
GET  /versions/{id}/diff?from=...     与相邻（或指定）版本的差异
GET  /candidates/{topic}             候选完整身世（上升原因、证据、版本变化、决策）
POST /candidates/{topic}/decisions   追加人工决策 {"action","reason","editor"}
GET  /duplicates                     同源重复留痕
POST /ingest                         追加信号信封（JSON 数组）
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlparse

from .service import RadarService
from .timeutil import parse_time


def _parse_as_of(value: str | None) -> datetime:
    if not value:
        from .timeutil import now_utc
        return now_utc()
    return parse_time(value)


def make_handler(service: RadarService) -> type[BaseHTTPRequestHandler]:
    class RadarHandler(BaseHTTPRequestHandler):
        server_version = "TopicRadar/0.1"

        def log_message(self, fmt, *args):  # 安静一些
            pass

        def _send(self, status: int, payload) -> None:
            body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _read_body(self) -> dict | list:
            length = int(self.headers.get("Content-Length", 0))
            if length == 0:
                return {}
            return json.loads(self.rfile.read(length).decode("utf-8"))

        def _error(self, status: int, message: str) -> None:
            self._send(status, {"error": message})

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            path, query = unquote(parsed.path), parse_qs(parsed.query)
            try:
                if path == "/health":
                    self._send(200, {"status": "ok"})
                elif path == "/radar/preview":
                    self._send(200, service.preview(_parse_as_of(query.get("as_of", [None])[0])))
                elif path == "/versions":
                    self._send(200, {"versions": service.list_versions()})
                elif path == "/duplicates":
                    self._send(200, {"duplicates": service.duplicates()})
                else:
                    m = re.fullmatch(r"/versions/([^/]+)", path)
                    if m:
                        record = service.get_version(m.group(1))
                        if record is None:
                            self._error(404, "版本不存在")
                        else:
                            self._send(200, record)
                        return
                    m = re.fullmatch(r"/versions/([^/]+)/diff", path)
                    if m:
                        self._send(200, service.diff_versions(
                            m.group(1), query.get("from", [None])[0]))
                        return
                    m = re.fullmatch(r"/candidates/([^/]+)", path)
                    if m:
                        self._send(200, service.candidate_detail(m.group(1)))
                        return
                    self._error(404, "未知路径")
            except ValueError as exc:
                self._error(400, str(exc))
            except KeyError as exc:
                self._error(404, f"不存在: {exc.args[0]}")

        def do_POST(self) -> None:  # noqa: N802
            path = unquote(urlparse(self.path).path)
            try:
                body = self._read_body()
                if path == "/ingest":
                    if not isinstance(body, list):
                        self._error(400, "请求体必须是信号信封 JSON 数组")
                        return
                    self._send(200, service.ingest_rows(body))
                elif path == "/versions":
                    if not isinstance(body, dict):
                        self._error(400, "请求体必须是对象")
                        return
                    result = service.save_version(
                        as_of=_parse_as_of(body.get("as_of")),
                        label=body.get("label"),
                    )
                    self._send(201, result)
                elif path.startswith("/candidates/") and path.endswith("/decisions"):
                    topic = path[len("/candidates/"):-len("/decisions")]
                    if not isinstance(body, dict):
                        self._error(400, "请求体必须是对象")
                        return
                    result = service.record_decision(
                        topic=topic,
                        action=body.get("action"),
                        reason=body.get("reason"),
                        editor=body.get("editor"),
                        decided_at=_parse_as_of(body["decided_at"]) if body.get("decided_at") else None,
                        version_id=body.get("version_id"),
                    )
                    self._send(201, result)
                else:
                    self._error(404, "未知路径")
            except ValueError as exc:
                self._error(400, str(exc))

    return RadarHandler


def serve(service: RadarService, host: str = "127.0.0.1", port: int = 8000) -> None:
    server = ThreadingHTTPServer((host, port), make_handler(service))
    print(f"选题雷达 API 已启动: http://{host}:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
