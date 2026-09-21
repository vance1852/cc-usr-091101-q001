"""命令行入口：导入信号、封存版本、查看候选与提交反馈。

示例：
    python -m topic_radar.cli ingest --file fixtures/sample_feed.json
    python -m topic_radar.cli save --cutoff 2026-09-10T19:00:00+08:00
    python -m topic_radar.cli board --version 1
    python -m topic_radar.cli candidate --version 2 --topic 非遗悬疑
    python -m topic_radar.cli feedback --topic 非遗悬疑 --decision follow_up --reason 剧本储备充足
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .config import RadarConfig, Taxonomy
from .service import RadarService, parse_time
from .store import RadarStore


def _print(payload: object) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="topic_radar", description="漫剧选题机会雷达")
    parser.add_argument("--db", default="radar.db")
    sub = parser.add_subparsers(dest="command", required=True)

    p_ingest = sub.add_parser("ingest", help="导入信号信封 JSON 数组")
    p_ingest.add_argument("--file", required=True)

    p_save = sub.add_parser("save", help="按 cutoff 封存不可改写的榜单版本")
    p_save.add_argument("--cutoff", required=True, help="接收截止时间（含 UTC 偏移）")
    p_save.add_argument("--note", default="")

    p_versions = sub.add_parser("versions", help="列出已保存版本")

    p_board = sub.add_parser("board", help="查看某版本候选榜")
    p_board.add_argument("--version", type=int, required=True)

    p_cand = sub.add_parser("candidate", help="查看候选可解释详情")
    p_cand.add_argument("--version", type=int, required=True)
    p_cand.add_argument("--topic", required=True)

    p_fb = sub.add_parser("feedback", help="追加编辑决策（跟进/搁置/误判）")
    p_fb.add_argument("--topic", required=True)
    p_fb.add_argument("--decision", required=True, choices=["follow_up", "shelve", "misjudge"])
    p_fb.add_argument("--reason", default="")
    p_fb.add_argument("--editor", default="")
    p_fb.add_argument("--at", dest="created_at", default=None,
                      help="决策时间（含 UTC 偏移）；省略则取当前时间，补录历史决策时必填")

    args = parser.parse_args(argv)
    with RadarStore(args.db) as store:
        service = RadarService(store, RadarConfig.load(), Taxonomy.load())
        if args.command == "ingest":
            rows = json.loads(Path(args.file).read_text(encoding="utf-8"))
            _print(service.ingest_rows(rows))
        elif args.command == "save":
            _print(service.save_version(parse_time(args.cutoff), args.note))
        elif args.command == "versions":
            _print({"versions": service.list_versions()})
        elif args.command == "board":
            _print(service.get_board(args.version))
        elif args.command == "candidate":
            _print(service.get_candidate(args.version, args.topic))
        elif args.command == "feedback":
            at = parse_time(args.created_at) if args.created_at else None
            fid = service.add_feedback(args.topic, args.decision, args.reason, args.editor, created_at=at)
            _print({"feedback_id": fid})
    return 0


if __name__ == "__main__":
    sys.exit(main())
