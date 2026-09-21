"""命令行入口：python -m topic_radar.cli ..."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .config import RadarConfig
from .repository import Repository
from .service import RadarService
from .taxonomy import Taxonomy
from .timeutil import parse_time


def build_service(args: argparse.Namespace) -> RadarService:
    config = RadarConfig.load(args.config)
    taxonomy = Taxonomy.load(args.taxonomy, uncategorized_fit=config.uncategorized_fit)
    repo = Repository(args.db)
    return RadarService(repo, config, taxonomy)


def print_json(payload) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="漫剧选题机会雷达")
    default_root = Path(__file__).resolve().parents[2]
    parser.add_argument("--db", default=str(default_root / "radar_state" / "radar.db"), help="SQLite 路径（默认本地缓存目录）")
    parser.add_argument("--config", default=str(default_root / "fixtures" / "radar_config.json"))
    parser.add_argument("--taxonomy", default=str(default_root / "fixtures" / "taxonomy.json"))

    sub = parser.add_subparsers(dest="command", required=True)

    p_ingest = sub.add_parser("ingest", help="摄入信号信封 JSON 文件（数组）")
    p_ingest.add_argument("file")

    p_preview = sub.add_parser("preview", help="预览当前榜单")
    p_preview.add_argument("--as-of", dest="as_of")

    p_save = sub.add_parser("save-version", help="固化榜单版本")
    p_save.add_argument("--as-of", dest="as_of")
    p_save.add_argument("--label")

    sub.add_parser("versions", help="列出已保存版本")

    p_show = sub.add_parser("show", help="查看版本完整快照")
    p_show.add_argument("version_id")

    p_diff = sub.add_parser("diff", help="版本差异")
    p_diff.add_argument("version_id")
    p_diff.add_argument("--from", dest="from_version")

    p_candidate = sub.add_parser("candidate", help="候选完整身世")
    p_candidate.add_argument("topic")

    p_decide = sub.add_parser("decide", help="追加人工决策")
    p_decide.add_argument("topic")
    p_decide.add_argument("--action", choices=["follow", "shelve", "misjudge"], required=True)
    p_decide.add_argument("--reason", required=True)
    p_decide.add_argument("--editor")
    p_decide.add_argument("--at", dest="decided_at", help="决策时间（补录历史决策时使用，默认现在）")

    sub.add_parser("duplicates", help="查看同源重复留痕")

    p_serve = sub.add_parser("serve", help="启动 HTTP API")
    p_serve.add_argument("--host", default="127.0.0.1")
    p_serve.add_argument("--port", type=int, default=8000)

    args = parser.parse_args(argv)
    service = build_service(args)

    from .repository import VersionExistsError

    try:
        if args.command == "ingest":
            rows = json.loads(Path(args.file).read_text(encoding="utf-8"))
            print_json(service.ingest_rows(rows))
        elif args.command == "preview":
            print_json(service.preview(parse_time(args.as_of) if args.as_of else None))
        elif args.command == "save-version":
            try:
                print_json(service.save_version(parse_time(args.as_of) if args.as_of else None, args.label))
            except VersionExistsError as exc:
                print(f"拒绝保存：{exc}", file=sys.stderr)
                return 2
        elif args.command == "versions":
            print_json({"versions": service.list_versions()})
        elif args.command == "show":
            record = service.get_version(args.version_id)
            if record is None:
                print("版本不存在", file=sys.stderr)
                return 1
            print_json(record)
        elif args.command == "diff":
            try:
                print_json(service.diff_versions(args.version_id, args.from_version))
            except KeyError as exc:
                print(f"版本不存在: {exc.args[0]}", file=sys.stderr)
                return 1
        elif args.command == "candidate":
            print_json(service.candidate_detail(args.topic))
        elif args.command == "decide":
            print_json(service.record_decision(
                args.topic, args.action, args.reason, args.editor,
                decided_at=parse_time(args.decided_at) if args.decided_at else None))
        elif args.command == "duplicates":
            print_json({"duplicates": service.duplicates()})
        elif args.command == "serve":
            from .api import serve
            serve(service, args.host, args.port)
    finally:
        service.repo.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
