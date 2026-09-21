"""信号采集与同源去重。

去重规则：同一 ``(event_id, source)`` 视为同一条信号的多次投递
（重放或补投）。结果与加载顺序无关——按 ``received_at`` 最早者胜出，
时间完全相同则以规范 JSON 的字典序兜底，保证可重复构建。
所有原始到达记录仍保留在 ``arrivals`` 中以便追溯。
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from .contracts import SignalEnvelope


def _canonical(signal: SignalEnvelope) -> str:
    payload = {
        "event_id": signal.event_id,
        "source": signal.source,
        "topic": signal.topic,
        "occurred_at": signal.occurred_at.isoformat(),
        "received_at": signal.received_at.isoformat(),
        "metrics": signal.metrics,
        "attributes": signal.attributes,
    }
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


@dataclass(frozen=True)
class DedupResult:
    signal: SignalEnvelope
    arrivals: tuple[SignalEnvelope, ...]
    metrics_conflict: bool = False

    @property
    def duplicate_count(self) -> int:
        return len(self.arrivals) - 1


@dataclass(frozen=True)
class IngestReport:
    accepted: tuple[DedupResult, ...]
    rejected: tuple[tuple[int, str, str], ...]  # (行号, 原文摘录, 原因)

    @property
    def duplicate_arrivals(self) -> int:
        return sum(r.duplicate_count for r in self.accepted)


def load_rows(path: str | Path) -> list[dict]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def ingest(rows: list[dict]) -> IngestReport:
    """解析、校验并按来源去重。

    坏行被隔离到 ``rejected`` 而不是中断整批导入——一条来源不明或
    字段缺失的记录不应让其余信号无法成榜。
    """
    grouped: dict[tuple[str, str], list[SignalEnvelope]] = {}
    rejected: list[tuple[int, str, str]] = []

    for index, row in enumerate(rows):
        try:
            signal = SignalEnvelope.from_dict(row)
        except (ValueError, TypeError, AttributeError) as exc:
            rejected.append((index, json.dumps(row, ensure_ascii=False)[:120], str(exc)))
            continue
        grouped.setdefault((signal.event_id, signal.source), []).append(signal)

    accepted: list[DedupResult] = []
    for key, arrivals in grouped.items():
        ordered = sorted(arrivals, key=lambda s: (s.received_at, _canonical(s)))
        metrics_variants = {json.dumps(s.metrics, sort_keys=True) for s in arrivals}
        accepted.append(DedupResult(ordered[0], tuple(ordered), len(metrics_variants) > 1))

    accepted.sort(key=lambda r: (r.signal.occurred_at, r.signal.source, r.signal.event_id))
    return IngestReport(tuple(accepted), tuple(rejected))
