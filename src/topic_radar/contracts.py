"""平台趋势信号的稳定交换格式。"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any


def _time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("时间必须包含 UTC 偏移")
    return parsed


@dataclass(frozen=True)
class SignalEnvelope:
    event_id: str
    source: str
    topic: str
    occurred_at: datetime
    received_at: datetime
    metrics: dict[str, float]
    attributes: dict[str, Any]

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "SignalEnvelope":
        required = {"event_id", "source", "topic", "occurred_at", "received_at", "metrics"}
        missing = required - raw.keys()
        if missing:
            raise ValueError(f"缺少字段: {', '.join(sorted(missing))}")
        metrics = raw["metrics"]
        if not isinstance(metrics, dict) or not all(isinstance(v, (int, float)) for v in metrics.values()):
            raise ValueError("metrics 必须是数值映射")
        return cls(str(raw["event_id"]), str(raw["source"]), str(raw["topic"]), _time(str(raw["occurred_at"])), _time(str(raw["received_at"])), {str(k): float(v) for k, v in metrics.items()}, {k: v for k, v in raw.items() if k not in required})
