"""时间解析与序列化工具：内部统一使用带 UTC 偏移的时间。"""

from datetime import datetime, timezone


def parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("时间必须包含 UTC 偏移")
    return parsed


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def to_utc_iso(value: datetime) -> str:
    """归一化为 UTC 存储，保证重启后字节一致。"""
    return value.astimezone(timezone.utc).isoformat()
