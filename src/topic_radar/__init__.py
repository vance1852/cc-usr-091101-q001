"""漫剧选题机会雷达。

模块职责：
- contracts：跨平台信号信封（稳定接入契约）
- config：分类词表、来源画像与评分参数
- ingest：同源去重，结果与加载顺序无关
- scoring：热度/增速/机会/受众四维评分与显式降级
- store：只追加的 SQLite 封存层，历史不可改写
- service：版本封存、反馈生效边界与相邻版本对比
- api / cli：对外接口
"""

from .config import Category, RadarConfig, SourceProfile, Taxonomy
from .contracts import SignalEnvelope
from .ingest import DedupResult, IngestReport, ingest
from .scoring import Candidate, Evidence, ScoreBreakdown, build_candidates
from .service import RadarService
from .store import RadarStore

__all__ = [
    "SignalEnvelope",
    "Category",
    "Taxonomy",
    "RadarConfig",
    "SourceProfile",
    "DedupResult",
    "IngestReport",
    "ingest",
    "Candidate",
    "Evidence",
    "ScoreBreakdown",
    "build_candidates",
    "RadarService",
    "RadarStore",
]
