# 漫剧选题机会雷达

把各平台采集记录、站内搜索反馈和编辑观察整理成**可追溯的候选题材**：
每条分数都能回溯到具体证据信号，每个时点的榜单一经策划保存即为不可改写版本。

## 它解决什么问题

- 平台热点两小时内翻转，午间导出的表格到开会时已失效 → 雷达按**可配置观察窗口**实时计算热度、增速、竞争密度、受众契合度。
- 一份来源不明的热词榜 → 每个候选附带完整证据清单（`event_id`/来源/发生与接收时间/指标）、分数分项与降级原因（`why_rising`）。
- 晚到数据污染历史判断 → 严格区分**事件发生时间 `occurred_at` 与接收时间 `received_at`**；榜单只纳入 `as_of` 之前"已经收到"的信号，晚到数据进快照的 `excluded_late` 留痕，只影响后续版本。
- 策划保存后榜单被悄悄改动 → 版本快照整盘冻结（SQLite 只增不改 + 内容寻址版本号），同一 `as_of` 不可二次保存。
- 编辑反馈左右历史 → 跟进/搁置/误判**只追加**、必须带理由，排序按决策时间截断，历史版本物理上不可变。
- 重启后状态漂移 → 去重正本规则与摄入顺序无关，版本/决策/去重结果全部落 SQLite，重启逐字节一致。

## 目录结构

```
fixtures/
  signals.json        # 乱序、含重复投递与晚到的信号样例
  taxonomy.json       # 分类词表：分类、关键词、受众契合度
  radar_config.json   # 观察窗口、来源权重、评分权重、降级系数、反馈加权
src/topic_radar/
  contracts.py    # 信号信封（必须带 UTC 偏移，保留未知扩展字段）
  timeutil.py     # 时间归一化（内部统一 UTC 存储）
  config.py       # 可配置窗口与权重
  taxonomy.py     # 题材分类与受众契合
  repository.py   # SQLite：信号、同源去重台账、冻结版本、追加式决策
  scoring.py      # 纯函数评分引擎 + 相邻版本 diff
  service.py      # 用例编排：摄入/预览/固化/候选身世/决策
  api.py          # 标准库 HTTP API
  cli.py          # 命令行
tests/            # unittest，python -m unittest discover -s tests -v
```

要求 Python 3.11+，**无第三方依赖**。本地缓存默认写 `radar_state/`（已在 `.gitignore`）。

## 快速开始

```bash
export PYTHONPATH=src
python3 -m topic_radar.cli ingest fixtures/signals.json
python3 -m topic_radar.cli preview --as-of 2026-09-10T15:00:00+08:00
python3 -m topic_radar.cli save-version --as-of 2026-09-10T15:00:00+08:00 --label 午间策划会
python3 -m topic_radar.cli decide 热血江湖录 --action misjudge \
    --reason "编辑核实为旧闻翻炒" --editor 阿闻 --at 2026-09-10T15:30:00+08:00
python3 -m topic_radar.cli save-version --as-of 2026-09-10T17:00:00+08:00 --label 傍晚复盘
python3 -m topic_radar.cli versions
python3 -m topic_radar.cli diff <新版本号>
python3 -m topic_radar.cli candidate 闪婚甜宠
python3 -m topic_radar.cli serve --port 8000
```

## HTTP API

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/radar/preview?as_of=...` | 实时预览，不固化 |
| POST | `/versions` `{"as_of","label"}` | 固化榜单；同一 `as_of` 返回 400 |
| GET | `/versions` / `/versions/{id}` | 版本列表 / 完整冻结快照（含配置与词表） |
| GET | `/versions/{id}/diff?from=...` | 与相邻（或指定）版本的名次、分数、证据集合、降级标记变化 |
| GET | `/candidates/{topic}` | 候选身世：上升原因、证据、版本时间线、相邻变化、人工决策 |
| POST | `/candidates/{topic}/decisions` | `{"action","reason","editor"}`，action ∈ follow/shelve/misjudge，reason 必填 |
| POST | `/ingest` | 追加信号信封 JSON 数组 |
| GET | `/duplicates` | 同源重复/正本替换留痕 |

## 评分与降级规则

- **热度**：热度窗口（默认近 2 小时，按 `occurred_at` 切分）内各信号的指标按来源权重求和，再做饱和归一。
- **增速**：当前窗口相对上一等长窗口的热度贡献变化率；上一窗口无信号时按"新题材"处理并打 `newcomer` 标记。
- **竞争密度**：同分类在热度窗口内活跃的竞品越多，密度分越高（从未分类题材之间不互相计为竞品）。
- **受众契合**：题材命中分类词表关键词，取该分类契合度；未命中则走低契合度并打 `uncategorized`。
- **显式降级**（乘数叠加，全部出现在候选的 `flags` 和 `why_rising` 中）：
  `low_confidence`（置信度不足，含来源置信与信封 `confidence` 扩展字段）、
  `insufficient_sources`（独立来源数不足）、`uncategorized`、`cooling`（窗口内无新事件）、
  `unknown_source`（配置外来源，按默认权重处理）、`contains_late_arrival`（含延迟送达证据）。
- **人工反馈**：在降级后分数上做可配置加减，且按决策时间截断——未来的决策不影响历史时刻重算。

所有阈值、窗口、权重都在 `fixtures/radar_config.json`，分类与关键词在 `fixtures/taxonomy.json`，无需改代码即可调整。

## 不变量（由测试守护）

1. `(source, event_id)` 相同时保留 `received_at` 最早的副本为正本，乱序/重放结果一致，重复投递在 `duplicates` 留痕。
2. `received_at > as_of` 的信号对该版本不可见；冻结后的快照不随后续摄入或决策发生任何字节变化。
3. 决策只追加、必须带理由；历史版本重算时按 `decided_at <= as_of` 截断。
4. 相同输入在全新数据库中生成相同的内容寻址版本号。
5. 重启后版本快照、决策、去重台账一致，重复整批重放仍全部识别为重复。
