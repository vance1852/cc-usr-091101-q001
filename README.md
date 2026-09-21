# 漫剧选题机会雷达

把各平台采集记录、站内搜索反馈与编辑观察整理成**可追溯的候选题材**：
双轨时间（发生 / 接收）、同源去重、四维评分、显式降级、不可改写的版本封存，
以及面向策划的可解释候选详情。不是来源不明的热词排行。

## 它如何回答业务问题

| 业务要求 | 实现方式 |
| --- | --- |
| 区分事件发生时间与接收时间 | 评分只看 `received_at ≤ 版本 cutoff` 的信号；信号落在观察窗近期还是前窗由 `occurred_at` 决定 |
| 热点两小时内翻转，晚到数据不能改旧决策 | 同一 cutoff 的榜单保存即冻结（SQLite 触发器禁止 UPDATE/DELETE）；晚到信号只进入后续版本 |
| 合并同源重复记录 | `(event_id, source)` 相同视为重复投递，最早 `received_at` 胜出，结果与加载顺序无关，全部到达记录留存 |
| 热度 / 增速 / 竞争密度 / 受众契合度 | `scoring.py` 四维各 0–1：对数热度（榜内归一）、近窗对前窗速率、供给侧密度取反、分类词表 × 受众偏好 |
| 来源缺失或置信不足明确降级 | 未知来源、低置信、证据稀薄、信号滞后、指标冲突均乘降级系数并在 `degradation` 中逐条写明，不静默丢弃 |
| 跟进 / 搁置 / 误判 + 理由 | 反馈只追加（不可改不可删），按时间边界影响**后续**版本排序，历史分数不变 |
| 打开候选就知道为何上升 | 候选详情含人话解读、分数拆解、逐条证据（发生/接收/滞后秒数/重投次数）、相邻版本 diff 与证据增删 |
| 重启后一致 | 去重结论与版本快照落库冻结；重复喂入相同数据零新增、零覆盖 |

## 目录

```
config/taxonomy.json     分类词表与本工作室受众偏好（可改）
config/radar.json        来源画像、观察窗（默认 6h）、权重、阈值、降级系数、反馈调整分
fixtures/signals.json    契约样例（含重复/迟到）
fixtures/sample_feed.json 乱序全量样例（20 条：重投、指标冲突、未知来源、坏信封、晚到）
src/topic_radar/
  contracts.py           信号信封（接入契约，保留未知扩展字段，时间必须带 UTC 偏移）
  config.py              配置加载
  ingest.py              校验 + 同源去重（坏行隔离而非中断整批）
  scoring.py             四维评分与降级
  store.py               SQLite 只追加封存层 + 防篡改触发器
  service.py             版本封存、反馈生效边界、相邻版本对比
  api.py                 标准库 HTTP API（无第三方依赖）
  cli.py                 命令行
```

要求 Python 3.11+，无第三方依赖。

## 快速开始

```bash
# 导入乱序样例
python -m topic_radar.cli --db radar.db ingest --file fixtures/sample_feed.json

# 封存 19:00 版：此刻之后才接收到的信号（sig-140/sig-141）不会进入
python -m topic_radar.cli --db radar.db save --cutoff 2026-09-10T19:00:00+08:00 --note "脚本会前快照"

# 看榜与候选详情
python -m topic_radar.cli --db radar.db board --version 1
python -m topic_radar.cli --db radar.db candidate --version 1 --topic 非遗悬疑

# 编辑决策（补录历史决策需用 --at 指定决策时间，否则按当前时间）
python -m topic_radar.cli --db radar.db feedback --topic 逆袭短篇 \
  --decision misjudge --reason "无复仇题材编剧储备" --editor 策划A \
  --at 2026-09-10T19:30:00+08:00

# 热点翻转后封存新版本；旧版本分数不变
python -m topic_radar.cli --db radar.db save --cutoff 2026-09-10T21:00:00+08:00
```

## HTTP API

```bash
python -m topic_radar.api --db radar.db --host 127.0.0.1 --port 8000
```

| 方法与路径 | 说明 |
| --- | --- |
| `POST /api/signals` | 上报信号信封数组（返回接收/去重/拒收统计） |
| `GET  /api/versions` | 已保存版本列表 |
| `POST /api/versions` | `{"cutoff": "带偏移的 ISO 时间", "note": ""}` 封存新版本 |
| `GET  /api/versions/<id>` | 该版本候选榜（排名 + 分数拆解） |
| `GET  /api/versions/<id>/candidates/<topic>` | 可解释详情：上升理由、证据、降级、相邻版本变化、反馈史 |
| `POST /api/feedback` | `{"topic","decision","reason","editor","created_at"?}`，decision ∈ follow_up/shelve/misjudge |
| `GET  /api/feedback?topic=` | 反馈历史（只追加） |

## 关键纪律

- **版本不可改写**：重复保存同一 cutoff 直接报错；数据库层面触发器阻止对版本、候选快照、证据、原始到达、反馈的 UPDATE/DELETE。
- **反馈不篡改历史**：只有 `created_at ≤ 版本 cutoff` 的反馈参与该版本；封存后补的反馈只影响后续版本。
- **未知扩展字段保留**：信封中 `metrics` 之外的字段（如 `region`、`confidence`）原样存入证据。
- **本地缓存不入库**：`*.db`、`.env`、凭据等已在 `.gitignore`。

## 测试

```bash
python -m unittest discover -s tests -v
```

覆盖：去重确定性与顺序无关性、双轨时间分桶、晚到数据只影响后续版本、
各类降级、竞争密度、触发器防篡改、反馈时间边界、相邻版本 diff、
重启后榜单/去重一致（重复喂入零新增）、HTTP API 端到端。
