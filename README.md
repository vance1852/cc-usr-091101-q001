# 漫剧选题信号协议

本项目保存自媒体选题雷达的公开接入契约。不同平台的趋势信号先转换成统一信封，后续排序服务据此区分信号发生时间、接收时间和来源。

`fixtures/signals.json` 含重复与迟到记录，`src/topic_radar/contracts.py` 负责基础字段校验。运行 `python -m unittest discover -s tests -v` 可验证样例契约。

项目要求 Python 3.11 或更高版本。业务实现应保留未知扩展字段，时间必须包含 UTC 偏移，本地缓存、数据库和凭据不进入版本库。
