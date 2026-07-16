# astock-data-toolkit — A股全市场数据本地化工具箱

一套经过**生产环境验证**的 A 股数据获取管线：行情 / 估值 / 财务 / 分红落 Parquet，公告 / 财务摘要 / 增减持落 SQLite，支持 15 年历史回填与每周全自动增量更新。

- 覆盖全 A 股约 5000 只（沪深主板 / 创业板 / 科创板，不含北交所），2011 年至今
- 断点续传、限频、多源容错——每一处设计都是被真实的封禁和数据坑逼出来的（见下方[踩坑实录](#踩坑实录)）
- 一条 cron 挂上后每周全自动增量，行情和公告一起更
- **本仓库只提供程序，不提供任何数据文件**——数据自己跑出来，归你自己

## 数据覆盖

### Parquet（量化回测用）

| 表 | 内容 | 数据源 |
|---|---|---|
| `stock_list` | 全 A 股名单 | akshare |
| `index_daily` | 沪深 300 指数日线（OHLCV） | akshare |
| `daily_ohlcv` | 个股日线行情（前复权 OHLCV） | 腾讯（经 akshare） |
| `valuation_daily` | pe_ttm / pb / ps_ttm / pcf_ttm / total_share / total_mv | baostock + 巨潮股本变动融合 |
| `financial_quarterly` | ROE / 营收 / 净利润（季报） | 新浪（经 akshare） |
| `dividend_history` | 分红送转记录 | akshare |

### SQLite `info_archive.db`（信息检索用）

| 表 | 内容 | 数据源 | 参考体量 |
|---|---|---|---|
| 公告 | 全 A 股公告元数据（标题 + PDF 链接 + 分类），可回填至 2011 | 巨潮资讯 | 全量约 630 万条 / 2.5GB |
| 财务摘要 | 新浪 abstract 精选 28 指标宽表，历史最早到 1998 | 新浪 | 约 30 万行 |
| 增减持 | 沪深北三所董监高及股东增减持，统一表结构 | 三所官网 | 约 19 万条 |

## 快速开始

环境：Python 3.9+，Linux / Windows 均可（长任务建议 Linux 服务器）。

```bash
pip install -r requirements.txt

# 行情五表：全量下载（首跑约 10 小时，受限频决定）
python download_astock_data.py

# 常用操作
python download_astock_data.py --step 2 --resume   # 单表断点续传
python download_astock_data.py --incremental        # 全部增量
python download_astock_data.py --step 99            # 数据完整性验证

# 信息库回填（可选，公告全量约 2 天）
python info_backfill_announcements.py
python info_backfill_fin_abstract.py
python info_backfill_holder_changes.py
```

数据默认写到**脚本所在目录**下的 `astock_data/`，可用环境变量 `ASTOCK_HOME` 指定其它主目录。

### 每周自动更新

```
# crontab（周六凌晨跑，收盘数据齐全，跑一夜也不影响任何人）
0 2 * * 6  PYTHON=/path/to/venv/bin/python3 /path/to/astock-data-toolkit/run_weekly_update.sh
```

`run_weekly_update.sh` 特性：lock 防并发、progress 先备份、单步失败不中断整体、跑前跑后记录文件大小、行情（Step 1~5）与信息库（Step 6）一起增量。若有下游服务依赖数据，用环境变量 `POST_UPDATE_CMD` 指定更新后钩子（如 `"pm2 restart my-service"`）。

## 目录结构

```
astock-data-toolkit/
├── download_astock_data.py              # 核心：行情五表全量/增量（Step 0~5 + 99 验证）
├── download_valuation_baostock_v4_full.py  # 估值表重建（baostock+巨潮融合，精度见踩坑实录#2）
├── download_cninfo_full.py              # 巨潮全市场股本变动下载（4 线程）
├── info_backfill_announcements.py       # 公告元数据回填 → SQLite
├── info_backfill_fin_abstract.py        # 财务摘要回填 → SQLite
├── info_backfill_holder_changes.py      # 三所增减持回填 → SQLite
├── info_weekly_update.py                # 信息库周增量（公告驱动财报重拉）
├── run_weekly_update.sh                 # 周更总入口（cron 挂它）
└── backfill_15y/                        # 15 年历史回填模块
    ├── orchestrator.sh                  # 六阶段串行总调度（setsid 脱终端，状态落 JSON）
    ├── run_backfill.sh                  # 单表回填 + 自动挂看门狗
    ├── mem_watchdog.sh                  # 内存看门狗：可用内存不足自动 SIGSTOP，恢复后 SIGCONT
    ├── backfill_15y.py                  # 四表历史回填主逻辑
    ├── refetch_cninfo_full.py           # 股本变动扩展重拉（上市日起）
    └── merge_backfill.py                # 回填数据合入生产表（先 dry-run，人工确认后 --commit）
```

## 15 年回填模块说明

小内存服务器上跑多天长任务的一套工程实践，可整体照搬到其它长任务上：

1. `orchestrator.sh` 用 `setsid` 启动，SSH 断开不受影响；每阶段状态写 `master_status.json`，随时可查
2. `mem_watchdog.sh` 每 30 秒检查 `MemAvailable`，低于阈值 `SIGSTOP` 暂停回填进程，回升后 `SIGCONT` 恢复——防止合并阶段内存峰值触发 OOM killer 误杀
3. 合并采取**两段式**：先 `--dry-run` 输出将要变更的行数供人工审核，确认无误再 `--commit`，生产表在此之前只读不写

## 踩坑实录

同类工具的文档不会告诉你这些，但每一条都会在你全量跑到 60% 的时候找上门：

1. **东方财富接口会封 IP**。`_em` 后缀接口高频拉取一次全量后 IP 即被拉黑（亲测，至今未解封）。本管线全程不依赖东财：日线走腾讯源，估值走 baostock + 巨潮。
2. **百度估值接口有隐形下采样**。"近五年"参数最多返回约 1100 行，即隔日采样——用它做日粒度估值会有 44% 缺失。本管线的 V4 方案：pe/pb 取 baostock，股本时间线取巨潮股本变动 + 季报兜底，`close × total_share` 算真实市值，与主流口径中位差仅 0.017%。
3. **巨潮全市场公告查询的 `hasMore` 恒为 true**（实测翻 3000 页都不停）。翻页终止必须靠"连续 N 页零新增"判断，不能信接口字段。
4. **baostock 服务端限 4 并发**。开 8 进程没有任何提升；同进程多线程共享 TCP 会随机 decode error，必须多进程。
5. **北交所（920xxx）多数接口的返回列名与主板不同**，直接跑会 KeyError。本管线默认剔除北交所。
6. **新浪财务接口需要真实 UA + 4~9 秒随机限速**，否则封。周更全量跑约 24 小时属正常，别误判为卡死。
7. 已知局限：A+H 股（如比亚迪）的 total_mv 只含 A 股股本。

## 硬件与耗时参考

- **磁盘**：最终数据约 5GB（行情 Parquet 约 1.5GB + 公告库约 3GB），加上临时文件与备份建议预留 **≥ 20GB**
- **内存**：4GB 可跑；合并阶段有峰值，回填模块自带内存看门狗自动调节
- **耗时**（单 IP，由各源限频决定，不建议提速——会封）：行情五表首跑约 10 小时；15 年回填约 3~5 天；公告全量回填约 2 天；每周增量一夜跑完

## 生产应用：AIHEY（艾嘿）

这套管线是 **AIHEY AI 助理**「信息模块 / 量化回测」功能的数据底座——公告事件回放、财报白话卡、历史策略回测等都跑在它产出的数据上。不想自己架服务器跑数据，可以直接用现成的：

- **网页版（免下载）**：https://agentos.tybbtech.com/app/
- **iOS 版**：[App Store 下载](https://apps.apple.com/cn/app/agentos/id6759725374)（搜索「AgentOS」）
- **桌面版（Windows / Mac）、安卓版**：[官网下载](https://www.tybbtech.com)

## 免责声明

- 本项目仅供**学习与研究**使用，请勿用于任何商业数据转售场景
- 所有数据版权归各数据源（上交所、深交所、巨潮资讯网、新浪财经、腾讯财经、baostock 等）所有，使用时请遵守各数据源的服务条款，控制访问频率
- 本项目仅提供数据获取程序，不提供任何投资建议；数据仅反映历史，不预示未来

## License

[MIT](LICENSE)
