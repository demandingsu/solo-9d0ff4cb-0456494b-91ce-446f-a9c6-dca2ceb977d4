# 双端 FASTQ 增量采集与后台拆样服务（FastAPI + SQLite）

测序中心数据员从仪器中转站接收持续追加的双端读段（`R1.fastq` / `R2.fastq`），
按读段名配对后交给下游比对。采集完成的混样运行再按上机样本表拆成各样本
与未分配桶的 R1/R2 文件。本项目提供：

- **采集端（collector）**：FastAPI + SQLite，通过 HTTP Range 增量拉取、
  四行 FASTQ 校验、按名称配对、字节级断点续采，全部配对完成后可下载 R1/R2；
  **同一进程内还提供后台拆样 API**，拆样期间采集与查询照常响应；
- **拆样（demux）**：按两端前 4 碱基（每端最多 1 处替换）把配对读段归入
  样本桶或未分配桶，分块检查点 + 崩溃安全发布，支持写盘失败/状态未保存的
  重启恢复，同请求号同参复用、异参冲突、并发提交只执行一次；
- **仪器中转站（relay）**：本地模拟服务，支持 HTTP Range、分段追加、
  单端（R2）延迟写入、完成标志 `DONE` 与可控制的连接中断窗口；
- **一键演示（demo / demo-demux）**：采集中断续采演示，以及拆样的
  歧义归属与两类失败恢复演示。

## 目录结构

```
.
├── collector/            # 采集端
│   ├── app.py            # REST API（采集/进度/下载/健康检查 + 挂载 demux 路由）
│   ├── service.py        # Range 续采 + 配对状态机 + 终态校验
│   ├── parser.py         # 字节级 FASTQ 解析与结构校验
│   └── db.py             # SQLite 模式与单事务配对提交
├── demux/                # 后台拆样
│   ├── app.py            # /demux REST 路由（提交/查询/下载门控）
│   ├── service.py        # 工作线程、分块检查点、崩溃安全发布、故障注入
│   ├── persistence.py    # 拆样任务/桶/分块的 SQLite 持久层
│   └── matcher.py        # 两端 4 碱基匹配（精确优先，每端最多 1 替换）
├── relay/                # 模拟仪器中转站
│   └── app.py            # Range 文件服务 + 追加写线程 + 中断控制
├── scripts/demo.py       # 采集中断续采端到端演示
├── scripts/demo_demux.py # 拆样歧义归属 + 失败恢复端到端演示
├── tests/                # 匹配器单测 + 真实子进程集成测试（含重启恢复）
├── Dockerfile            # 通用镜像（APP_MODULE 选择入口）
├── docker-compose.yml    # relay / collector / demo / demo-demux
└── requirements*.txt
```

## 快速开始（Docker Compose）

```bash
# 启动两个常驻服务（collector 在 8000，relay 在 8001）
docker compose up -d --build

# 运行端到端演示：自动建含中断的场景、增量采集、下载校验
docker compose --profile demo run --rm demo

# 运行拆样演示：歧义归属 + 写盘失败恢复 + 状态未保存恢复
docker compose --profile demo run --rm demo-demux

# 查看日志
docker compose logs -f collector
```

交互式 API 文档：启动后访问 <http://localhost:8000/docs>。

也可手动操作：

```bash
# 1) 在中转站创建一个分段追加、R2 滞后、带连接中断的场景
curl -X POST http://localhost:8001/runs -H 'content-type: application/json' -d '{
  "run_id": "runA", "reads": 20, "append_delay": 0.2,
  "r2_extra_delay": 0.4, "chunk": 2,
  "interrupt_at_second": 1.0, "interrupt_duration": 2.0
}'

# 2) 提交采集（可任意重复调用，幂等）
curl -X POST http://localhost:8000/collect/runs -H 'content-type: application/json' \
  -d '{"run_id": "runA", "base_url": "http://relay:8000/runs/runA"}'

# 3) 查询进度
curl http://localhost:8000/collect/runs/runA

# 4) 全部配对（status=completed）后下载
curl -OJ http://localhost:8000/collect/runs/runA/R1
curl -OJ http://localhost:8000/collect/runs/runA/R2
```

> 在宿主机直接调用且不经过 compose 网络时，`base_url` 用
> `http://localhost:8001/runs/runA`。

## API 一览

### 采集端 `:8000`

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/collect/runs` | 提交/触发一次增量采集，body：`{"run_id","base_url"}`，幂等可重复 |
| GET | `/collect/runs` | 列出全部运行 |
| GET | `/collect/runs/{id}` | 查询状态、两端字节进度、配对数、错误详情 |
| GET | `/collect/runs/{id}/R1` `/R2` | 下载已配对 FASTQ，**仅 `completed` 允许**，否则 409 |
| POST | `/demux/tasks` | 提交后台拆样任务（202），同参复用 / 异参 409 / 并发只执行一次 |
| GET | `/demux/tasks` | 列出全部拆样任务 |
| GET | `/demux/tasks/{request_id}` | 查询拆样状态、各桶计数与错误详情 |
| GET | `/demux/tasks/{request_id}/download/{bucket}/R{1,2}` | 下载某桶 FASTQ，**仅 `completed` 开放**，否则 409 |
| GET | `/healthz` | 存活探针 |
| GET | `/docs` | Swagger UI 交互式文档（OpenAPI） |

进度响应示例：

```json
{
  "run_id": "runA",
  "base_url": "http://relay:8000/runs/runA",
  "status": "running",
  "r1_offset": 320,
  "r2_offset": 280,
  "pairs": 7,
  "error": null,
  "notices": ["R1 源站连接中断（HTTP 503），保留断点 440，等待重试"]
}
```

状态取值：`running`（采集中/等待追加）、`completed`（全部配对）、
`error`（结构错误、名称不匹配、完成后残条或孤读）。

### 中转站 `:8001`

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/runs` | 按场景启动后台追加任务（延迟、块大小、中断窗口均可配） |
| GET | `/runs/{id}/status` | 查看追加进度与中断窗口状态 |
| POST | `/runs/{id}/interrupt` | 手动打开中断窗口（body：`{"duration": 3}`） |
| POST | `/runs/{id}/resume` | 提前关闭中断窗口 |
| GET/HEAD | `/runs/{id}/R1.fastq` `/R2.fastq` `/DONE` | 文件服务，支持 `Range: bytes=a-b`（206/416） |

## 数据格式与配对规则

- 仅处理**未压缩** FASTQ，每条记录严格四行：
  `@名称` / 序列 / `+` / 质量串；
- R1 名称以 `/1` 结尾、R2 以 `/2` 结尾，**去掉后缀后名称一致**才配对；
- 序列与质量串必须等长，第三行必须以 `+` 开头，否则判定结构错误；
- 末条未写完（残行）或另一端滞后时**等待续采**，不产生半对；
- 源站 `DONE` 出现后，若仍有残条（`residual_records`）或孤读
  （`orphan_read`），进入 `error` 终态并回报两端字节位置；只有全部
  配对成功才置 `completed`，此时才允许下载。

## 不漏对、不重对的设计保证

1. **单事务提交边界**：每配成一对，在**同一个 SQLite 事务**里写入
   配对内容（`pairs` 表）并前推两端字节偏移（`runs` 表）。事务失败
   整体回滚，不存在"内容落库但偏移没更新"或反之的中间态。
2. **Range 断点续采**：下一次请求的起点 = 已提交偏移 + 内存残条长度；
   断线（503/连接重置/网络异常）只记录 `notices`，状态保持 `running`，
   已收字节保留，重连后从断点继续。
3. **重启恢复**：进程重启后内存状态清空，从 SQLite 中已提交的两端
   偏移重新发起 Range；已配对记录因唯一约束
   （`UNIQUE(run_id,name)` + 顺序号主键）不会重复。
4. **完成前必须确认 EOF**：源站出现 `DONE` 后，只有两端都收到明确的
   EOF 信号（416/404 且无传输错误）才做终态校验，避免把"还没拉到的
   完整记录"误判成残条。
5. **Range 一致性保护**：若源站在非零偏移处返回 200 而非 206（不支持
   Range，可能发生字节改写），直接进入 `error`，拒绝继续采集。

## 后台拆样（demultiplexing）

### 归属规则

样本表每行给出样本名与两端各 4 位 `A/C/G/T` 条码。对每条**已配对**读段，
取 R1、R2 序列行的前 4 个碱基分别匹配：

- 每端允许 **0 或 1 处替换**（Hamming 距离 ≤ 1）；
- **精确命中优先**：读段精确等于某样本条码时，即使它同时落在另一个样本
  的 1-替换邻域内，也只算精确样本，不会被稀释成歧义；
- 两端候选交集**唯一**才归属该样本；
- 任一端序列**不足 4 碱基**、两端交集为空（**无匹配**）、或交集含多个
  样本（**多重匹配/歧义**），一律归入 `unassigned` 未分配桶；
- 桶内严格保留读段的**原始四行记录**与**原始顺序**。

### 提交与幂等

```bash
curl -X POST http://localhost:8000/demux/tasks -H 'content-type: application/json' -d '{
  "request_id": "job-20261114-01",
  "run_id": "runA",
  "sample_sheet": [
    {"sample_name": "S1", "barcode_r1": "AAAA", "barcode_r2": "CCCC"},
    {"sample_name": "S2", "barcode_r1": "GGGG", "barcode_r2": "TTTT"}
  ]
}'
```

- `run_id` 必须是采集端已 `completed` 的运行，否则 422；
- 条码必须为 4 位 `A/C/G/T`，样本名/条码组合不得重复，不能使用保留名
  `unassigned`，否则 422 且不建任务；
- 同 `request_id` + 同运行号 + 同规范化样本表 → **复用**（响应体
  `reused=true`），已完成直接返回，运行中不重复启动工作线程；
- 同 `request_id` 但运行号或样本表不同 → **409 冲突**；
- 并发提交同一请求号由进程内活动标志与 SQLite 主键共同保证**只执行一次**；
- 拆样在后台守护线程执行阻塞式 IO，不占用事件循环，**拆样期间采集、
  查询、下载接口全部照常响应**。

状态响应示例：

```json
{
  "request_id": "job-20261114-01",
  "run_id": "runA",
  "status": "running",
  "total_pairs": 1200,
  "processed_pairs": 400,
  "output_dir": null,
  "error": null,
  "buckets": [
    {"bucket": "S1", "pair_count": 210},
    {"bucket": "S2", "pair_count": 180},
    {"bucket": "unassigned", "pair_count": 10}
  ]
}
```

状态取值：`pending`（已登记）、`running`（含崩溃后待重试）、
`completed`（整组文件与计数齐备，开放下载）、`failed`（写盘失败，可重试）。

### 输出与下载门控

任务目录位于持久卷 `DEMUX_OUTPUT/{request_id}/`：

```
output/
├── MANIFEST.json          # 总对数、各桶计数、样本表（原子写）
├── sample_sheet.json      # 规范化样本表
├── S1_R1.fastq / S1_R2.fastq
├── S2_R1.fastq / S2_R2.fastq
└── unassigned_R1.fastq / unassigned_R2.fastq
```

只有所有桶的 R1/R2 与 MANIFEST 全部落盘、桶计数之和与源运行总对数一致、
状态置为 `completed` 后才开放下载；任何未完成状态下载均返回 409。
0 对的桶同样生成空文件，保证整组文件齐备。

```bash
curl -OJ http://localhost:8000/demux/tasks/job-20261114-01/download/S1/R1
curl -OJ http://localhost:8000/demux/tasks/job-20261114-01/download/unassigned/R2
```

### 崩溃安全与重启恢复

1. **不可变分块 + 单事务检查点**：配对读段按 `DEMUX_CHUNK_PAIRS`
   （默认 100）切成只写一次的分块文件，`fsync` 后在**单个 SQLite 事务**
   里登记分块、累计桶计数、前推 `processed_pairs`。事务未提交的分块
   在恢复时作为残留整体清理，已提交分块绝不重写。
2. **写盘失败**：先落 `failed` 状态，再以非零码退出进程；compose 的
   `restart: unless-stopped` 拉起后，用**原请求号、原样本表**重新提交，
   分块与磁盘一致则从 `processed_pairs` 续跑（不重复读段），发现任何
   残留/缺块/尺寸不符则清空 tmp 从头重来（不混入残留文件）。
3. **文件完成而状态未保存**：最终 R1/R2 与 MANIFEST 已原子发布、但
   `completed` 状态落库前崩溃时，重启重试先校验发布完整性，通过则
   **只补状态、不重读任何读段**。
4. **已成功任务**：状态与文件都在持久卷上，重启后仍可查询与下载。

请求体支持两个仅用于演示/测试的故障注入字段（不参与同参比对，且只对
首次执行生效，重试自动剥离）：`fault_after_pairs`（处理到该对数模拟
写盘失败退出）、`fault_before_status`（模拟状态落库前崩溃）。

## 测试

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
python -m pytest tests/ -q
```

测试包含 39 个用例：

- 解析器偏移/残行/结构校验单元测试；
- 拆样匹配器单元测试（精确优先、每端 1 替换、不足 4 碱基、无匹配、歧义）；
- 真实 uvicorn 子进程下的端到端集成测试——增量配对、R2 滞后、连接中断
  续采、**采集进程重启后从 SQLite 断点恢复**、名称不匹配/结构错误/残条/
  孤读的错误终态、Range 206/416、下载门控；
- 拆样集成测试——归属规则与桶计数、桶内原始顺序、同参复用/异参 409/
  并发只执行一次、写盘失败退出后**重启按检查点续跑（不重复、不混残留）**、
  分块损坏/残留时**清空后从头重跑**、"文件完成而状态未保存"的只补状态
  恢复、已完成任务重启后仍可下载。

## 环境变量

| 变量 | 服务 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `COLLECTOR_DB` | collector | `/data/collector/collector.db` | 采集 SQLite 文件路径 |
| `DEMUX_DB` | collector | 与采集库同目录 `demux.db` | 拆样任务 SQLite 文件路径 |
| `DEMUX_OUTPUT` | collector | `/data/demux` | 拆样分块与最终文件的持久卷根目录 |
| `DEMUX_CHUNK_PAIRS` | collector | `100` | 每个不可变分块包含的配对数（检查点粒度） |
| `COLLECTOR_TIMEOUT` | collector | `10` | 单次 HTTP 超时秒数 |
| `RELAY_DATA` | relay / demo-demux | `/data/relay` | 运行目录根路径 |
| `RELAY_URL` / `COLLECTOR_URL` / `RUN_ID` | demo | 见 `scripts/demo*.py` | 演示目标 |
