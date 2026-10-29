# EthVis2 Collector v0.1

首版使用 **Go 1.27.3** 采集，用 **Node.js 24.22.1** 做研究验证。PostgreSQL 只管理任务/进度/归档索引，ClickHouse 保存事实和查询视图。完整源响应独立存为 SHA-256 寻址的 gzip 文件。

## 配置

已预留未填写的 `.env`（权限 600，不纳入版本管理）。填写：

```dotenv
TEKU_API=http://127.0.0.1:5051
POSTGRESQL_API=postgresql://user:password@127.0.0.1:5432/ethvis_control?sslmode=disable
CLICKHOUSE_API=http://127.0.0.1:8123
CLICKHOUSE_DATABASE=ethvis
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=
```

PostgreSQL 填已有数据库的 **DSN**，不是 HTTP 接口；ClickHouse 填 **HTTP 接口**，一般为 8123，不能填 native 9000。远程服务可使用 https。ClickHouse 也可在 URL 中写 user/password（含特殊字符需 URL encode）。程序自动加载 `.env`，不需要 `source .env`，已有环境变量优先。模板见 [.env.example](.env.example)。

`.env` 不执行 shell 展开；推荐路径使用绝对路径或相对当前工作目录的路径，不能填 `~/...` 期待自动展开。

## 只检查连接（不采集）

```bash
scripts/check_connections.sh
# 或指定另一个环境文件：
scripts/check_connections.sh /path/to/.env
```

脚本读取 `.env`，只请求 Teku 的 `node/version`，并对两个数据库执行 `SELECT 1`。不使用 Docker、不建表、不创建任务、不抓取区块，也不归档响应。每个服务最多等待 10 秒，三个检查都会执行，任一个失败时退出码为 1。不打印连接串或密码。

脚本借用现有 Go 驱动，因此无需额外安装 psql；首次运行可能编译代码/下载 Go 依赖。此检查只确认连通和认证/简单查询，不证明历史数据可读或建表权限完整。

### ClickHouse 返回 502 时

502/503/504 表示代理、网关或上游服务异常，不能据此断言密码错误。确认 `CLICKHOUSE_API` 指向 HTTP 服务（默认 8123/HTTPS 8443），而不是 native TCP 服务（通常 9000/9440）；自定义映射以实际服务配置为准。同时检查 shell 的 HTTP(S)_PROXY / NO_PROXY 设置，以及反向代理和服务器监听/防火墙。脚本不会自动更换端口或绕过代理。

## 构建与首次试跑

在仓库目录执行：

```bash
source /home/chen/.g/env
go version                         # go1.27.3
nvm use                            # 读取 .nvmrc：24.22.1
go build -o bin/ethvis-collector ./cmd/ethvis-collector
./bin/ethvis-collector version
./bin/ethvis-collector probe
./bin/ethvis-collector init
```

`probe` 只读 Teku 并归档网络身份、pec、fork schedule、客户端和 finalized 信息。`init` 显式创建 `ethvis_*` 控制表、ClickHouse 数据库/表/视图，不修改s节点配置，也不创建 PostgreSQL 数据库。连接角色需有相应建表权限。`collect` 不自动迁移 schema。

建议先用一个独立小范围检查真实节点/权限，再创建正式 10 万块任务：

```bash
./bin/ethvis-collector plan --run smoke_100 --blocks 100
./bin/ethvis-collector collect --run smoke_100
./bin/ethvis-collector verify --run smoke_100
```

正式范围：

```bash
./bin/ethvis-collector plan --run pilot_100k --blocks 100000
./bin/ethvis-collector collect --run pilot_100k --subset 100
./bin/ethvis-collector collect --run pilot_100k --subset 1000
./bin/ethvis-collector collect --run pilot_100k
./bin/ethvis-collector status --run pilot_100k
./bin/ethvis-collector verify --run pilot_100k
```

- `plan` 沿 finalized parent 链选取实际产生的 N 个 blocks，固定锚点，附带空 slot 和完整 epoch/结算支持上下文。规划是网络工作，10 万 header 的回溯可能较久；每 100 个 headers 保存进度，同一命令可继续。状态根规划逐 epoch 持久化。
- 主区间结束于 finalized checkpoint 前两个 epochs 之前，以留出结算支持；这只是保守选段，**奖励接口失败仍视为失败**，不以固定偏移证明已成功结算。
- `--subset` 是同一 frozen manifest 的 smoke 前缀；会扩展为完整 epoch 和支持块，因此实际请求/入库数量可能大于 100/1000。它不改变原始 core 标签，不把整个任务标为完成。
- `collect` 同一命令重跑只补未完成任务，失败退出码非零；Ctrl+C 可停止，之后恢复。相同 run 不能由两个进程同时 collect。请保持 run 文件、归档目录和 PG 控制库配套，恢复时沿用原目录。
- `.env` 改 BLOCK_COUNT 不会改变 frozen run。要换锚点/网络/范围，请使用新的 run id。
- `verify` 核查所有必需任务与 published 核心 block/reward/epoch/slot 覆盖，写 `var/runs/<run>.report.json`。它验证 **本版本采集范围**，不代表完整 Validator 收益账本已经重放验证。

## 本版实际采集

| 数据 | 实现 |
| --- | --- |
| 网络信息/客户端/fork 配置 | 完整原始归档，配置引用保存在 manifest |
| SignedBeaconBlock | 完整 JSON，保留签名、未知字段和执行 payload 中的原始交易 |
| operations | attestation、slashings、deposit、exit、BLS change、commitments、execution requests 等已知数组拆行；同步聚合/payload 保留对象 |
| slot | 由固定 canonical 祖先链形成时间轴，链内缺口标为 skipped；不把任意 API 404 解释为缺块 |
| 历史 Validator | 固定 state root 查询；保留余额 0、完整 registry 对象 |
| committees / proposer duties / sync members | 历史上下文；sync 位置原数组不提前去重 |
| finality checkpoints | 固定历史 state 查询 |
| proposer reward | 按 block root；核对 proposer identity 和分项总和 |
| sync reward | 按 block root，保存有符号 validator 级 reward |
| attestation reward | 按 epoch，actual、ideal、inactivity/适用 inclusion_delay；分项有符号整数不转浮点 |
| 完整 state SSZ | `.env` 的 `COLLECT_SSZ=true` 可开启逐 epoch 归档；非常大，默认关闭 |
| 失败响应 | 非 2xx 同样归档，429/5xx/连接错误有限重试，未解决任务留作恢复 |

Validator 和 reward 数组逐条解析，不用一个大 DataFrame 容纳全部验证者。ClickHouse 按 `BATCH_ROWS` / `BATCH_BYTES` 分批写入；单条超宽记录可单独成批。完整 block 的 JSON 目前按单 block 在内存解码，最多由 WORKERS 限制并行数；所有状态类任务额外受 STATE_WORKERS 限制。

## ClickHouse 查询与 Node.js 验证

数据库内：

- `records`：物理事实版本，含 run/task/attempt、slot/epoch、block/state roots、原始 hash 和 JSON payload。
- `published`：每个 task 的选定 attempt 与递增 generation。
- **`facts`**：逻辑去重后只读取已发布 attempt。分析应查这里和下面的视图，不直接 sum `records`。
- `proposer_rewards`、`attestation_rewards`、`sync_rewards`、`validator_snapshots`、`attestations`：类型化查询视图。

`core` 的粒度需注意：block/reward 记录表示主样本 root；epoch 记录表示与主样本相交的完整 epoch，因此边界 epoch 包含主样本 block 时间窗外的职责。两者不能当作同一个严格起止时刻的收益总额。

示例见 [analysis/overview.sql](analysis/overview.sql)。注意一个还在 collect 中的 run，其已完成任务可见但整体可能不完整；正式范围分析先运行 `verify`。失败/替代 attempt 的物理数据不进入分析视图；本版不自动删除历史 attempts 或归档。

导出已发布奖励研究样本，在 Node 中流式检查：

```bash
./bin/ethvis-collector export --run pilot_100k --output var/rewards.jsonl --limit 10000
node analysis/verify.mjs var/rewards.jsonl
npm test
```

脚本使用 `BigInt`，不将 Wei/Gwei 送入 JS Number。输出 proposer、attestation、sync 三类分项汇总；它不是完整净收益，也不把提款本金或未采的 EL/MEV 自动加进去。默认 export 只导出有限样本（上限 100000 行），不把几十亿行一次读入内存。导出支持 JSONL，验证脚本也接受 `.jsonl.gz`。

## 一致性和恢复

1. 原始 body 先完整落盘并校验寻址，相关响应头和失败状态另存 request metadata。
2. 每次执行使用独立 attempt。CH insert 不做可能重复累加的盲目重试，失败尝试保留但不发布。
3. 对成功 insert 回读核验逻辑行数和唯一键；PG 标记 validated 后发布 CH attempt，再标记 complete。
4. CH 使用 ReplacingMergeTree，查询视图显式 FINAL，防止未 merge 的重复物理行影响结果；published 本身也按 generation 去重。
5. CH/PG/文件之间没有分布式事务。发布成功但 PG 完成失败时，下次新 attempt 的更高 generation 覆盖选定结果；不会将两份收益相加。
6. 存储/发布失败停止继续抓取，失败项保留。普通单个源接口失败不会静默跳过或伪造 0。

本版选择 **ClickHouse HTTP + JSONEachRow**，便于直接填写 API URL并保持少量依赖；这是代码设计中 native driver 方案的首版简化。PG 使用 pgx；共识层通过固定 root 的原始 REST 读取，参考 chaind 的结构，不引入其 PG 链上明细后端。

## 本版边界

- 不采 Geth receipts、trace、relay；没有完整 EL tips/MEV 收益归因。后续可加可选 GETH_API，当前无需填写。
- 历史状态边界明确标为 `node_slot_state` / `pre_block_verified=false`。尚未实现精确 pre-block state 重放、participation flags 解码、slashing 余额账本和余额残差对账；`verify` 报告会显式列出。
- 未做全量 attestation 成员展开及正确性/机会损失计算；完整 bits、委员会、源签名与原始数据已保存供后续派生。
- 还未采实时 SSE/fork-choice、blob bytes/data columns。Block 内 commitments 和完整原始字段保留；archive 节点不保证能恢复旧网络观测。
- 不做 BLS/SSZ block root 密码学重算；当前核验节点给出的 canonical parent 链与 block/header 身份一致。未知 block 字段保留，但不声称解析了所有未来 fork 的收益规则。
- 默认开启全体 Validator/epoch 和 reward 数据采集。10 万块即使 block 表很小，validator × epoch 仍可能数十亿行。先完成 smoke 并实测容量；本版不自动配置 TTL 或删除数据。

## 测试

```bash
source /home/chen/.g/env
nvm use
go test -race ./...
go vet ./...
npm test
```

数据库集成测试在未配置 `ETHVIS_TEST_*` 测试连接时会自动跳过。连接真实服务只需使用上述 `scripts/check_connections.sh`；不要把正式数据库连接填入测试变量。

详细设计：[数据清单](DATA_COLLECTION_DESIGN.md)、[代码设计](COLLECTOR_CODE_DESIGN.md)。两份设计描述的是目标架构；可执行范围以本 README 的 v0.1 列表为准。


## 遇到 HTTP 500、损坏 JSON 或超时后恢复

同一个 frozen run 可以补采：

```bash
./bin/ethvis-collector status --run smoke_100
WORKERS=1 STATE_WORKERS=1 HTTP_TIMEOUT_SECONDS=900 ./bin/ethvis-collector collect --run smoke_100
./bin/ethvis-collector verify --run smoke_100
```

先确认上一次 collect 已退出。保留 PG 控制表、`var/runs/smoke_100.json`、原始归档和 CH 数据，不要重新 plan 或换 run id。`complete` 任务跳过，`error/running/validated` 恢复到 pending。失败 block 任务包含原始区块与两类 block reward，恢复时按整个任务重做，而不是只补那一次失败 HTTP；逻辑视图按 attempt 去重。

- HTTP 500 需要检查已归档正文，不能据状态码推断历史数据被裁剪。新版错误日志带原始 SHA-256；若正文是 `Request Timeout`，说明 HTTP 服务端或中间层报告超时。
- HTTP 200 也可能包含被中断的 JSON，甚至在 JSON 中混入 `Request Timeout`。采集器会以有界内存验证完整 JSON，保留损坏响应作为证据，再按 `HTTP_ATTEMPTS` 重试，绝不把半份数组发布为成功数据。
- 响应头之后的传输截断、EOF 和客户端读取超时现在也会重试。磁盘错误、上层主动取消和不可重试 HTTP 错误不会盲目重试。
- `HTTP_TIMEOUT_SECONDS` 仅控制客户端；即使设为 900，也不能阻止服务端或反向代理更早结束响应。需要在 Teku 主机核查同时间段日志、历史 state 重建与 CPU/I/O/内存负载，以及适用版本的 REST/代理超时配置。
- 最后一条 collector stopped 是本轮失败汇总，可能只回显第一项错误。先看 task 状态和 coverage，不能据最后一行认为其余任务都失败。

本次仅排障时，不要用 `init` 清理数据库或删除已有成功结果。恢复后以 `verify` 确认必需任务完成；它仍不代表已完成精确余额重放对账。
