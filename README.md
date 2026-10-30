# EthVis2 Collector v0.3

Go **1.27.3** 采集，Node.js **24.22.1** 独立核验。默认采集完整共识层区块及 validator 的 attestation 包含明细；不请求奖励接口或全量 validator 注册表，不计算收益。ClickHouse 保存 typed facts，PostgreSQL 管理任务、依赖和 attempt，本地 gzip 保存完整原始响应。

## 构建与配置

```bash
source /home/chen/.g/env
nvm use
make build
./bin/ethvis-collector version
# ethvis-collector v0.3.0 go1.27.3 schema=3
```

现有 `.env` 保留使用，模板见 [.env.example](.env.example)。Teku 填 REST URL，PostgreSQL 填已有数据库 DSN，ClickHouse 填 HTTP URL（通常 8123，9000 通常是 native TCP）。程序自行解析 `.env`，不执行 shell 展开；已有进程环境变量优先。不需要 Docker 或 Python。

```bash
scripts/check_connections.sh
```

连接检查只请求 Teku node/version 和两个数据库 SELECT 1，不建表、不采集、不归档，每个服务限时 10 秒。502/503/504 不能证明密码错误，应检查代理、服务端监听和 HTTP 端口。

## 清理旧测试数据后重新跑 100 块

v0.3 schema 与旧版不同，不迁移旧 `smoke_100`。先停止旧采集进程，在仓库根目录查看并执行项目测试清理：

```bash
./bin/ethvis-collector reset
./bin/ethvis-collector reset --execute
```

`reset` 默认只列清单，`--execute` 才实际删除。仅清理列出的 EthVis2 PG 表、配置的 CH 数据库内本项目对象，以及 ARCHIVE_DIR/RUNS_DIR；保留 `.env`，不触碰 Teku/Geth 链数据库。自动文件清理要求两个目录均位于本仓库 `var/` 下且不含符号链接；外部自定义归档目录需要单独处理。不会删除整个 PostgreSQL/ClickHouse 数据库，也不会在 collect 启动时自动清理。

清理后手动初始化新表：

```bash
./bin/ethvis-collector init
```

## 从 Plan 开始手动执行

以下每一步单独执行，确认结果后再进入下一步；在仓库根目录运行，已填写 `.env` 并完成 schema v3 的 init。

**1. Plan：冻结 100 个实际区块的范围。**

```bash
./bin/ethvis-collector plan --run smoke_votes_100 --blocks 100 --profile blocks_votes
```

读取 Teku 的网络、spec、finalized 信息，从 finalized anchor 沿 parent headers 回溯 100 个实际 canonical blocks。生成 RUNS_DIR 下的 `smoke_votes_100.json`（默认 `var/runs/smoke_votes_100.json`），记录固定 roots、slots 和 profile。空 slot 不计入 100。这一步会归档元数据响应，但不采集完整区块正文，不向两个数据库写入任务或区块数据；不会调用奖励/全量 validators 接口。同名同参数再次 plan 读取已冻结清单，不移动范围。

**2. Collect：采集正文、入库并展开成员投票。**

```bash
WORKERS=1 STATE_WORKERS=1 HTTP_TIMEOUT_SECONDS=7200 \
  ./bin/ethvis-collector collect --run smoke_votes_100
```

读取刚才的固定清单，将任务登记到 PG，归档完整区块，拆解写入 CH；根据实际 attestations 获取对应历史委员会，再展开 validator 成员、所投内容及包含位置。此命令内部会完成这些采集阶段，不计算收益。三个行内环境变量只覆盖本次命令，不修改 `.env`。失败后重跑同一条 collect，可复用成功任务和归档，不要重新 plan 改范围。

**3. Status：查看任务进度。**

```bash
./bin/ethvis-collector status --run smoke_votes_100
```

读取 PG 任务状态，分别显示区块/投票任务完成情况以及失败、blocked 数量；不重新采集。可以在 collect 结束后查看，也可以从另一个终端观察。collect 完成后尚未 verify 时，`profile_complete` 仍可能为 false。

**4. Verify：检查数据库中的覆盖和发布一致性。**

```bash
./bin/ethvis-collector verify --run smoke_votes_100
```

检查 required tasks、封存的 attempt、100 个冻结区块和 slot 覆盖、逐 aggregate 成员数量及唯一性。通过后写入本地报告并将 PG run 标记为 verified；不重新从 Teku 采集，也不计算收益。

**5. Export：准备 Node.js 独立核验所需的完整数据包。**

```bash
./bin/ethvis-collector export --kind votes --run smoke_votes_100 \
  --output var/verification/smoke_votes_100
```

先再次确认当前发布结果可核验，再导出 manifest、publication、原始区块/委员会、CH 已发布成员行和 checksum。按块完整导出，无默认行数截断，不启动新一轮采集。输出目录必须不存在；重复导出可指定新的目录名。

**6. Node.js：从原始输入独立重算并比对投票映射。**

```bash
node analysis/verify-votes.mjs --input var/verification/smoke_votes_100
```

使用 Node.js 24.22.1 离线读取第 5 步导出的目录，独立解码 bits、映射成员与位置、计算 AttestationData SSZ root，并逐条比对；不连接 Teku、PG 或 CH，不计算收益。成功输出 `votes_verified: true`，失败返回非 0。

本次文档调整未执行上述真实清理或采集命令。

## 两个 profile 与完成语义

| profile | 必需数据 | 不包含 |
| --- | --- | --- |
| `blocks` | 原始区块、完整字段拆解、空 slot 时间轴及覆盖 | 委员会映射、奖励、余额、Geth receipts |
| `blocks_votes`（默认） | blocks 全部 + 历史委员会 + 每个 aggregate 的 validator 成员包含长表 | 奖励、全量 validator 注册表/余额、Geth receipts |

plan 冻结 finalized anchor 及从它沿 parent 链回溯的 **N 个实际 canonical blocks**，空 slot 不计入 N，不等待额外奖励结算窗口。同 run resume 不移动范围。旧版 `--subset`、`--limit`、默认奖励 export 和未实现的 `--from-run` 不属于新 CLI。

队列按 raw → decode/slots → 发现委员会需求并封口 → committees → attestation_map 执行。区块事实独立发布；委员会失败不会撤销区块，但 votes profile 不能标为完整。status 分别显示区块/投票任务完成及 verified 状态；Go verify 成功才将 profile 标为 verified，Node 独立核验报告另行输出。失败 task 可以重跑同一 collect 命令恢复，已完成 task 不重复累计。

委员会按实际 attestation slot 的 epoch 获取，使用冻结 canonical 分支上某包含块的 state root（适用于该 state 的当前/前一 epoch），请求明确 `?epoch=`。无需全量 validator registry，但历史委员会仍可能触发节点状态恢复；服务端 Request Timeout 与客户端 HTTP_TIMEOUT_SECONDS 是不同预算。

## 保存的数据与投票查询

完整 HTTP 响应保存在 ARCHIVE_DIR，数据库保存 typed 查询字段及原始对象/归档 hash：

- beacon_blocks、block_operations：签名、body 字段、slashings、deposits、exits、sync aggregate、blob commitments、execution requests 等。未知字段保留。
- execution_payloads、execution_transactions、withdrawals：完整 payload、按原数组位置保存交易字节和提款；不代表取得 receipts/trace/blob sidecars。
- attestation_inclusions：投票 slot、包含 slot/root/index、head/source/target、原始 bits、签名、SSZ AttestationData root、参与位数量。
- beacon_committees：有序成员及历史 state/context、原始响应。
- validator_attestation_inclusions：validator index、投票内容、包含位置、真实 committee index/position、aggregation position、输入 attempt 与原始证据。
- slot_coverage、attestation_mapping_coverage：空 slots、零 attestation 区块、逐 aggregate 映射覆盖。

读取 `*_published` 视图，不直接聚合物理表。视图以 `(run, task)` 选中的唯一 attempt 过滤并逻辑去重；collect 最后封存 selection，verify/export 确认其与 PG 和本地 publication 相同。查询同时限定 run/network；同一事实在不同 run 重采不会自动成为新的链上投票。

查询例子见 [validator_votes.sql](analysis/sql/validator_votes.sql)、[slot_arcs.sql](analysis/sql/slot_arcs.sql) 和 [coverage.sql](analysis/sql/coverage.sql)。SQL 默认数据库名 ethvis；其他库名应按实际配置修改。

SSZ bitlist 按 LSB 顺序排除终止位；Electra/Fulu 用 committee_bits 选择委员会并按 index 顺序拼接。旧 fork 使用 data.index。支持 phase0、altair、bellatrix、capella、deneb、electra、fulu；版本与冻结 spec 不符或未知 fork 会阻止解码，原始响应仍保留。

同一投票被多个 aggregate/区块包含时，基础表保留每次包含。逻辑投票可后续按 network + validator + attestation_data_root 去重；不要直接按 validator + epoch 覆盖。bit=0、未采到或窗口截断都不等于证明 validator 没投票。链上包含记录不能恢复仅在 gossip 中传播的投票。

## 独立核验与已知边界

Go verify 检查 required tasks、封口 publication、冻结区块/slot 覆盖、逐 aggregate 成员行数及唯一性。Node 从导出包中的完整原始 block/committee 重新解码并逐条比较成员、位置和 SSZ data root，同时检查文件 hash、精确大整数、重复身份和零行覆盖。完整 votes export **没有默认行数截断**，按块导出和核验；每次内存规模受单块数据约束。

不在本版计算收益、余额权重、head/source/target 正确性分类，不重放全部状态转换，不重新验证聚合 BLS 签名，也不本地重算完整 block SSZ root。区块身份基于受信任节点的 frozen canonical headers 与正文 slot/parent/state 对应检查。

## 代码与测试

入口为 `cmd/ethvis-collector` → `internal/app`；活动代码分为 config/domain/beacon/archive/decode/pipeline/storage。`internal/collector` 旧包暂作为参考和原测试保留，新 CLI 不依赖它。旧奖励研究脚本隔离在 `analysis/rewards/`。设计细节见 [代码设计](docs/COLLECTOR_CODE_DESIGN.md)。

```bash
make test
# 可选：仅在专用、空的测试数据库运行真实数据库集成测试。
# PostgreSQL 数据库名必须以 ethvis_test_ 开头；测试会清理其测试表。
ETHVIS_TEST_PG_DSN='postgresql://…/ethvis_test_empty' \
ETHVIS_TEST_CH_URL='http://127.0.0.1:8123' \
  go test -run TestDatabaseBlocksVotesRoundTrip -v ./internal/pipeline
```

常规测试不读取 `.env`。真实数据库集成测试在没有显式测试连接变量时跳过；不能把本地 mock/离线核验通过当成已完成真实 Teku/数据库的 100 块验收。首轮通过后再新建 `--blocks 1000`、`--blocks 100000` 的 run。

| 环境变量 | 含义 |
| --- | --- |
| ARCHIVE_DIR / RUNS_DIR | 原始响应/spool 与冻结清单/检查点/报告目录 |
| BLOCK_COUNT | plan 未传 --blocks 时的默认实际块数 |
| WORKERS / STATE_WORKERS | 普通任务并发 / 委员会并发上限 |
| HTTP_TIMEOUT_SECONDS / HTTP_ATTEMPTS | Teku 每次请求总预算 / 尝试次数 |
| BATCH_ROWS / BATCH_BYTES | CH 有界写入批次的行数/字节阈值；单个大行独立写入 |

旧 `.env` 中的 COLLECT_SSZ、VALIDATOR_BATCH_SIZE 在 v0.3 不生效，不会因此开启全量 state/validators 请求。真实 `.env` 没有被模板覆盖。
