# EthVis2：以太坊共识层与 Validator 收益分析数据采集设计

文档日期：2026-10-26。本文是完整数据目标设计。已实现首版采集器，使用 Go 1.27.3 与 Node.js 24.22.1；实际操作和当前覆盖边界见 [README.md](README.md)。尚未填写真实连接或采集用户节点。

## 1. 建议与设计范围

**已确认：ClickHouse 是分析主库，PostgreSQL 是控制库。** 两个数据库均已具备。ClickHouse 保存链上事实及分析结果；PostgreSQL 管理采集任务、检查点、批次清单、质量状态、标签和配置。原始 JSON/SSZ 等大对象独立归档。

**首轮范围已确认：近期已 finalized 的连续区间内，实际产生的 100,000 个 canonical Beacon blocks。** 保留中间空 slot，并额外采集首尾 epoch/结算所需上下文；先跑通采集、归档、事实表、派生分析和结果查询，再扩展全历史。具体冻结锚点和 slot 范围由试采启动时只读探测后写入 manifest。

chaind 可复用，但不能原样直写 ClickHouse，也不能单独满足完整原始归档与收益分析。采用评估和接入方案见第 24 节。

建议采用三个逻辑层次，对应用户的两步工作方式：

1. **第一步 A：原始归档层**。保存节点返回的完整响应、原始签名区块、必要的 SSZ 状态及对象清单。它是可重新解析、核验和应对后续升级的依据。
2. **第一步 B：结构化事实层**。把原始区块、状态、职责、奖励和执行层数据拆成有明确粒度和关联键的表；只做无损解析、类型转换和可验证的关联。
3. **第二步：分析派生层**。根据明确版本的规则计算投票正确性、参与率、收益、机会损失、排名和集中度；可删除重建，不覆盖原始记录。

**重要修正：逐 block 抓取必要，但不充分。** Block 不包含完整验证者注册表、全部余额、未完成职责、奖励结算细节，也不包含所有网络投票和历史 fork-choice 过程。因此第一步必须同时有 slot、epoch、state、执行层和实时观测几条采集线。

“尽可能多地记录”建议理解为：**完整保存能获取的原始信息，并为大数据对象安排存储层级，而不是把所有对象都重复展开进数据库。** 全量 validator × slot 状态、全部交易 trace、所有 blob/data column 常驻热库的成本，远高于单纯区块表。

本设计中的表名、键、字段别名、优先级和调度方式是 EthVis2 的建议；不是声称参考仓库或标准 API 已经实现这些表。节点地址、版本、硬件、历史保留范围尚未验证。

## 2. 参考仓库实际提供了什么

### 2.1 EthVis

本次检查的本地版本：`2549693f461c0b89d2866b37819c26eba30c3aa8`。

| 文件 | 已有能力 | EthVis2 应保留或补充的内容 |
| --- | --- | --- |
| `../Archived/EthVis/Database/Clickhouse/query-teku.js` | 按 slot 请求区块和 header，存 root、parent、proposer、eth1_data，拆出 attestations | 保留其关系模型；扩展为完整签名区块、所有 operation、响应元数据和失败分类 |
| `../Archived/EthVis/Database/Clickhouse/beacon_block.sql` | 区块基础事实 | 增加 fork、body root、signature、执行 payload、状态和 canonical 历史 |
| `../Archived/EthVis/Database/Clickhouse/attestations.sql` | inclusion slot/root/index、投票 slot、committee、head/source/target | 增加签名、bit 长度、升级后的 committee bits、委员会位置映射 |
| `../Archived/EthVis/Database/Clickhouse/query-geth.js`、`eth1_block.sql` | 执行区块、gas、base fee、交易 hash | 增加完整交易、receipt、withdrawals、精确金额和共识层关联 |
| `../Archived/EthVis/Database/Clickhouse/*slashings.sql` | 文件存在，但 schema 只是少量占位字段 | 需要完整保存两份冲突证据、签名、validator indices 和被惩罚对象 |
| `../Archived/EthVis/Database/Postgresql/main_table.js`、`main_table.sql` | 计算 Casper 正确/错误/未投票集合和余额、GHOST 选择 | 作为第二阶段分析需求，不能直接作为原始事实 |
| `../Archived/EthVis/Database/Postgresql/overview.js`、`overview.sql` | 缺块、deposit 概览 | 修正错误请求被计为缺块、按 genesis day 分桶与自然日混用等问题 |
| `../Archived/EthVis/Backend/flask1.py`、`Frontend/src/components/{Overview,Epoch,Slot}.vue` | Overview / epoch / slot / validator 视图，投票边、有效余额和延迟 | 保留时间尺度和图关系需求；明确链上包含投票与实时网络投票的区别 |

旧代码中不能继承的假设：

- `getBalance()` 在查询失败时默认 32 ETH，并把更大有效余额截到 32 ETH。缺失必须保持缺失；现代有效余额上限取决于 fork 和 withdrawal credentials，不能固定截断。
- 普通网络错误、状态缺失和真正 skipped slot 不能都 `continue` 或视为缺块。
- `eth1_data.block_hash` 是共识层历史存款投票数据，**不是**当前区块执行 payload 的 hash。
- 把某 slot 没有区块等同于 head vote 错误不成立：应解析该 slot 的 canonical ancestor，包括空 slot。
- 从已上链 attestations 汇总得到的是事后可见投票；不能标成节点当时真实 LMD-GHOST 权重。
- `f_target_epoch - f_source_epoch` 表示 checkpoint epoch 差，不是 attestation inclusion delay。

### 2.2 ETH2.0-reward

本次检查的本地版本：`dd22adad105a0b27240eb19bd97dc1eadea7e794`。

| 文件 | 参考价值 | 需要扩展/修正 |
| --- | --- | --- |
| `../Archived/ETH2.0-reward/code/data_collection/collect_proposer_reward.py` | block reward API，提议者收益分项 | 按 block root 固定分支，记录失败和响应上下文；它不是完整 EL/MEV 收益 |
| `../Archived/ETH2.0-reward/code/data_collection/collect_attestation_reward.py` | epoch × validator 的 head / target / source 收益 | 同时保存 inactivity、Phase0 inclusion_delay 和 ideal_rewards |
| `../Archived/ETH2.0-reward/code/data_collection/collect_sync_committee_reward.py` | sync committee reward API | 脚本对已解码响应再次 `json.loads`，且未正确按成员数组落表；只借鉴接口思路 |
| `../Archived/ETH2.0-reward/code/data_collection/collect_reward_beacon.ipynb` | 三类奖励接口的较清晰调用示例 | 增加幂等、重试、历史覆盖、分叉和 fork 适用性 |
| `../Archived/ETH2.0-reward/code/data_collection/aggregated_epoch_reward.py`、`aggregated_daily_reward.py` | 三类收益合并，epoch/day 聚合 | left join 后缺项未妥善处理会传播 NaN；不能把未知值全部填 0；保留多种时间归属 |
| `../Archived/ETH2.0-reward/code/utility.py` | slot/epoch/时间换算 | genesis time、slot 时长、每 epoch slot 数从网络配置读取，不写死 |
| `../Archived/ETH2.0-reward/code/data_cross_validation.ipynb` | 第三方收入明细交叉验证 | 只能作为辅助；需要统一时间边界、单位、惩罚和版本 |
| `../Archived/ETH2.0-reward/code/decentralization_metrics_calculation/*.py` | Gini、HHI、Nakamoto、Shannon 相关指标 | 保留负收益；指标输入中的截零另设字段和方法；区分全部活跃者与仅获奖者样本 |

该仓库的聚合逻辑主要针对其历史研究区间，不能直接作为 genesis 至今所有 fork 的完整收益账本。README 中的收益单位描述也不能替代 API 单位：共识奖励接口返回 Gwei，应保留整数原值再换算。

## 3. PostgreSQL 与 ClickHouse 的选择

### 3.1 按本项目工作负载比较

| 维度 | PostgreSQL | ClickHouse | 对本项目的含义 |
| --- | --- | --- | --- |
| 小量数据建模、约束、事务和多表更新 | 适合 | 需要更多应用层协调 | 采集任务、检查点、标签更适合 PG |
| 单 validator 点查、复杂关系查询 | B-tree 与关系模型便利 | 依赖排序、projection、字典及适当预聚合 | CH 要显式设计 validator 时间线访问路径 |
| 大量 append-only 数值明细、跨年聚合 | 可以，需仔细分区和运维 | 列式读取和压缩更符合这类负载 | 全量 validator-epoch 与投票参与事实倾向 CH |
| 幂等写入 | UNIQUE + UPSERT 很直接 | PRIMARY KEY 不强制唯一 | CH 必须单独设计去重和发布流程 |
| 高频随机修正/重组撤销 | 事务更新方便 | 追加版本和批量重建更合适 | CH 主表以 finalized 为主，临时区单独管理 |
| 未知结构原始响应 | JSONB 方便查询 | 可保存 String/JSON，能力依版本而异 | 大对象优先归档文件，只在库里存索引与常用列 |
| 长期运维成本 | 单库路径简单 | 多一种系统，但大分析表更匹配 | 先单节点 CH，不因设计提前上集群 |

这是基于数据形状的架构判断，**不是在用户机器上测出的性能结论**。实际选择需用历史样本验证查询和容量。

### 3.2 推荐落地方式

- **分析主库：ClickHouse**。区块、operation、epoch 状态、奖励、投票明细和聚合结果。
- **控制库：PostgreSQL（已确认）**。`ingest_tasks`、检查点、归档清单、人工实体标签、标注和配置。
- **原始大对象：本地磁盘/NAS/对象存储**。压缩 JSON/JSONL、SSZ、Parquet、blob、trace；数据库保存 URI、hash、大小和覆盖清单。本地目录即可起步，不要求云服务。
- PostgreSQL 的完成状态必须在 CH 数据及原始归档校验后提交；跨库不假定原子事务，通过 batch manifest、幂等键和重试恢复保持一致。
- chaind 原样运行会把链上明细写入 PG，而不仅是控制信息；这是一条额外暂存路径，不属于当前已确认的默认分工。详见第 24 节。

### 3.3 为什么数据规模会影响选择

以 **100 万个验证者在一整天保持活跃**作为容量推演假设，不是当前链上观测值。按主网 12 秒/slot、32 slots/epoch：

| 数据粒度 | 理论行数/天 | 说明 |
| --- | ---: | --- |
| slot | 7,200 | 含空 slot |
| epoch | 225 | 数量是全天长度；自然日不保证与 epoch 边界重合 |
| validator × epoch | 225,000,000 | 单独一张 epoch 余额或奖励表就可能达此量级 |
| validator × slot | 7,200,000,000 | 不建议默认逐 slot 保存全体余额 |
| sync committee position × slot | 3,686,400 | 按 512 positions 估算，实际 fork 配置为准 |
| attestation × participant 展开 | 约 225,000,000 起 | 同一投票重复包含会进一步放大 |

225M 行/天约为 82.125B 行/年。若**假设**实测压缩后每行 40 bytes，单表就约 9GB/天、3.29TB/年；这不含副本、索引、merge 空间、原始对象和其他表。40 bytes 不是承诺值，必须实际测量。

压缩不能替代粒度管理：委员会有序数组、签名、原始 bits 先完整保存；参与者长表按查询需要展开。余额采用 epoch 快照 + 状态变化记录 + 归档，避免全体逐 slot 重复。

## 4. 采集之前必须验证的节点能力

Archive 不意味着“任意历史数据全部可用”。Teku 的历史 block、历史 state、状态重建和 sidecar 保留是不同问题；Geth 的历史状态、区块体、receipt、trace 也需分别验证。

Teku 官方说明，checkpoint sync 后可通过 archive 模式配合历史状态重建恢复 genesis 到 checkpoint 的 states；这需要相应配置和完成重建，不应仅看到 archive 字样就认为已具备全部历史状态。[Teku 历史状态重建](https://docs.teku.consensys.io/how-to/reconstruct-historical-states)

建立 `node_capabilities`：

| 字段组 | 内容 |
| --- | --- |
| 身份 | node_id、client name/version、采集时间、network、chain_id、genesis root/time |
| 配置 | 历史状态模式、重建进度、可用 REST/RPC/Debug 方法、fork schedule、配置摘要 |
| 边界 | 最早/最晚可读 block、state、receipt、trace、blob/column 的已验证样本与范围 |
| 样本 | genesis、各已激活 fork 前后、旧 epoch、近期 finalized、最近 head、已知空 slot |
| 测试结果 | endpoint、参数、HTTP/JSON-RPC code、响应 hash、耗时、是否超时/不支持 |
| 覆盖可信度 | sample_verified / range_verified / unknown，不能把若干样本标成整段验证 |

上线前只读探测清单：

1. 从 Teku 获取 genesis、spec、fork schedule、node version、syncing 和 finalized checkpoint。
2. 对 genesis、每个已激活 fork 的前后和研究区间各抽样读取 block、state、committees、validators、三类 reward。
3. 对一个确认空 slot 验证 state 查询语义；记录能否获得该 slot 的状态，不能用相邻区块 state 冒充。
4. 用 Geth 按 payload block hash 读取 block、transactions、receipts；测试历史 balance 和代表性 trace。
5. 对 body/receipt/state 被裁剪、尚未重建、API 不支持分别建状态，安排补采或保留边界说明。
6. 对接口响应版本、可选字段、fork 不适用情况保存结果，优先固定已验证客户端与规范版本。

本文没有访问或探测用户的 Teku/Geth 服务，也没有假设它们运行在某个默认端口。

## 5. 通用字段、标识与数据类型

### 5.1 所有事实表应具有的上下文

不必在每一行重复所有字符串，但应能通过引用无损关联：

- `network_id`：至少包含 chain_id 与 genesis_validators_root；测试网重置不能与旧链混淆。
- `slot`、`epoch`、`block_root`、`state_root`、`fork_name`：按记录实际粒度适用。
- `source_node_id`、`source_api`、`api_version`、`source_response_id`。
- `collected_at_utc`、`first_seen_at_utc`（仅真实首次观测）、`ingest_batch_id`。
- `parser_version`、`schema_version`、`spec_commit_or_release`。
- `execution_optimistic`、`finalized_at_fetch`、`canonical_at_fetch`：接口未返回就 NULL，不推测。
- `data_status`、`quality_flags`、`raw_object_id`、`content_hash`。

`collected_at` 不是历史区块传播时间；`finalized_at_fetch=true` 不是该区块真实达成 finality 的时间。

### 5.2 键设计

以下所有自然键默认带 `network_id`。同 slot 可能存在多 block，同 state slot 可有分支，不能只用 slot/epoch 覆盖。

| 对象 | 建议自然键 |
| --- | --- |
| 原始区块 | block_root |
| slot 的 canonical 选择 | slot + selection_version / observation_id |
| 区块内 operation | inclusion_block_root + operation_type + operation_index |
| attestation 成员 | inclusion_block_root + attestation_index + committee_index + committee_position |
| 状态 | state_root；另记录 state_slot，避免与 block slot 混淆 |
| 状态内 validator | state_root + validator_index |
| epoch 派生快照 | epoch + boundary_kind + anchor_state_root + dataset_version |
| 投票委员会 | duty_epoch + dependent_root/state_root + slot + committee_index |
| sync 委员会位置 | period + anchor_state_root + committee_position |
| proposer/sync reward | block_root + validator_index + reward_schema_version |
| attestation reward | rewarded_epoch + reward_context_id + validator_index |
| 执行层区块 | execution_block_hash |
| receipt | execution_block_hash + transaction_index |

Epoch 奖励 API 只有 epoch 参数时，创建 `reward_context_id`，绑定请求前后核验的 canonical/finalized anchor、所需历史状态根和响应 hash；不虚构不存在的 block-root 请求参数。

### 5.3 类型与单位

| 内容 | ClickHouse 建议 | PostgreSQL 建议 | 约定 |
| --- | --- | --- | --- |
| slot、epoch、validator_index、block number | UInt64 | BIGINT 并校验范围 | `FAR_FUTURE_EPOCH` 等 uint64 哨兵需额外处理 |
| 任意协议 uint64，包括哨兵 | UInt64 | NUMERIC(20,0) | PG BIGINT 无法表示完整 uint64 |
| 共识余额/存款/提款 | UInt64（Gwei） | BIGINT 或 NUMERIC(20,0) | 原始金额保持整数 |
| 奖励、惩罚、余额 delta | Int64，累计按范围升级 Int128 | BIGINT / NUMERIC | 净额可负；大范围聚合防溢出 |
| EL 金额、gas price、256-bit 值 | UInt256；差额用 Decimal256/经过范围验证的有符号表示 | NUMERIC(78,0) | 最大 uint256 可达 78 位；Decimal256 不覆盖所有 78 位整数 |
| hash/root | FixedString(32) 二进制 | BYTEA 并校验长度 | UI 才转 0x hex；不要混用 32-byte 与 66-char |
| pubkey / signature / address | FixedString(48/96/20) | BYTEA | 保留原始 bytes |
| bitlist/bitvector | String bytes + logical_length | BYTEA + 长度 | 保留 SSZ 终止位语义，不存成普通二进制字符长串 |
| 时间 | DateTime64 UTC | TIMESTAMPTZ | UTC 是事实时间，上海时间用于显示 |
| 任意原始 JSON | String 或版本验证后的 JSON | JSONB，另存原始 bytes/hash | 解析 JSON 可能改变字段顺序/数字表现 |
| 归档路径 | String | TEXT | 附压缩方式、对象大小、checksum |

Gwei → Wei 精确乘 10^9，Wei → ETH 除 10^18 只用于展示或高精度 Decimal。禁止浮点金额和 JavaScript `Number` 解析大整数。PG 类型范围依据其[数值类型文档](https://www.postgresql.org/docs/current/datatype-numeric.html)。

## 6. 第一阶段 A：原始对象与采集审计

### 6.1 `raw_objects` / `api_responses`

粒度：一个归档对象/一次实际 API 响应；相同 bytes 可通过 hash 复用存储，访问记录仍分别保留。

| 字段组 | 必存字段 |
| --- | --- |
| 归档标识 | object_id、object_kind、content_sha256、uncompressed_sha256（如有）、URI |
| 编码 | JSON/SSZ/JSONL/Parquet、content_type、compression、byte_length、uncompressed_length |
| 来源 | node_id、endpoint/method、path params、query params、request body hash、request_id |
| 响应 | HTTP status、JSON-RPC error code/message、相关响应头、完整 envelope、返回版本 |
| 时间与重试 | requested_at、responded_at、duration、attempt_no、retry_parent_id |
| 链上下文 | slot、epoch、block_root、state_root、fork、finalized/optimistic |
| 解析状态 | parser_version、schema_version、parse_success、unknown_fields、error_detail |

保留完整 `data.signature`、envelope 和 API 元数据，不能像旧采集器一样只取 `response.data.data.message`。SSZ 仅对支持的端点请求；响应内容格式与 fork 版本一起存。一般 JSON + 压缩归档即可覆盖初期，区块与 checkpoint state 建议同时存 SSZ。

### 6.2 `ingest_tasks`、`ingest_attempts`、`dataset_manifests`

- task：dataset、范围起止、目标分支、状态、优先级、依赖、worker、lease、retry count、最后错误。
- attempt：真实请求、耗时、bytes、返回行数、失败分类、原始响应引用。
- manifest：batch id、数据集版本、范围、文件清单、记录数、校验和、依赖完成度、发布状态。
- coverage：区分 block 完成、委员会完成、状态完成、reward 完成、EL 完成；只有全部必需依赖完整才发布对应分析数据集。
- checkpoint：已完成的连续范围和待补洞列表，不能只保存“最后处理到哪个 slot”。

状态建议：`pending / fetching / archived / parsed / validated / published / retryable_error / permanent_unavailable / not_applicable`。API 不适用和数据缺失必须分开。

## 7. Slot、区块头与完整 Beacon Block

### 7.1 `slot_observations` / `canonical_slot_versions`

粒度：每个 slot 的一次状态判断；覆盖所有 slot，包括没有 canonical block 的 slot。

必存：`slot`、理论开始时间、epoch、查询时 head/finalized slot、observed canonical block root（可空）、最近 canonical ancestor root/slot、预期 proposer 引用、observed_at、状态依据。

状态至少区分：

- `canonical_block_present`；
- `skipped_confirmed`：在完整、已 finalized 范围内，经祖先链缺口/交叉验证确认无 canonical block；
- `not_yet_produced`：尚未到达或正在出块；
- `not_found_unclassified`：仅收到 404，原因未确认；
- `history_unavailable`、`node_syncing`、`request_failed`。

“skipped”只能说明该 canonical slot 缺块，不能证明全网没人提出过 orphan block；“invalid block”也不是空 slot 的同义词。对确认空 slot，保留预期 proposer 供职责分析，不制造 block reward。

### 7.2 `beacon_headers`

粒度：一个签名 header / block root。

保存：root、slot、proposer_index、parent_root、state_root、body_root、signature、canonical_at_fetch、execution_optimistic、finalized_at_fetch、signed_header 原文引用。

`parent_slot` 可从 parent root 关联补充，标记为解析关联字段。Genesis 的 parent/root 规则单独处理。

### 7.3 `beacon_blocks`

粒度：block root；保存完整 SignedBeaconBlock，结构化提取：

| 层次 | 字段 |
| --- | --- |
| 外层 | version、签名、block root、header/body root、响应元数据 |
| message | slot、proposer_index、parent_root、state_root |
| body 基础 | randao_reveal、eth1_data.deposit_root / deposit_count / block_hash、graffiti 原始 bytes |
| 操作引用 | proposer_slashings、attester_slashings、attestations、deposits、voluntary_exits 的有序子表引用 |
| Altair 起 | sync_aggregate bits、signature |
| Bellatrix 起 | execution_payload，按实际区块格式保存完整 body |
| Capella 起 | bls_to_execution_changes；payload withdrawals |
| Deneb 起 | blob_kzg_commitments；payload blob_gas_used / excess_blob_gas |
| Electra 起 | execution_requests 的 deposits / withdrawals / consolidations |
| 统计辅助列 | 各 operation 数量、交易数、字节大小、关联 EL block hash |

Graffiti 可以另存安全解码文本，但不能用文本替代原值。未知 fork/字段先完整归档，解析器报告待支持；不静默丢弃。

基础结构与升级差异分别参照[Phase0](https://ethereum.github.io/consensus-specs/specs/phase0/beacon-chain/)、[Altair](https://ethereum.github.io/consensus-specs/specs/altair/beacon-chain/)、[Bellatrix](https://ethereum.github.io/consensus-specs/specs/bellatrix/beacon-chain/)、[Capella](https://ethereum.github.io/consensus-specs/specs/capella/beacon-chain/)、[Deneb](https://ethereum.github.io/consensus-specs/specs/deneb/beacon-chain/)规范。采集时应固定实际使用的 release/commit；这些链接的在线内容会更新。

## 8. Attestation：投票事实与成员映射

### 8.1 `attestations`

粒度：某 block 内第 i 条 attestation，不能仅以 attestation data root 去重。

| 字段 | 用途 |
| --- | --- |
| inclusion_block_root、inclusion_slot、attestation_index | 定位区块内 operation |
| attestation_root、attestation_data_root | 内容标识及重复包含分析；不替代 inclusion identity |
| data.slot、data.index | 原始投票 slot/index；升级后 index 语义按 fork 处理 |
| data.beacon_block_root | head vote 指向 |
| data.source.epoch/root、data.target.epoch/root | FFG source/target 边 |
| aggregation_bits 原始 bytes、逻辑 bit 长度 | 成员解析与核验 |
| committee_bits 原始 bytes/长度（存在时） | Electra 后多委员会解释 |
| signature | 聚合签名原值 |
| fork_name、raw_object_id、解析版本 | 防止用旧格式解码新投票 |

可增加 inclusion_delay_slots，但作为可重算字段：`inclusion_slot - data.slot`。不要与网络到达延迟混用，也不要假定规则跨 fork 不变。

### 8.2 `beacon_committees` / `committee_members`

粒度：某 duty context 下的 slot + committee；成员子表粒度是 position。

保存：epoch、slot、committee_index、dependent_root 或用于计算职责的 state_root、committee_size、**有序 validator_indices**、committee_position、validator_index、响应状态。数组顺序必须保留，否则无法解释 aggregation bits。

委员会由对应历史状态/职责上下文确定，不可拿 head 的委员会解释历史 block。相同 epoch 在不同分支上职责上下文可能不同。

### 8.3 `attestation_participants`（无损映射/可延期展开）

保存 inclusion identity、committee_index、committee_position、aggregation_bit_position、validator_index、bit_value、mapping_context。只存参与者 bit=1 时还必须保留完整委员会，以便第二阶段找出未包含者。

解码要求：

- 使用 SSZ bitlist 的位顺序，区分终止位和成员位。
- Electra 后使用 committee_bits 挑选委员会，并按规范组合有序成员，不能继续只按 `data.index` 查一个委员会。
- 保留重复上链证据；在派生层按 duty/validator 和 fork 规则去重，不能对原始事实直接 `DISTINCT validator_index`。
- 未在 canonical block 中看到某验证者投票，只能称“未观察到被包含”，不能据此断言他没有在网络上发出投票。

Electra 修改了 attestation/委员会表达、有效余额及请求处理；具体规则以[Electra 规范](https://ethereum.github.io/consensus-specs/specs/electra/beacon-chain/)为准。

### 8.4 第二阶段的投票正确性需要哪些锚点

每条派生判定必须带 `canonical_anchor`、state root、规则版本和证据：

- head：参照投票 slot 的规范 block root，空 slot 通过祖先链和历史 state roots 处理。
- target：参照 target epoch 起点的 checkpoint root，不能要求那个 slot 必须产块。
- source：对照该投票处理上下文和 fork 所要求的 justified checkpoint，不是简单与 target epoch 相减。
- timely source/target/head：由 inclusion timing、participation flags 和适用 fork 规则确定。
- missed：从职责全集减去满足规则的已包含参与事实，不能只在出现过 attestation 的委员会内计算。

保存 `correctness_status=unknown` 比无依据地填 false 更有价值。

## 9. Block 中其他 operations：都要完整保留

以下每个子表默认保存 inclusion_block_root、operation_index、原始对象引用、fork 和 source_response_id。

| 表 | 粒度与字段 |
| --- | --- |
| `proposer_slashings` | 每条证据；signed_header_1/2 的 slot、proposer_index、parent/state/body roots 和两份 signature |
| `attester_slashings` | 每条证据；attestation_1/2 的完整 data、两组有序 attesting_indices 和 signature；额外关联被 slash 的交集仅作为派生 |
| `beacon_deposits` | 每个旧式 deposit；proof 有序数组、pubkey、withdrawal_credentials、amount_gwei、signature、区块内 index |
| `voluntary_exits` | message.epoch、validator_index、signature；退出消息不等于已退出或已提款 |
| `bls_to_execution_changes` | validator_index、from_bls_pubkey、to_execution_address、signature |
| `sync_aggregates` | 每 block 一个；sync_committee_bits、长度、sync_committee_signature、被签名 parent block 关系和 committee context |
| `execution_deposit_requests` | pubkey、withdrawal_credentials、amount、signature、request index |
| `execution_withdrawal_requests` | source_address、validator_pubkey、amount、request 在 body 中的顺序 |
| `execution_consolidation_requests` | source_address、source_pubkey、target_pubkey、request 顺序 |

Slashings 要分别表达“证据被包含”“validator 当时是否可 slash”“状态实际扣款发生在哪个边界”。一条 attester slashing 可以涉及多个 validator，证据本身不携带完整最终扣款账。

Deposit 的 proof、处理顺序及状态 `eth1_deposit_index` 才能帮助恢复处理进度；不能只用累计 `eth1_data.deposit_count` 当作当前 block 的新增存款数。对 Electra 后请求，保留请求事件与状态实际生效/入队/出队两个层次。

## 10. Validator 注册表、历史状态与职责

### 10.1 `validator_registry_versions`

粒度：validator 在某 state 的注册信息，存初始快照 + 变化版本；若先保存 epoch 全量响应也应可重建该历史。

必存：validator_index、pubkey、withdrawal_credentials、effective_balance_gwei、slashed、activation_eligibility_epoch、activation_epoch、exit_epoch、withdrawable_epoch、API status、observed state root/slot、有效起止版本。

`pubkey` 身份与可变 credentials/status/effective balance 分开。withdrawal address、credentials type 为可重算字段。不能把 `effective_balance` 当成实际余额，也不能把相同 fee recipient 当成同一运营实体的确证。

### 10.2 `validator_balance_snapshots`

粒度：某个**明确定义的状态边界** × validator。

必存：state_root、state_slot、boundary_kind、validator_index、balance_gwei、effective_balance_gwei 引用、epoch、snapshot completeness。

推荐 baseline：

- 每 epoch 起点的 pre-block state（即 `process_slots` 到 `epoch*slots_per_epoch` 后、处理该 slot block 前），或者另一种同样精确且可重现的边界；选定一种后固定。
- 每个已包含 block 的 state root 是 **post-block state**，不能直接替换上述 pre-block state。REST slot 查询的语义需核实；必要时从 checkpoint 用规范状态转换重放。
- 每天/固定 epochs 保存一个完整 SSZ checkpoint，稀有事件前后和 fork 边界额外保存。
- 对发生提款、slash、合并、deposit 应用的 validator，补存相关 block 前后余额或重放账本。
- 不默认按每 slot 对全部 validator 调用接口并存重复快照。

完整状态快照加原始区块、网络配置和可验证重放，是后续新增状态字段的重要保障；仅保留余额数组不等于可重建整个 BeaconState。

### 10.3 `beacon_state_snapshots` / `state_metadata`

保存完整 SSZ（推荐）或完整 JSON 原始对象；热表提取以下字段组：

| 字段组 | 内容与用途 |
| --- | --- |
| 基础身份 | genesis_time、genesis_validators_root、slot、fork.previous/current_version/epoch |
| header / roots | latest_block_header；block_roots、state_roots 的归档引用，historical_roots / historical_summaries |
| ETH1 桥接 | eth1_data、eth1_data_votes、eth1_deposit_index |
| 注册与余额 | validators、balances 对象引用/行数/校验摘要 |
| 随机性 | randao_mixes，辅助复现职责 |
| slashing | slashings 向量及索引上下文 |
| Phase0 参与 | previous_epoch_attestations、current_epoch_attestations，包括 inclusion_delay/proposer 信息 |
| Altair 后参与 | previous_epoch_participation、current_epoch_participation、inactivity_scores |
| finality | justification_bits、previous/current_justified_checkpoint、finalized_checkpoint |
| sync | current_sync_committee、next_sync_committee 的完整有序 pubkeys 和 aggregate_pubkey |
| execution | latest_execution_payload_header |
| withdrawals | next_withdrawal_index、next_withdrawal_validator_index |
| Electra 队列 | pending_deposits、pending_partial_withdrawals、pending_consolidations，以及对应 churn/余额消费/最早处理 epoch 等状态字段 |
| Fulu 扩展 | proposer_lookahead 等该 fork 实际包含字段；保持完整 SSZ 和版本 |

不要求每个向量每 slot 重复展开；以 checkpoint 原始快照为完整依据，按分析需要展开切片。Fulu 的状态扩展参照[Fulu 规范](https://ethereum.github.io/consensus-specs/specs/fulu/beacon-chain/)。

### 10.4 `validator_epoch_participation`

粒度：validator × duty epoch × 结算上下文；来源是历史 state 或规范重放。

记录 participation flags 原值及 source/target/head flags、inactivity_score、active/slashed 状态、effective balance、用于奖励计算的状态边界、是否处于 inactivity leak、规则版本。Phase0 另存 pending-attestation 证据及 inclusion 信息，不能虚构 Altair flags。

这一表是解析状态的事实切片；“正确率”“理想收益差”仍放第二阶段。

### 10.5 `proposer_duties`

保存 duty epoch、slot、validator_index、pubkey（若返回）、dependent_root、execution_optimistic、请求 state/head 上下文。保留计划职责与实际 proposer 的关联校验。

空 slot 也要有职责记录，才能分析 missing proposal。历史 duties API 不可用时，从历史 state 按该 fork 规范重放，标明 `source=replayed` 和实现版本，不能拿近期 duties 回填。

### 10.6 `sync_committee_members`

保存 period、起止 epochs、anchor_state_root、position、validator_index、pubkey、aggregate_pubkey、子委员会位置（可重算）。**同一 validator 可以占多个 positions**：位置事实不能先去重；API 的每 validator reward 结果与位置表也不能连接后重复求和。

sync aggregate 证明的对象通常是 parent block root，委员会解释与签名 slot 按规范处理。不要把 inclusion block、parent block 和 epoch 边界混为一谈。

## 11. Finality、canonical 选择与实时观测

### 11.1 `finality_checkpoints`

按 state/观测保存 previous_justified、current_justified、finalized 的 epoch/root，justification_bits、state slot/root、查询时间、finalized/optimistic 元数据。

历史状态告诉我们该状态记录的 finality；“实际在本地几点首次看到 finalized”需要实时事件记录。

### 11.2 `canonical_chain_observations` / `reorg_events`

保存 observation time、head root/slot、finalized root/epoch、old/new head、common ancestor、重组深度、被移除/新增 root 清单、数据源。canonical 是随时间变化的关系，不是 block 固有不可变属性。

历史回填通常只能恢复当前 canonical 链与节点仍保留的其他 blocks；archive 不能保证全网历史 orphan/invalid block、所有 fork-choice store 或消息到达时间。

### 11.3 `node_events` / `fork_choice_snapshots`（从现在开始采）

建议单独低负载采集实时 SSE、客户端可用调试快照和必要节点指标：

- head、block、chain_reorg、finalized_checkpoint、attestation、voluntary_exit、slashings、sync contribution 等**本地节点实际支持**的事件及原始 payload。
- node_id、session_id、stream/topic、event id（若有）、本地收到时间、链内 slot/root、重连/断连区间。
- fork-choice 的节点集合、parent、weight、justified/finalized checkpoints、execution validity（取决于接口）。
- 可选 peer count、syncing、clock offset、CPU/I/O 等观测，帮助判断采集缺口与节点异常。

实时事件是单节点视角，不能声称全网完整；SSE 不保证可无限回放。完整 validator 级 gossip 或未聚合投票可能需要额外 P2P 监听/客户端 instrumentation，不能承诺标准 REST 能提供。

## 12. 共识层收益：直接保存节点计算结果

三类标准接口是第一阶段应该归档的**源计算事实**。即使第二阶段计划自己算，也保留节点输出用于交叉校验。保存负值、单位、完整 `ideal_rewards`、响应 envelope 和上下文。

接口定义：[block rewards](https://raw.githubusercontent.com/ethereum/beacon-APIs/master/apis/beacon/rewards/blocks.yaml)、[attestation rewards](https://raw.githubusercontent.com/ethereum/beacon-APIs/master/apis/beacon/rewards/attestations.yaml)、[sync rewards](https://raw.githubusercontent.com/ethereum/beacon-APIs/master/apis/beacon/rewards/sync_committee.yaml)。返回字段和 Gwei 单位参照[reward schemas](https://raw.githubusercontent.com/ethereum/beacon-APIs/master/types/rewards.yaml)。

### 12.1 `proposer_rewards`

粒度：block root × proposer。

保存：slot、epoch、block_root、proposer_index、`total_gwei`、`attestations_gwei`、`sync_aggregate_gwei`、`proposer_slashings_gwei`、`attester_slashings_gwei`、reward context、原始响应。

检查 `total = attestations + sync_aggregate + proposer_slashings + attester_slashings`。这些是**该 proposer 因包含相应内容获得的奖励**，不是被包含的所有 attesters/sync 成员奖励总额；也不包含 EL tips、MEV、提款本金。

### 12.2 `attestation_rewards`

粒度：rewarded_epoch × reward_context × validator。

保存：validator_index、`head_gwei`、`target_gwei`、`source_gwei`、`inactivity_gwei`、`inclusion_delay_gwei`（Phase0 若提供）、字段是否存在、对应有效余额、state/context 引用。

**所有有符号返回值按原样保存**。净收益按实际接口语义累加已签名分项，不能先 `abs(inactivity)` 再重复减一次。API 如果返回某组件为扣款负数，该负数应直接进入净额；具体客户端用状态变化/重放验证。

### 12.3 `ideal_attestation_rewards`

粒度：rewarded epoch × context × effective_balance 档位，而非每个 validator 一份。

保存 API 的 effective_balance、ideal head/target/source/inactivity、Phase0 inclusion_delay、适用 fork、原始响应。后续按 validator 的奖励计算有效余额映射。它是该状态/条件下的理想参照，不是固定 ETH 收益承诺。

### 12.4 `sync_committee_rewards`

粒度：inclusion block root × validator，按 API 返回的 validator 级记录保存。

保存 slot、epoch、block root、validator_index、reward_gwei、committee context、原始响应。位置级参与从 sync aggregate + members 得到；不可把 validator 级 reward 复制到每个 position 后再求和。

空 slot 没有 sync aggregate block，接口 404 不能直接生成全体成员惩罚；依据真实状态转换处理是否有奖惩。

### 12.5 `slashing_penalty_events` / `balance_change_events`

标准三类 reward API 不等于完整余额变动账。需要额外跟踪：

- 被 slash 的 validator、slashed state transition、即时扣款；
- 后续相关性惩罚、slashed/inactivity 状态下的其他扣款；
- whistleblower/proposer 侧奖励归属与已采 proposer reward 的重叠；
- fork 特有结算、余额饱和到零、内部 consolidation 转移。

保存 validator、事件类型、effect block/epoch/state、pre/post balance、signed delta、证据 root、计算/重放版本。没有标准端点的扣款通过可验证状态重放产生，并标为 `replayed`，不能宣称来自 block body。

### 12.6 三种时间归属都保留

- `duty_epoch / rewarded_epoch`：履行职责属于哪个 epoch；
- `inclusion_slot`：证据在哪个 block 出现；
- `applied_state_slot / settlement_epoch`：余额实际何时变化。

日统计需明确使用 duty date 还是 settlement date；跨午夜 epoch 不得靠“本 epoch 第一个已有区块的时间”随意归日。可提供两套指标，不混成一列。

## 13. 执行层与 Validator 综合收益

如果只研究 CL 发放机制，可先完整采 CL；但用户的 Validator 收益分析建议至少保存 EL payload、receipts 和 fee recipient，避免以后无法解释提议者收益差异。

### 13.1 `execution_payloads`

粒度：beacon block root ↔ execution block hash。

保存：parent_hash、fee_recipient、state_root、receipts_root、logs_bloom、prev_randao、block_number、gas_limit、gas_used、timestamp、extra_data、base_fee_per_gas、block_hash、完整 transactions 有序原始字节；适用 fork 的 withdrawals、blob_gas_used、excess_blob_gas 等。CL 原始 body 完整保留，热表交易可只放 hash + 外部对象引用。

注意 Beacon root、execution hash、state root 是不同标识；提款对象在 CL payload 与 EL block 中有同一事实的双重表示，不可重复记账。

### 13.2 `execution_blocks`

从 Geth 按 hash 获取完整 block，保存原始 JSON 与所有实际返回 header 字段；结构化提取 number/hash/parent、roots、miner/fee recipient、gas、base fee、timestamp、extra data、mixHash/prevRandao、withdrawalsRoot、blob gas、parentBeaconBlockRoot、requestsHash（存在时）等。

在 Merge 前，CL block 不含 execution payload；研究更早 deposit 时按执行链独立采集，不能强制所有 CL slot 关联 EL block。Geth hash、CL payload hash 和 block number 需交叉核验。

### 13.3 `execution_transactions`

粒度：EL block hash + transaction_index。

保存 hash、type、from/to、nonce、value_wei、gas limit、gasPrice、maxFeePerGas、maxPriorityFeePerGas、input、accessList、chainId、v/r/s 或 yParity；适用类型的 maxFeePerBlobGas、blobVersionedHashes、authorizationList；原始 signed transaction bytes 和未知字段归档。

`to=null` 的合约创建、交易类型字段不存在与零值必须区分。交易输入和日志可存冷层，不必全部成为热表宽列。

### 13.4 `transaction_receipts` / `execution_logs`

receipt 保存：tx hash/index、block hash/number、status 或早期 root、gasUsed、cumulativeGasUsed、effectiveGasPrice、contractAddress、logsBloom、type、blobGasUsed/blobGasPrice（若有）、完整 logs。

logs 子表保存：block/transaction/log index、address、topics 有序数组、data、removed（若返回）、raw reference。支持存款合约、质押协议、奖励分配和提款请求后续研究。

来自标准 JSON-RPC 的方法与字段以[Execution APIs 官方仓库](https://github.com/ethereum/execution-apis)及本地 Geth 实际支持版本为准。

### 13.5 `execution_traces` / `fee_recipient_balance_snapshots`（扩展）

为区分直接 payment、内部转账、builder/relay 支付，可按 block/transaction 保存 trace：调用路径、call type、from/to/value、gas、success/error、logs 或 state diff（按 tracer 输出）。保存 tracer 名称、配置、client version、超时和完整原始结果。

用 Geth `debug_traceBlockByHash` / `debug_traceTransaction` 等已启用方法采集；trace 是昂贵的客户端重执行结果，archive 不保证任意 tracer 任意历史都能在可接受时间返回。建议从获奖 block、fee recipient 相关转账和样本开始，后续补全。[Geth Debug API](https://geth.ethereum.org/docs/interacting-with-geth/rpc/ns-debug)

fee recipient balance 保存 parent/post execution block hash、address、balance_wei、查询成功状态。净余额变化只是对账信号，混有转入/转出和该地址自身交易，不能直接全部叫收益。

### 13.6 EL tips 与 MEV 的计算边界

第二阶段基本可验证量：

```text
priority_fees_wei = Σ receipt.gasUsed × (receipt.effectiveGasPrice - block.baseFeePerGas)
```

适用于有 base fee 的相应历史区间；旧格式按当时规则处理。base fee 被销毁，blob base fee 也不是 proposer 收益，不能将全部 gas fee 算作收入。

推荐分别记录：

- `el_priority_fees_wei`：已验证 receipt 推导；
- `direct_payments_to_fee_recipient_wei`：trace/交易中识别的独立转账；
- `builder_payment_wei`：有来源证据的 builder/relay 支付；
- `relay_reported_value_wei`：外部 relay 声称值；
- `el_observed_revenue_wei`：按明确地址和收支口径计算；
- `reconciliation_residual_wei`：尚未解释差额。

这几项**可能重叠，不能简单相加**。Builder bid、builder payment 和 fee recipient 的全部余额增量不是三个独立收益。没有 builder/relay 证据时，不能仅凭 Geth/Teku 判断某交易一定是 MEV；也不能证明完整套利利润。

### 13.7 `withdrawals`、存款事件、请求与内部转移

`withdrawals` 保存 withdrawal_index、validator_index、address、amount_gwei、payload block hash、beacon block root、withdrawal order。部分/全部提款类型需用当时状态规则派生，不只是看 amount。

`deposit_contract_events` 保存 EL block/tx/log identity、pubkey、withdrawal_credentials、amount、signature、deposit index 原始 bytes 与解码值。验证者身份可能在请求/存款后才建立，应允许 validator_index 暂缺，之后通过 pubkey 关联。

`pending_queue_snapshots` 保留请求入队和最终生效信息；deposit event/request、CL deposit operation、余额入账不是三个独立资金流。Consolidation source/target 是内部资金转移，对单 validator 的余额有影响，对合并后的实体总资产未必构成收益。

提款是资产从 CL 转到 EL，**不是新发放的奖励**；本金和历史收益混合提款不能全额计入某日收益。

## 14. 收益账本与对账设计（第二阶段）

### 14.1 `validator_income_ledger`

粒度：validator × 一次可解释余额变动。字段包括：event id、component、signed amount、unit、duty epoch、inclusion slot、applied state slot、CL/EL 层、资金流/收益分类、counterparty、证据引用、计算规则、confidence、是否已被其他分项包含。

建议 component：att_source、att_target、att_head、phase0_inclusion、inactivity、proposer_attestation、proposer_sync、proposer_slashing、attester_slashing_reward、sync_participation、initial_slash_penalty、correlation_penalty、deposit_applied、withdrawal、consolidation_in/out、EL_tip、builder_payment、other_transfer、unexplained。

不要在直接 API 分项中再重复加已包含的 whistleblower/proposer 奖励。可能重叠的账项通过 provenance/parent event 关联。

### 14.2 CL 净收益与余额对账

在两个**严格一致的状态边界**间，设 deposit 为真正进入 CL balance 的金额，transfer 为内部 consolidation 等流量：

```text
CL_net_income_gwei
  = balance_end - balance_start
    - deposits_applied
    + withdrawals
    - internal_transfers_in
    + internal_transfers_out
```

等式用于确定总余额收益，再和组件账本核对。`effective_balance` 的变化不是资金流；请求已发出但未处理也不是已入账 deposit。

```text
residual_gwei = CL_net_income_gwei - Σ(已识别 CL 奖惩净额)
```

记录 residual，不为了让曲线好看而填 0。可能原因包括边界错位、漏 inactivity/slashing、合并转移、字段语义/符号错误、缺失状态、fork 规则和余额扣款饱和。

原始奖励分项与实际余额扣款不总能在余额接近零时简单逐项相加，重放需遵循状态转换顺序。

### 14.3 收益率和比较口径

保留实际余额、有效余额、时间加权本金、active duration、deposit/withdrawal/transfer、资金流时间。分别提供：CL 收益、EL 收益、CL+EL 总收益、如有可信成本后的净收益。

APR/APY 的分母和年化方法需版本化；不再把每个 validator 都假设成恒定 32 ETH。出块/同步委员会机会损失是反事实估计，保留模型与不确定性，不能伪装成链上实际损失。

如果地址由 staking pool/builder/operator 控制，block 提议者收入与最终 validator 持有人收入应分层；没有协议分配/成本证据时，不输出持有人净利润。

## 15. Blob、数据可用性和未来格式扩展

### 15.1 `blob_commitments` / `blob_objects`

保存 block root、commitment index、KZG commitment、versioned hash、适用时的 proof、signed header/inclusion proof、sidecar 或 blob 原始对象引用、大小、采集时间、来源、验证结果。

新版 API 可能提供不同 blob/sidecar 读取方式；按本地已验证版本配置 adapter，不能把某一旧 API 路径当成永久接口。Blob bytes 不在普通 BeaconBlock JSON 内，也不保证 archive 永久保留。

### 15.2 `data_column_objects`

若本地 fork/客户端支持，保存 column index、block root、column/cells、KZG commitments/proofs、inclusion/header 证据、来源、验证状态和原始对象。单节点可能只保存其 custody 子集，不将“采到了若干 columns”标为全数据可用性证明。

### 15.3 Fork 扩展机制

保存 `fork_schedule`、`protocol_configs` 和实际 `version`。Phase0 → Altair → Bellatrix → Capella → Deneb → Electra → Fulu 分别测试；尚未激活或客户端不支持的后续格式，作为未来 adapter，不预先假定已上线。

在线 Beacon API master 可能包含开发中的 builder、payload envelope 等接口。**规范出现不等于本地主网/本地节点已支持**。采集器遇到未知版本应原始归档并标为待解析，之后补齐；不要直接丢失该区间。

## 16. API 到表的映射与频率

下面是接口族与采集计划，精确路径/参数/版本必须以固定规范和本地 capability probe 为准。标准接口入口见[Beacon API OpenAPI](https://raw.githubusercontent.com/ethereum/beacon-APIs/master/beacon-node-oapi.yaml)。

| 数据 | 接口/来源 | 频率 | 目标 |
| --- | --- | --- | --- |
| 网络身份/配置 | `/eth/v1/beacon/genesis`、`/eth/v1/config/spec`、`/eth/v1/config/fork_schedule` | 初次、节点升级、配置变化 | network/config/fork tables |
| 节点状态 | node version、syncing、health | 启动、定时、失败时 | node_capabilities / observations |
| 区块和 header | `/eth/v2/beacon/blocks/{block_id}`、`/eth/v1/beacon/headers/{block_id}` | 每个 slot / 已发现 root | raw + block/header/operations |
| 状态 root/fork/finality | states root/fork/finality_checkpoints | 边界、事件附近；需要精确 finality 可逐 block | state metadata / checkpoints |
| 完整历史 state | `/eth/v2/debug/beacon/states/{state_id}`，支持时 SSZ | 周期 checkpoint + fork/异常事件 | state archives |
| 验证者和余额 | states validators / validator_balances | epoch 边界，变化事件补充 | registry / balances |
| 投票委员会 | states committees | 每 epoch，按固定历史 state | committees / members |
| proposer duties | `/eth/v1/validator/duties/proposer/{epoch}` | 每 epoch，历史能力需测 | proposer_duties |
| attester duties | validator duties attester，POST validators 列表 | 可选；committees 已可提供主要映射 | duty cross-check |
| sync 成员 | states sync_committees | 每 period 与分支变化 | sync members |
| proposer rewards | GET rewards/blocks/{block_root} | 每个实际 block | proposer rewards |
| attestation rewards | POST rewards/attestations/{epoch} | 每个具备结算上下文的 epoch | actual + ideal rewards |
| sync rewards | POST rewards/sync_committee/{block_root} | 每个适用 block | sync rewards |
| 请求 pending queues | state API 支持时 / 完整 state | epoch/队列变化 | queue snapshots |
| EL block/tx | `eth_getBlockByHash(hash,true)` | 每 payload | EL blocks / transactions |
| receipts | `eth_getBlockReceipts`（若支持）或逐 tx receipt | 每 payload | receipts / logs |
| 合约 logs | `eth_getLogs`，分范围 | 存款和扩展研究区间 | contract events |
| 历史地址余额 | `eth_getBalance`，按 hash/state 支持核实 | proposer 事件/对账 | recipient balances |
| trace | debug trace 方法 | 可选批次、按需补采 | transfers / traces |
| blobs/columns | 该版本支持的 Beacon/Debug 接口 | 实时优先，历史探测 | availability objects |
| 实时链事件 | `/eth/v1/events`，topics 依支持情况 | 持续 | node_events / reorgs |
| fork-choice | 本地支持的 Debug API | 事件触发/采样 | fork_choice_snapshots |

对 attestation rewards，不将“epoch 的 blocks 都 finalized”视为其结算所需历史状态一定可用。确认客户端需要的后续状态已经形成并 finalized，再固定上下文采集；API 不能按 root 请求时应在请求前后核验 anchor，必要时重采。

全体验证者结果过大时，按 validator indices 分批请求并保留 batch coverage；不能假定所有 state/reward API 都具有通用分页接口。不同方法的空数组/省略请求体语义按规范与本地行为验证。

## 17. 历史回填与实时增量流程

### 17.1 历史回填

1. 固定网络配置、客户端版本、fork 配置、finalized 锚点和目标范围，保存本次任务 manifest。
2. 从 genesis/选定起点逐 slot 枚举。返回区块后立即转为按 root 抓取完整 block/header，避免 head 变化造成混读。
3. 验证 parent 链，建立空 slot 候选；只对确认有完整历史的 finalized 缺口标 skipped。
4. 按 epoch 读取/重放状态边界、委员会、职责、参与信息、实际/理想 rewards。
5. 对每 block 采 proposer/sync rewards；按 payload hash 对接 Geth block/receipt。
6. 所有原始对象归档后，再批量解析写入事实表；可采用有界并行，不要求所有网络调用串行。
7. 完成范围级校验后发布数据集；失败范围进入补洞，不因一个错误停止全部回填，也不静默跳过。
8. 恢复任务时读取已发布 manifest 和未完成子任务，而非单一最大 slot。

### 17.2 实时采集与 finalized 发布

- 实时发现事件先入 staging；block root 不变但 canonical 选择会变化。
- 对近期窗口反复核验 parent/canonical/finality，保留 reorg 版本。
- 待 finalized 且所需结算状态完整后，将对应批次发布到稳定事实表。
- 低延迟视图显示 provisional 标识；历史收益默认使用稳定数据。
- 一旦观测到不同 finalized 历史，保留原记录与冲突证据，暂停该范围发布，不以普通 reorg 静默覆盖。

### 17.3 请求与错误策略

超时/连接错误、429、5xx 可退避重试；404 需区分 skipped、历史缺失、所需 state 不可用；400/不支持应进入能力/参数诊断。保存响应原文。

设置节点级并发预算，区块轻请求、历史 state 重建、trace 分开限流。状态重建可能非常昂贵；利用 checkpoint 顺序重放/缓存，不按 validator 发数百万个独立 HTTP 请求。

## 18. ClickHouse 表引擎、排序和发布原则

### 18.1 优先不可变批次

对 finalized 事实，优先 `MergeTree` + 明确的 batch manifest + 经验证的幂等发布。重复批次在发布前排除；同一范围重算时生成新 dataset_version，原子切换查询入口或受控替换分区。

如用 `ReplacingMergeTree(version)`：相同排序键的替换依赖后台 merge，**不是写入时强唯一**；查询要有 FINAL 或正确的 argMax/版本过滤策略。跨分区替换和查询设置另需验证。[ClickHouse 官方引擎文档](https://clickhouse.com/docs/reference/engines/table-engines/mergetree-family/replacingmergetree)

不要把采集时间或不稳定 canonical flag 放进需要去重的完整排序键，否则更新会变成两个不同对象。版本跨分区时后台合并也不能替代明确治理。

### 18.2 建议访问路径

| 表族 | 分区建议 | 主排序建议 |
| --- | --- | --- |
| blocks / slot facts | slot 理论时间月份 | network、slot、block_root |
| operations / attestations | inclusion 月份 | network、inclusion_slot、block_root、operation_index |
| epoch rewards / balances | epoch 起点月份 | network、epoch、validator_index、context/version |
| validator 时间线 | 与主表兼容 | projection/独立服务表：network、validator_index、epoch |
| committees | duty 月份或较粗 epoch bucket | network、epoch、slot、committee_index、context |
| EL tx / receipts | block 月份 | network、block_number、block_hash、transaction_index |
| events | 观测月份 | node_id、observed_at、session/event sequence |

不按每 validator 或每 slot 建分区，不为每 block 单独提交极小 insert。没有 EL timestamp 的 Merge 前 CL 数据仍可用理论 slot time 分区。

### 18.3 聚合的去重陷阱

ClickHouse 增量物化视图基于插入块触发，上游后来通过 replacing 去重不会自动撤销已加到 sum 中的重复值。因此 raw/replacing 表不能未经幂等控制直接接收益总和物化视图。建议对已验证稳定批次生成聚合，或按范围重建、版本化发布。

初期先保证 correctness，再按实测添加 projection、dictionary 和预聚合。节点数据库、采集归档与 CH merge 争用同一磁盘时要预留 I/O 与空间，不在未测容量前承诺全历史完成时间。

## 19. 第二阶段建议派生的分析表

这些表只定义需求和依赖，当前不实现。

| 表 | 粒度 | 主要指标 | 依赖 |
| --- | --- | --- | --- |
| `slot_summary` | slot | block/skipped、proposer、operation 数量、inclusion 分布、EL tips | slot/block/operations/receipts |
| `epoch_summary` | epoch/context | 活跃 stake、source/target/head participation、finality、奖惩、队列 | states/committees/rewards |
| `attestation_evaluation` | inclusion + validator | 正确性、timely flags、first valid inclusion、重复证据 | votes/mapping/states/fork |
| `slot_vote_edges` | slot + source/target/head root | 验证者集合、有效余额、证据窗口 | attestation evaluation |
| `validator_epoch_summary` | validator/epoch | 职责、参与、实际/理想收益、奖惩、余额变化和 residual | duties/rewards/states/ledger |
| `validator_daily_income` | validator/date/时间口径 | CL、EL、流入/流出、罚款、净收益、本金、完整性 | epoch/slot ledger |
| `proposal_income_breakdown` | block/proposer | CL proposer、tips、payments、relay 值、对账差 | rewards/receipts/traces |
| `validator_lifecycle` | validator/event | deposit、activation、exit、slashing、withdrawal、consolidation | registry/operations/queues |
| `finality_timeline` | state/observation | checkpoint 边、finality lag、inactivity leak | checkpoints/states/events |
| `entity_income_summary` | entity/time | 验证者数、stake、收入、职责表现 | 时效化实体标签 + validator facts |
| `reward_distribution_metrics` | time/sample/method | Gini、HHI、entropy、effective entities、阈值集中度 | 完整性检查后的收入表 |
| `data_quality_summary` | dataset/range | coverage、失败、缺失、残差、版本 | manifests/quality checks |

EthVis 的 Casper/GHOST 图可以从 `slot_vote_edges` 等表生成，但图例需标明“canonical 链已包含 attestations 的事后重建”；有实时 fork-choice 数据时另做观测视图。

### 19.1 集中度指标的口径

- 保存全部活跃者和仅有该类奖励者两种样本；零收益本身有意义。
- 原始负收益不删不改；若 Gini/entropy 等方法要求非负输入，另建转换视图并记录截零/平移方法及影响。
- ETH2.0-reward 的 `calculate_shannon_entropy_index.py` 返回 `2**H`，是 effective number，不是 Shannon entropy H 本身，两者分别命名。
- 其 Nakamoto 代码采用累计收益份额严格大于 50% 的阈值。EthVis2 应保存 threshold/comparison operator，不能将奖励份额阈值直接解释为 PoS 安全 stake 阈值。
- entity 标签有误差与时间变化；输出 validator 粒度、entity 粒度和未识别份额，不能把 100 万 validator 当作 100 万独立运营者。

## 20. 数据质量与验收要求

### 20.1 原始和结构化一致性

- 原始归档可解压，hash/长度一致；解析条数与数组长度一致，未知字段有记录。
- 以相应 fork SSZ 实现重算 block/header/state roots，并与节点数据比较；按样本成本扩大核验。
- parent root 连续；slot 缺口有明确分类，不能由异常吞掉。
- committee mapping 的 bit 长度、位置、参与者数量一致；Electra 多委员会与 sync 重复位置有专门样本。
- CL payload ↔ EL block hash/number、transactions/withdrawals 顺序对应。
- 分批 validator 请求有完整覆盖证明，失败批次不假装空数组。

### 20.2 收益与余额一致性

- proposer reward 分项相加等于 total。
- attestation 正负组件与原始响应一致，包含 inactivity 和适用的 Phase0 字段。
- sync validator reward 不因多 position join 重复计数。
- 账本与状态余额对账，残差可定位；提款和本金 deposit 不混为收益。
- 没有职责/确定无事件的 0，与未采/接口失败的 NULL 区分。
- 参考仓库/第三方数据仅在相同日期归属、单位、组件、fork 和完整性口径下比较。

### 20.3 跨边界样本

至少覆盖 genesis、各已激活 fork 前后、epoch 首末 slot、空 slot、跨午夜、deposit、withdrawal、slash、退出、consolidation、多个 sync positions、重复 attestation 包含、重组实时样本及不支持接口。

这是一份后续实现验收清单，本文没有执行链上数据测试。不能以文档已生成推断节点历史已验证。

## 21. 采集优先级与保留策略

| 优先级 | 内容 | 建议 |
| --- | --- | --- |
| P0：基础完整性 | 网络/fork 配置、原始 signed blocks、headers、全部 slot 状态、所有 operations、采集审计 | 先完整，不能只选可视化已用字段 |
| P0：收益可解释 | epoch 边界 state/余额/registry、committees、duties、参与 flags、三类 actual rewards + ideal | 与 block 回填共同完成，避免只有 block 没有奖励上下文 |
| P0：短期易丢失 | 实时 head/reorg/finality、可用 blob/column | 从现在起尽早归档，历史任务不能恢复未来丢失观测 |
| P1：综合收益 | EL payload 对接、完整 tx/receipts、withdrawals、deposit/request logs | 全量回填，支持 tips 与资金流 |
| P1：重建保障 | 周期 SSZ state、fork/异常前后快照、manifest/checksum | 保留可验证重放基础 |
| P2：扩展归因 | traces、fee recipient 前后余额、relay、实体标签、协议分配 | 先样本/重点，再按研究需要扩展 |
| P2：最大成本 | 全量 data columns、全量 traces、每 slot 全体状态 | 不作为默认热库计划；先评估成本和可获得性 |

P0 中实时易丢失线可与历史回填并行，逻辑优先级不要求所有工作严格串行。

原始 signed blocks、配置、状态 checkpoint、采集清单和来源信息建议长期保留。大 trace/blob/state 使用冷存储。热表 TTL 必须在验证可重建、归档成功且研究允许后设置；默认不因“旧数据”直接删除证据。

## 22. 首轮 100,000 个 finalized blocks：端到端试跑方案

### 22.1 已确认范围与计数口径

用户已选择**近期已 finalized 的区间**。这里“从头到尾”指完整跑通分析流程，不指从 genesis 开始。

- 目标数量：`core_canonical_block_count = 100000`，按不同 canonical block root 计数。
- 不是 `end_slot - start_slot = 100000`；空 slot 不产生 block，但属于研究时间轴，仍需落表。
- 主样本为一个连续 canonical 链段，不随机挑散落的 blocks。
- 试跑时先确认链上实际激活 fork、节点历史状态和奖励端点能力，再冻结具体区间，不在文档中编造当前 finalized slot。
- 区间所需的额外 parent、epoch 边界、后续包含窗口及奖励结算状态是 `support_context`，不计入 100000 个主样本。
- 10 万 slots 在主网 12 秒节奏下约 13.89 天；**10 万实际 blocks 的时间跨度通常更长**，取决于空 slot 数，不等于 13.89 天的固定日历区间。

### 22.2 冻结区间和完整上下文

1. 读取网络配置、客户端版本、fork schedule 和 finalized checkpoint；选择有足够后续 finalized 结算状态的终点，不把“当前最后一个 finalized block”自动当作可立即结清收益的终点。
2. 固定 `anchor_finalized_root / epoch`，选定 `core_end_block_root` 后沿 parent_root 向前枚举，包含终点在内取得 100000 个 blocks。正向回填也可以，但最终必须满足相同计数和连续性约束。
3. 记录 `core_start_slot / end_slot`、首尾 block roots、完整 root 清单 hash；从最早到最晚 slot 补齐 slot 时间轴，并分类空 slot。
4. 把上下文扩到首个 epoch 的必要起始状态之前、末个 epoch 的有效包含窗口及奖励结算状态之后。额外 lookback/lookahead 由 fork 规则和客户端所需状态确定，不写死为一个通用 slots 数。
5. 边界 epoch 也采完整。核心 block 的 slot 视图与完整 epoch 的收益视图使用不同显式过滤器，不能把边界外 proposal 混入“核心 10 万块”的总额。
6. 日收益只把完整覆盖的 UTC 自然日作为可比较日；首尾不完整日标为 partial。Shanghai 时区只作显示或另一个明确统计维度。

范围 manifest 至少保存：`pilot_id`、network、anchor、core roots/count、core slots、support 范围、duty epochs、settlement states、完整日集合、fork、客户端/解析器/计算版本、required datasets、各表完成度。

### 22.3 试跑数据范围

| 内容 | 本轮要求 | 落点 |
| --- | --- | --- |
| 配置、任务和覆盖 | 全部保存，支持暂停/续跑/补洞 | PG control + 原始配置归档 |
| signed blocks / headers / operations | 10 万主块 + 必要支持区间；原始完整，结构化全 operations | 原始文件 + CH facts |
| slot / canonical / parent 关系 | 覆盖整个时间轴，空 slot 单列 | CH facts |
| committees / proposer / sync duties | 覆盖全部必要 epochs/periods 和历史上下文 | CH facts |
| validator registry / balances | 全体所需 validator、精确状态边界、字段存在性与完整覆盖 | 原始响应 + CH facts |
| participation / inactivity / checkpoints | 从 state/经验证重放提取；记录真实边界 | 原始 SSZ + CH facts |
| proposer / attestation / sync rewards | 全覆盖，含有符号分项和 ideal rewards | 原始响应 + CH facts |
| EL block / transactions / receipts / withdrawals | 核心区间全量，并补齐完整 epoch 对账所需支持数据 | 原始响应 + CH facts |
| 状态 checkpoint | 首尾、周期点、fork/异常事件附近，保留 SSZ | 原始归档 + 索引 |
| traces / relay / 实体识别 | 选定 proposal 样本扩展；未覆盖部分明确标 unknown | 原始对象 + 可选 CH 表 |
| blobs / columns | 完整记录 commitments；实际内容按节点保留能力采集并标覆盖 | 原始冷存储 + 索引 |

**全量 CL 收益 + EL tips 是本轮可验收的主要收入范围。** 没有完整 trace/relay/分配证据时，不把结果命名为“已完整恢复所有 MEV/持有人净利润”。所有原始 block 字段仍完整保留，不因为 trace/column 的选采而缩减 block 归档。

### 22.4 分阶段跑通，最终仍达到 10 万块

| 阶段 | 核心块规模 | 目的和出口 |
| --- | ---: | --- |
| capability probe | 少量固定样本 | 确认 API、状态与 fork，冻结正式范围 |
| smoke | 100 | 原始归档 → PG task → CH facts → 一次派生查询全通 |
| correctness | 1000 | 奖惩组件、余额边界、空 slot、重试和幂等核验；异常例可取支持样本 |
| pilot | 100000 | 完成目标区间全数据、聚合、查询和质量报告 |

100/1000 是同一冻结区间内的前置检查，不替换最终 100000 目标；先发现类型和边界错误，避免错误放大到几十亿行。

### 22.5 必须产出的分析结果

1. **共识链概览**：block/slot/epoch 数、缺块、活跃 stake、投票参与、inclusion 分布、finality 状态。
2. **slot/epoch 投票图数据**：head/source/target 边、成员、有效余额及正确性证据，注明事后重建口径。
3. **Validator epoch/day 时间线**：余额、有效余额、职责、参与、CL 分项奖惩、EL tips、deposit/withdrawal/transfer。
4. **proposal 收益分解**：CL proposer 分项 + EL tips；有额外证据时增加 payments，标记重叠和完整性。
5. **收益分布**：排名、分位数、Gini/HHI/entropy/effective number 等，保留样本与负值处理规则。
6. **质量与容量报告**：各表 rows/bytes、coverage、失败/补洞、对账 residual、节点负载、查询延迟和可重复运行结果。

本轮不要求先完成生产前端；这些结果可由 SQL、可复用查询、导出的图数据与报告验收，再接 EthVis 风格的页面。

### 22.6 完成条件

- 主样本恰好 100000 个不同 canonical roots，与 anchor 的祖先链关系可验证。
- 所有核心 slots 已分类；无未说明请求失败被归类为 skipped。
- 必需 epoch/validator/state/reward 数据覆盖完整，缺少依赖的范围不发布“完成”标记。
- block 与 EL hash、receipt、withdrawal 关联正确；金额全程整数。
- 余额账本按固定边界对账；完整目标范围逐条保存 residual，不能只抽样对账后声称全范围成功。未解释残差必须分类列入未解决项，不能仅以行数足够验收。
- 相同批次重跑不重复累计，任务中断可恢复，分析结果记录 dataset/calculation version。
- 全链容量按实测 rows/bytes 和历史 validator 数量估算；不以 10 万块的小 block 表大小替代整体预算。

### 22.7 这轮仍可能很大

即便只采 10 万 blocks，涉及的 epoch 数仍至少约 3125，且有边界/结算附加量。以 100 万 validators 的假设估算，一张 validator × epoch 表约 **31.25 亿行**；余额、奖励、参与信息各自展开会叠加。

因此第一阶段归档与解析必须流式、按 epoch/batch 提交；先保存有序数组和压缩响应，再按分析访问模式展开。不能把全部 epochs 的 validator 数据一次读进内存，也不能让 PG 控制库承载这些明细。1000 块检查后给出实际磁盘和耗时预算，再完成原定 10 万块。

## 23. 资料与可追溯性

本地参考文件路径见第 2 节；两仓库版本已记录。外部资料于本文日期查阅，开发时需固定规范版本并重新验证本地支持情况。

- [Ethereum Beacon APIs 官方仓库](https://github.com/ethereum/beacon-APIs)：接口和返回 schema。
- [Ethereum Consensus Specs 官方仓库](https://github.com/ethereum/consensus-specs)：数据类型、状态转换和奖励规则。
- [Teku 历史状态重建](https://docs.teku.consensys.io/how-to/reconstruct-historical-states)：archive/checkpoint 状态恢复边界。
- [Ethereum Execution APIs](https://github.com/ethereum/execution-apis)：执行层 JSON-RPC。
- [Geth Debug API](https://geth.ethereum.org/docs/interacting-with-geth/rpc/ns-debug)：trace 能力与参数。
- [ClickHouse ReplacingMergeTree](https://clickhouse.com/docs/reference/engines/table-engines/mergetree-family/replacingmergetree)：替换与查询时去重语义。
- [PostgreSQL Numeric Types](https://www.postgresql.org/docs/current/datatype-numeric.html)：金额和 uint64 范围选择。


## 24. chaind 采用评估：能用，但不能原样替代本项目采集层

### 24.1 核查版本与结论

本次只读检查了 [wealdtech/chaind](https://github.com/wealdtech/chaind)，固定 master commit：`77bef5cae806afd0ea8715ca0e7b1898d5a8d8c6`。GitHub releases API 返回的 latest release 为 `v0.10.2`（2025-12-10 发布）；master commit 与 release 不是自动等价，本轮实现应固定其中一个明确版本再测试。

**可以使用 chaind 获取大量共识层结构化数据；不能配置一个 ClickHouse URL 就直接得到本设计的完整数据层。** 官方 README 明确它支持 Teku，数据库后端只有 PostgreSQL；本次源码中的后端使用 pgx/PG 事务和 SQL，没有发现生产 ClickHouse 实现。[官方说明](https://github.com/wealdtech/chaind#requirements-to-run-chaind)、[PG 后端源码](https://github.com/wealdtech/chaind/blob/77bef5cae806afd0ea8715ca0e7b1898d5a8d8c6/services/chaindb/postgresql/service.go)

CHANGELOG 已记录 Electra、Fulu 支持和 Fulu block event 修复；不能把它判断为仅支持早期 Phase0 的旧工具。不过，是否兼容用户本地 Teku 版本及所选历史范围，仍需 capability probe 与样本运行验证。[更新记录](https://github.com/wealdtech/chaind/blob/77bef5cae806afd0ea8715ca0e7b1898d5a8d8c6/CHANGELOG.md)

### 24.2 已有能力与需要补采的差距

| 能力 | 本次核查结果 | EthVis2 处理 |
| --- | --- | --- |
| blocks、attestations、slashings、deposits、exits | 有相应采集/写表模块 | 可复用结构、映射和测试思路 |
| BLS change、withdrawals、Electra requests | 源码有对应结构与存储 | 保留 fork 适用性、请求与实际状态变动区别 |
| beacon/proposer/sync committees | 有模块；attestation 多委员会有专门处理 | 验证位置、顺序和数据源上下文 |
| finalizer / epoch/validator summaries | 有 canonical 与职责表现相关汇总 | 可作对照，不能代替第一阶段原始证据 |
| epoch 余额 | 有，默认开关关闭；读取 epoch 首 slot 的 validators | 额外核实状态语义，不自动等同 EthVis2 pre-block 边界 |
| 完整 SignedBeaconBlock / SSZ / API envelope | DB Block 类型没有区块外层 signature，也不保存完整响应 | 增加独立原始归档，不能只导出 chaind 的结构化表 |
| attestation / sync 签名 | 所检结构无签名字段 | 从原始区块保留完整签名、committee bits 等 |
| 历史 registry 版本 | registry 更新读取 `head` | 自行按历史 state 归档，不能拿当前 registry 替代过去状态 |
| 三类 reward + ideal/inactivity 明细 | 所检模块与 summary 类型未发现本设计的完整奖励采集/账本 | 独立请求 Teku rewards APIs，状态对账/重放补充 |
| 完整 EL transactions / receipts | ExecutionPayload 类型明确省略 transactions，未见通用全量 receipts 管线 | Geth 补采；deposit 专用读取不能等同全 EL 数据采集 |
| blobs | 有读取/保存开关；历史数据仍受节点保留限制 | 记录 unavailable，完整性不能凭开关推定 |
| 原生 ClickHouse | 没有发现实现，官方仅列 PG | 若原样运行需要 PG 暂存，再 ETL 到 CH |
| 有限范围 batch/恰好 10 万块停止 | 未发现通用 end-slot/max-blocks CLI | 独立范围调度或改造，不能直接运行后任其追到 head |

证据：[数据类型](https://github.com/wealdtech/chaind/blob/77bef5cae806afd0ea8715ca0e7b1898d5a8d8c6/services/chaindb/types.go)、[block 处理](https://github.com/wealdtech/chaind/blob/77bef5cae806afd0ea8715ca0e7b1898d5a8d8c6/services/blocks/standard/handler.go)、[validator 读取](https://github.com/wealdtech/chaind/blob/77bef5cae806afd0ea8715ca0e7b1898d5a8d8c6/services/validators/standard/handler.go)、[validator epoch 汇总](https://github.com/wealdtech/chaind/blob/77bef5cae806afd0ea8715ca0e7b1898d5a8d8c6/services/summarizer/standard/validatorepoch.go)。这些是固定版本的代码检查结论，没有安装或运行 chaind，未做完整正确性审计。

### 24.3 两条接入路线与推荐

| 路线 | 数据流 | 优点 | 代价/是否符合当前分工 |
| --- | --- | --- | --- |
| A：直接运行上游 chaind | Teku → chaind → 独立 PG chain staging → 校验/ETL → CH；另补 raw/rewards/Geth | 上游采集模块可直接使用 | PG 额外存链上明细，需增加暂存职责；还需限定范围、补采和 ETL |
| B：保持 PG 纯控制库 | PG tasks → EthVis2 范围采集器 → raw archive + CH；借鉴 chaind 解析/委员会逻辑，或复用其客户端依赖 | 直接符合已确认的 CH 分析、PG 控制分工，范围与完整归档可统一 | 需要实现少量本项目编排和写入；不等于零开发 |

**本设计默认路线 B，chaind 用作参考实现和可选交叉校验来源。** 这不是否定 chaind，而是遵守已经明确的数据库职责和完整数据目标。若希望优先直接运行现成 chaind，则选择路线 A，并明确接受独立 PG 明细暂存；当前没有默默替用户改变 PG 的用途。

不建议首轮就实现 chaind 的全部 ClickHouse backend：它的接口围绕 PG 事务、更新、canonical/finalizer/summarizer 组织，换驱动不足以解决一致性和去重。更小的复用粒度是：已验证的数据类型、fork 分派、委员会解码、上游 `go-eth2-client` 客户端以及参考测试。若后续沿用 Go，可评估 `go-eth2-client`；其具体 API/版本以固定依赖及样本结果为准。

### 24.4 为什么 `--blocks.start-slot` 不足以约束试采

固定版本 `main.go` 注册了 `blocks.start-slot`、refetch、各模块 enable 等，但未注册通用 `blocks.end-slot` 或 `max-blocks`。block catchup 循环会追到 `CurrentSlot()`；committee catchup 会追到 `CurrentEpoch()`。余额、职责、sync 和 summarizer 各有自己的进度/循环，不是统一受 blocks 起点约束。[配置源码](https://github.com/wealdtech/chaind/blob/77bef5cae806afd0ea8715ca0e7b1898d5a8d8c6/main.go)、[block catchup](https://github.com/wealdtech/chaind/blob/77bef5cae806afd0ea8715ca0e7b1898d5a8d8c6/services/blocks/standard/handler.go)、[committee catchup](https://github.com/wealdtech/chaind/blob/77bef5cae806afd0ea8715ca0e7b1898d5a8d8c6/services/beaconcommittees/standard/handler.go)

因此：

- 只设置 blocks 起点，不能证明其他模块不会从更早 epoch 扫起。
- 定时观察 PG 的 block count，达到 100000 再杀进程不是严格边界：会有并发、未完成依赖和后续结算问题。
- 需要有限范围 runner/改造，统一 block、epoch、period、finalizer、summarizer 的主样本及 support 范围；或者关闭超范围模块，由本项目受控补采。
- 不能给出并不存在的 `--blocks.end-slot` 或 `--max-blocks=100000` 当作可执行命令。
- 默认 blob fetch 开启并不表示旧 blobs 可找回；余额默认关闭也不能忘记开启/补采，否则流程缺少收益对账基础。

### 24.5 若采用路线 A，如何避免上游表污染分析和控制库

此节是备选方案，不表示已批准或已部署 PG 暂存。

1. 单独数据库/角色运行 chaind，建议概念名 `chaind_staging`；`ethvis_control` 与其隔离，避免自动迁移、巨大明细表和 pruning 影响控制数据。
2. 固定 chaind commit/tag、migration 版本和 schema fingerprint。不要沿用 README 中旧 Geth 参数就直接修改当前节点；先验证实际客户端版本和历史查询能力。
3. 仅发布已 finalized、上下文完整且核验过的批次到 CH。canonical 从 NULL 到 true/false 的后续更新必须同步，不能只做一次 append。
4. 按 inclusion identity / block root 建字段映射，转换 PG BYTEA、数组、NUMERIC 及 NULL 语义；保留 chaind source 与自采 source 的 provenance。
5. `t_metadata` 是 chaind 的内部游标；EthVis2 不把它当覆盖验收证明，也不在未测试语义前手工伪造进度。
6. 上游汇总字段与原始链上字段分开；`f_head_correct` 等派生值应带 chaind 版本，供对照而非当作唯一真值。
7. raw/rewards/Geth 补采完成且 CH 核验后才允许清理 staging；未完成前不启用会删除研究证据的 retention。

### 24.6 迁移时必须注意的实际语义

- `t_validators` 把 `FAR_FUTURE_EPOCH` 转为 NULL；导出时需记录这种 NULL 是哨兵映射，不是未采到。
- `t_blocks.f_canonical` / `t_attestations.f_canonical` 为三态：NULL 是尚未确定，不能转为 false。
- 0.9.0 起 `f_committee_index` 被弃用，改用 `f_committee_indices`；旧库不自动迁移全部历史，兼容导出不能只读一个旧列。
- epoch balance 采集会跳过余额为 0 的 validators；所以“该 validator 没有一行”可能是上游省略零值，也可能是采集缺失。必须结合覆盖清单、registry/原始历史响应区分，不能统一补 0。
- registry 更新取 head，因此同一 validator 的最新 credentials/effective balance 不能直接用于过去 epoch 的收益计算。
- 执行 payload 的 transaction 缺失是该结构有意省略，不是空区块；不能写 `transaction_count=0`。

三态/哨兵语义见[表说明](https://github.com/wealdtech/chaind/blob/77bef5cae806afd0ea8715ca0e7b1898d5a8d8c6/docs/tables.md)，多委员会兼容说明见[README](https://github.com/wealdtech/chaind#notes-for-090-and-electra)，零余额和 registry 行为见前述 validator handler。

### 24.7 当前交付边界

本次完成：双库分工确认、chaind 固定源码评估、首轮 10 万个近期 finalized blocks 的端到端范围和验收设计。没有启动下载链数据、创建数据库、安装 chaind 或修改节点。

进入执行阶段需要本地 Teku REST、Geth JSON-RPC、CH/PG 连接配置及归档目录；凭据通过本地配置/环境变量提供，不写入本文或控制台日志。试采开始时冻结 anchor 与实际范围，再输出每阶段覆盖和容量报告。


## 25. 采集器代码与语言设计

采集器建议使用 Go，分析使用 ClickHouse SQL，Python 用于研究与交叉验证。模块布局、Go 接口、任务依赖、精确范围、归档、PG 租约、CH 幂等发布和实现顺序见 [COLLECTOR_CODE_DESIGN.md](COLLECTOR_CODE_DESIGN.md)。该文件依据本地 `../Archived/chaind` 与 `../Archived/ETH2.0-reward` 源码检查编写，目前仍为实现设计。
