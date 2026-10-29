CREATE DATABASE IF NOT EXISTS {{db}};
CREATE TABLE IF NOT EXISTS {{db}}.records (
 run_id String, task_id String, attempt_id String, dataset LowCardinality(String),
 row_id String, slot UInt64, epoch UInt64, block_root String, state_root String, core Bool,
 payload String, payload_sha256 FixedString(64), raw_sha256 String
) ENGINE=ReplacingMergeTree ORDER BY (run_id,task_id,attempt_id,dataset,row_id);
CREATE TABLE IF NOT EXISTS {{db}}.published (
 run_id String, task_id String, attempt_id String, generation UInt64, rows UInt64
) ENGINE=ReplacingMergeTree(generation) ORDER BY (run_id,task_id);
CREATE VIEW IF NOT EXISTS {{db}}.facts AS
 SELECT f.* FROM (SELECT * FROM {{db}}.records FINAL) AS f
 INNER JOIN (SELECT * FROM {{db}}.published FINAL) AS p
 ON f.run_id=p.run_id AND f.task_id=p.task_id AND f.attempt_id=p.attempt_id;
CREATE VIEW IF NOT EXISTS {{db}}.proposer_rewards AS
 SELECT run_id,block_root,slot,epoch,core,
 JSONExtractString(payload,'proposer_index') AS validator_index,
 toUInt64(JSONExtractString(payload,'total')) AS total_gwei,
 toUInt64(JSONExtractString(payload,'attestations')) AS attestations_gwei,
 toUInt64(JSONExtractString(payload,'sync_aggregate')) AS sync_aggregate_gwei,
 toUInt64(JSONExtractString(payload,'proposer_slashings')) AS proposer_slashings_gwei,
 toUInt64(JSONExtractString(payload,'attester_slashings')) AS attester_slashings_gwei
 FROM {{db}}.facts WHERE dataset='proposer_rewards';
CREATE VIEW IF NOT EXISTS {{db}}.attestation_rewards AS
 SELECT run_id,epoch,core,JSONExtractString(payload,'validator_index') AS validator_index,
 toInt64(JSONExtractString(payload,'head')) AS head_gwei,
 toInt64(JSONExtractString(payload,'target')) AS target_gwei,
 toInt64(JSONExtractString(payload,'source')) AS source_gwei,
 toInt64OrNull(JSONExtractString(payload,'inactivity')) AS inactivity_gwei,
 toInt64OrNull(JSONExtractString(payload,'inclusion_delay')) AS inclusion_delay_gwei
 FROM {{db}}.facts WHERE dataset='attestation_rewards';
CREATE VIEW IF NOT EXISTS {{db}}.sync_rewards AS
 SELECT run_id,block_root,slot,epoch,core,
 JSONExtractString(payload,'validator_index') AS validator_index,
 toInt64(JSONExtractString(payload,'reward')) AS reward_gwei
 FROM {{db}}.facts WHERE dataset='sync_rewards';

CREATE VIEW IF NOT EXISTS {{db}}.validator_snapshots AS
 SELECT run_id,epoch,slot,state_root,core,
 JSONExtractString(payload,'index') AS validator_index,
 toUInt64(JSONExtractString(payload,'balance')) AS balance_gwei,
 toUInt64(JSONExtractString(payload,'validator','effective_balance')) AS effective_balance_gwei,
 JSONExtractString(payload,'validator','pubkey') AS pubkey,
 JSONExtractString(payload,'validator','withdrawal_credentials') AS withdrawal_credentials
 FROM {{db}}.facts WHERE dataset='validators';
CREATE VIEW IF NOT EXISTS {{db}}.attestations AS
 SELECT run_id,block_root,slot AS inclusion_slot,epoch,core,row_id AS inclusion_index,
 toUInt64(JSONExtractString(payload,'data','slot')) AS attestation_slot,
 JSONExtractString(payload,'data','beacon_block_root') AS head_root,
 JSONExtractString(payload,'data','source','root') AS source_root,
 JSONExtractString(payload,'data','target','root') AS target_root,
 JSONExtractString(payload,'aggregation_bits') AS aggregation_bits,
 JSONExtractString(payload,'committee_bits') AS committee_bits,
 JSONExtractString(payload,'signature') AS signature
 FROM {{db}}.facts WHERE dataset='attestations';
