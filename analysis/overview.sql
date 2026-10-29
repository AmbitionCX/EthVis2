-- Replace the run id and database as needed. Only published attempts are read.
SELECT dataset, count() AS rows FROM ethvis.facts
WHERE run_id = 'pilot_100k' GROUP BY dataset ORDER BY dataset;

SELECT count() AS core_blocks, min(slot), max(slot)
FROM ethvis.facts WHERE run_id='pilot_100k' AND dataset='beacon_blocks' AND core;

SELECT JSONExtractString(payload,'status') AS status,count()
FROM ethvis.facts WHERE run_id='pilot_100k' AND dataset='slots' GROUP BY status;

SELECT epoch,sum(toInt128(head_gwei)+target_gwei+source_gwei+
 ifNull(inactivity_gwei,0)+ifNull(inclusion_delay_gwei,0)) AS attestation_net_gwei
FROM ethvis.attestation_rewards WHERE run_id='pilot_100k' AND core GROUP BY epoch ORDER BY epoch;

-- Stream this JSONEachRow result into a file for Node.js verification.
-- Keep this to a research sample initially; no all-validator in-memory frame.
SELECT dataset,payload FROM ethvis.facts
WHERE run_id='pilot_100k' AND core AND dataset IN ('proposer_rewards','attestation_rewards','sync_rewards')
LIMIT 100000 FORMAT JSONEachRow;
