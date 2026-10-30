-- One row per aggregate inclusion; graph can request member details separately.
SELECT block_root,slot AS inclusion_slot,attestation_index,
       attestation_slot,head_root,source_epoch,source_root,
       target_epoch,target_root,participant_count
FROM ethvis.attestation_inclusions_published
WHERE run_id={run:String} AND network={network:String}
  AND slot>={start:UInt64} AND slot<{end:UInt64}
ORDER BY slot,attestation_slot,attestation_index;
