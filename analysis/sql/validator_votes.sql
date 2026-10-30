-- Bind parameters through ClickHouse HTTP param_* settings or a driver.
SELECT validator_index, attestation_slot, head_root,
       source_epoch,source_root,target_epoch,target_root,
       slot AS inclusion_slot,block_root AS inclusion_block_root,
       attestation_index,committee_index,committee_position
FROM ethvis.validator_attestation_inclusions_published
WHERE run_id={run:String} AND network={network:String}
  AND validator_index={validator:UInt64}
  AND attestation_slot>={start:UInt64} AND attestation_slot<{end:UInt64}
ORDER BY attestation_slot,slot,block_root,attestation_index;
