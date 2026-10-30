SELECT 'blocks' AS dataset,count() AS rows FROM ethvis.beacon_blocks_published WHERE run_id={run:String}
UNION ALL SELECT 'aggregates',count() FROM ethvis.attestation_inclusions_published WHERE run_id={run:String}
UNION ALL SELECT 'member_inclusions',count() FROM ethvis.validator_attestation_inclusions_published WHERE run_id={run:String}
UNION ALL SELECT 'mapped_blocks',count() FROM ethvis.attestation_mapping_coverage_published WHERE run_id={run:String} AND row_id='block';
