Schema v3 has one source per database:
- internal/storage/postgres/schema/001_control.sql
- internal/storage/clickhouse/schema/001_facts.sql

Schemas are embedded in the new CLI. init explicitly creates them; collect
never migrates. Existing v0.2 test data requires explicit project reset first.
reset is a dry run unless --execute is supplied. It preserves .env and never
removes Teku/Geth chain data. Run from the repository root; automatic filesystem
cleanup is restricted to configured strict descendants of ./var, without symlinks.
The old internal/collector/schema files are legacy references, not active DDL.
