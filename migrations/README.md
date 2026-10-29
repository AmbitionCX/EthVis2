Schemas are embedded in the binary from `internal/collector/schema/`.
`ethvis-collector init` explicitly creates only the `ethvis_*` control tables
and the configured ClickHouse database/tables/views. Collection does not migrate schemas.
Use an existing PostgreSQL database and a role permitted to create these tables.
