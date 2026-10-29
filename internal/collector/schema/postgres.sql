CREATE TABLE IF NOT EXISTS ethvis_runs (
 id text PRIMARY KEY, manifest_hash text NOT NULL, manifest jsonb NOT NULL,
 status text NOT NULL DEFAULT 'planned', created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS ethvis_tasks (
 run_id text NOT NULL REFERENCES ethvis_runs(id), id text NOT NULL, kind text NOT NULL,
 spec jsonb NOT NULL, status text NOT NULL DEFAULT 'pending', generation bigint NOT NULL DEFAULT 0,
 attempt_id text, lease_until timestamptz, error text, rows bigint, raw_refs jsonb,
 updated_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(run_id,id)
);
CREATE INDEX IF NOT EXISTS ethvis_tasks_queue ON ethvis_tasks(run_id,status,kind,id);
