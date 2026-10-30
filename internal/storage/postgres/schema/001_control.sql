CREATE TABLE IF NOT EXISTS ethvis_schema (id integer PRIMARY KEY CHECK(id=1),version integer NOT NULL);
INSERT INTO ethvis_schema VALUES(1,3) ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS ethvis_runs(id text PRIMARY KEY,manifest_hash text NOT NULL,manifest jsonb NOT NULL,status text NOT NULL DEFAULT 'planned',discovery_complete boolean NOT NULL DEFAULT false,created_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS ethvis_tasks(run_id text NOT NULL REFERENCES ethvis_runs(id),id text NOT NULL,kind text NOT NULL,spec jsonb NOT NULL,status text NOT NULL DEFAULT 'pending',generation bigint NOT NULL DEFAULT 0,attempt_id text,lease_until timestamptz,error text,rows bigint,raw_refs jsonb,updated_at timestamptz NOT NULL DEFAULT now(),PRIMARY KEY(run_id,id));
CREATE INDEX IF NOT EXISTS ethvis_tasks_queue ON ethvis_tasks(run_id,kind,status,id);
CREATE TABLE IF NOT EXISTS ethvis_task_dependencies(run_id text NOT NULL,task_id text NOT NULL,dependency_id text NOT NULL,PRIMARY KEY(run_id,task_id,dependency_id),FOREIGN KEY(run_id,task_id) REFERENCES ethvis_tasks(run_id,id),FOREIGN KEY(run_id,dependency_id) REFERENCES ethvis_tasks(run_id,id));

CREATE TABLE IF NOT EXISTS ethvis_attempts(run_id text NOT NULL,task_id text NOT NULL,attempt_id text NOT NULL,generation bigint NOT NULL,status text NOT NULL,rows bigint,raw_refs jsonb,error text,created_at timestamptz NOT NULL DEFAULT now(),updated_at timestamptz NOT NULL DEFAULT now(),PRIMARY KEY(run_id,task_id,attempt_id),FOREIGN KEY(run_id,task_id) REFERENCES ethvis_tasks(run_id,id));
