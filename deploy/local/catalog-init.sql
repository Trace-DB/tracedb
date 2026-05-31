CREATE TABLE IF NOT EXISTS organizations (
  org_id text PRIMARY KEY,
  name text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS projects (
  project_id text PRIMARY KEY,
  org_id text NOT NULL REFERENCES organizations(org_id),
  name text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS databases (
  database_id text PRIMARY KEY,
  project_id text NOT NULL REFERENCES projects(project_id),
  name text NOT NULL,
  region text NOT NULL,
  endpoint text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS branches (
  branch_id text PRIMARY KEY,
  database_id text NOT NULL REFERENCES databases(database_id),
  parent_branch_id text,
  state text NOT NULL,
  endpoint text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS api_keys (
  api_key_id text PRIMARY KEY,
  org_id text NOT NULL REFERENCES organizations(org_id),
  token_hash text NOT NULL,
  scopes jsonb NOT NULL DEFAULT '[]'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  revoked_at timestamptz
);
-- API tokens stored as bcrypt hashes; never store or log plaintext tokens.
-- Use bcrypt::verify(plaintext, stored_hash) for token validation.

INSERT INTO organizations (org_id, name)
VALUES ('local-org', 'Local TraceDB')
ON CONFLICT (org_id) DO NOTHING;

INSERT INTO projects (project_id, org_id, name)
VALUES ('local-project', 'local-org', 'Local Project')
ON CONFLICT (project_id) DO NOTHING;

INSERT INTO databases (database_id, project_id, name, region, endpoint)
VALUES ('db_local', 'local-project', 'local', 'local', 'http://tracedb-engine:8080')
ON CONFLICT (database_id) DO NOTHING;

INSERT INTO branches (branch_id, database_id, parent_branch_id, state, endpoint)
VALUES ('db_local:main', 'db_local', NULL, 'ACTIVE', 'http://tracedb-engine:8080')
ON CONFLICT (branch_id) DO NOTHING;
