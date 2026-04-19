CREATE TABLE IF NOT EXISTS captures (
  id text PRIMARY KEY,
  namespace text NOT NULL,
  name text NOT NULL,
  status text NOT NULL,
  target_kind text,
  target_name text,
  storage_name text,
  storage_type text,
  started_at timestamptz,
  completed_at timestamptz,
  error text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS captures_namespace_status_idx
  ON captures (namespace, status, updated_at DESC);

CREATE INDEX IF NOT EXISTS captures_started_at_idx
  ON captures (started_at DESC);

CREATE TABLE IF NOT EXISTS capture_artifacts (
  id bigserial PRIMARY KEY,
  capture_id text NOT NULL REFERENCES captures(id) ON DELETE CASCADE,
  storage_backend text NOT NULL,
  s3_bucket text NOT NULL,
  s3_key text NOT NULL,
  s3_region text,
  content_type text,
  content_encoding text,
  size_bytes bigint,
  sha256 text,
  created_at timestamptz NOT NULL DEFAULT now(),
  expires_at timestamptz,
  UNIQUE (capture_id, s3_bucket, s3_key)
);

CREATE INDEX IF NOT EXISTS capture_artifacts_capture_id_idx
  ON capture_artifacts (capture_id);

CREATE INDEX IF NOT EXISTS capture_artifacts_s3_location_idx
  ON capture_artifacts (s3_bucket, s3_key);
