# RDS Capture History

Kapture stores capture payloads in the configured storage backend. For S3 captures, the JSONL gzip artifacts remain in S3; PostgreSQL/RDS stores only the capture history and S3 object metadata.

## Schema

Apply `migrations/001_capture_history.sql` to the PostgreSQL database before enabling history.

The migration creates:

- `captures`: one row per `TrafficCapture`, keyed by the capture ID (`namespace/name`)
- `capture_artifacts`: one row per uploaded S3 object, keyed by `(capture_id, s3_bucket, s3_key)`

## Helm Configuration

Use an existing Kubernetes Secret for the database URL:

```yaml
history:
  enabled: true
  databaseURL:
    secretName: kapture-history-db
    secretKey: DATABASE_URL
```

The secret value should be a PostgreSQL connection URL, for example:

```text
postgres://kapture:password@my-rds.cluster-abc.us-east-1.rds.amazonaws.com:5432/kapture?sslmode=require
```

Capture agents are created in the same namespace as each `TrafficCapture`, so the referenced secret must exist in every namespace where captures run. For local development only, a direct value is also supported:

```yaml
history:
  enabled: true
  databaseURL:
    value: postgres://kapture:password@localhost:5432/kapture?sslmode=disable
```

## Runtime Behavior

When `DATABASE_URL` is present in a capture-agent pod:

1. The agent upserts a `captures` row with status `running`.
2. The normal storage writer uploads capture data to S3.
3. After each successful S3 upload, the writer records bucket, key, region, size, SHA-256, content type, and encoding in `capture_artifacts`.
4. On shutdown, the agent marks the capture `completed` or `failed`.

If `DATABASE_URL` is not configured, capture agents behave as before and no RDS writes are attempted.
