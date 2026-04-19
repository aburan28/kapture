# RDS Capture History

Kapture stores capture payloads in the configured storage backend. For S3 captures, the JSONL gzip artifacts remain in S3; PostgreSQL/RDS stores only the capture history and S3 object metadata.

## Schema

Apply `migrations/001_capture_history.sql` to the PostgreSQL database before enabling history.

The migration creates:

- `captures`: one row per `TrafficCapture`, keyed by the capture ID (`namespace/name`)
- `capture_artifacts`: one row per uploaded S3 object, keyed by `(capture_id, s3_bucket, s3_key)`

## Helm Configuration

Use an existing Kubernetes Secret for the database URL in shared or production-like environments:

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

## Local Development PostgreSQL

For local Kubernetes development, the Helm chart can deploy a single-node PostgreSQL StatefulSet and initialize it with the capture history schema:

```yaml
history:
  enabled: true
  localPostgres:
    enabled: true
    auth:
      database: kapture
      username: kapture
      password: kapture
```

Install with local Postgres enabled:

```bash
helm upgrade --install kapture charts/kapture \
  --namespace capture-system \
  --create-namespace \
  --set history.enabled=true \
  --set history.localPostgres.enabled=true
```

When `history.localPostgres.enabled` is true, the chart:

1. Creates a PostgreSQL `Secret`, `Service`, and `StatefulSet`.
2. Mounts `charts/kapture/files/001_capture_history.sql` into `/docker-entrypoint-initdb.d` so fresh databases start with the capture history tables.
3. Injects an in-cluster `DATABASE_URL` into the spoke controller.
4. Lets the spoke pass that URL to capture-agent pods, even when captures run in namespaces other than the chart namespace.

This local database is intended for development only. Use `history.databaseURL.secretName` for RDS or any shared database.

## Runtime Behavior

When `DATABASE_URL` is present in a capture-agent pod:

1. The agent upserts a `captures` row with status `running`.
2. The normal storage writer uploads capture data to S3.
3. After each successful S3 upload, the writer records bucket, key, region, size, SHA-256, content type, and encoding in `capture_artifacts`.
4. On shutdown, the agent marks the capture `completed` or `failed` with a bounded database update timeout.

If `DATABASE_URL` is not configured, capture agents behave as before and no RDS writes are attempted.
