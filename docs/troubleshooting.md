# Troubleshooting

`kubectl describe` on any Kapture resource shows Events for the key
transitions (shard distribution, aborts, denied targets, job failures) —
start there. Controller metrics are on each manager's `:8080/metrics`;
capture agent metrics on `:8081/metrics` of each agent pod.

## TrafficCapture stuck in Pending

- **Storage not Ready**: `kubectl get capturestorage -n <ns>` — the
  referenced CaptureStorage must be phase `Ready`. Its `Ready` condition
  message names the invalid field.
- **Route not found**: the `targetRef` HTTPRoute/GRPCRoute must exist in
  the same namespace. Check the capture's conditions for
  `StorageNotFound` / target errors.
- **Agent pods not scheduling**: `kubectl get pods -n <ns> -l
  app.kubernetes.io/component=capture-agent` — image pull or resource
  pressure.

## Capture agents run but no data arrives in storage

- Confirm the Gateway implementation supports `RequestMirror` and the
  filter is present on the route
  (`kubectl get httproute <name> -o yaml`).
- Check agent metrics: `capture_agent_requests_total` zero means no
  mirrored traffic reaches the agent (gateway-side issue);
  `capture_agent_storage_write_errors_total` rising means storage
  credentials/connectivity; `capture_agent_queue_dropped_total` rising
  means the backend cannot keep up (raise `--write-queue-size`, or
  lower `filters.percentage`).
- Filters: `filters.percentage` below 100 and path/header filters drop
  requests by design (`capture_agent_requests_filtered_total`).

## CaptureLoadTest stays Pending with NoEligibleSpokes

The hub found no connected spoke matching `distribution.cells`.

- `kubectl get capturehub -o yaml` — `status.cells` lists cells and
  connected spoke counts as the hub sees them. A spoke missing here is
  not registered: check spoke logs for `registered with hub`, hub
  address/TLS config, and that the spoke's `--cell` matches the cell
  name in the load test exactly.
- With mTLS, a spoke whose name does not match its certificate CN/SAN
  is rejected at registration (`PermissionDenied ... does not match the
  client certificate identity`). Fix the certificate or the spoke name.

## CaptureLoadTest Failed immediately with TargetDenied

`spec.target.host` matches no pattern in `spec.safety.allowedHosts`.
This is terminal by design — replaying at the wrong host is the worst
failure mode of a load-testing system. Fix the allowlist or the target
and recreate the resource.

## Replay shards fail with "no captured requests found"

The replay preflight failed: the capture ID has no data (typo?) or,
with `distribution.presharded: true`, the preshard slice layout for
that exact shard count does not exist. The slice layout is
`{captureID}/shards/{i}-of-{n}` — `n` must equal participating spokes ×
`workersPerSpoke`. Re-run `kapture-preshard` with the matching
`--shard-count`, or drop `presharded` to stream the full capture.

A "manifest formatVersion N is newer than supported" error means the
dataset was written by a newer preshard than the replay engine —
upgrade the replay-engine image.

## Hub restarts / spokes reconnecting

Spokes reconnect with jittered backoff and re-register automatically;
in-flight shards keep running (they're Jobs on the spokes). Load-test
state is re-aggregated from spoke heartbeats. Shards whose
CaptureLoadTest vanished while a spoke was disconnected are deleted by
the spoke's orphan GC within a few minutes.

## Multiple CaptureHub resources

Only the oldest CaptureHub drives the gRPC server; the others carry an
`Active: False / NotAuthoritative` condition naming the authoritative
one. Deleting the authoritative CR fails over to the next oldest.

## TLS errors between hub and spokes

- `connection refused`: hub server not listening — check the CaptureHub
  exists and `kubectl logs` on the hub for `Starting gRPC server`.
- `certificate signed by unknown authority`: the spoke's `ca.crt` does
  not match the hub's serving certificate. During CA rotation, serve a
  bundle containing old and new CAs first (see
  [tls-rotation.md](tls-rotation.md)).
- `PermissionDenied ... certificate identity`: spoke name and client
  certificate CN/DNS SAN disagree.
- Certificates renewed by cert-manager are picked up in place by the
  hub and on next reconnect by spokes — if a spoke still presents an
  old certificate long after renewal, check that its secret mount
  updated (kubelet sync can take ~1 minute).

## Directive buffer full (ResourceExhausted in hub logs)

A spoke with many assigned shards fell behind consuming directives.
The coordinator retries automatically; if
`kapture_hub_directive_rejects_total{reason="buffer_full"}` keeps
rising, raise the hub's `--directive-buffer-size`.

## Load test aborted unexpectedly

`kubectl describe captureloadtest` — the `Aborted` event and condition
carry the reason: `maxDuration` exceeded or the error rate crossed
`abort.errorPercent` (evaluated after `minSampleRequests`). Per-shard
failure detail is on the TrafficReplays:
`kubectl get trafficreplays -l capture.gateway.io/load-test=<name>`.
