# Kapture UI

Web dashboard for the Kapture Kubernetes traffic capture controller.

## Architecture

### How the UI connects to the Hub

The Kapture UI is a Next.js application that communicates **exclusively with the
Hub controller**.  The hub aggregates capture status from all connected spokes
via gRPC and exposes a Query API.  The UI never talks to spokes directly.

```
Browser ──HTTP/JSON──> Next.js API Routes ──gRPC──> Hub (:9443)
                       (src/app/api/*)                  |
                                                        | aggregates via
                                                        | heartbeats + status
                                                        v
                                                  Spoke 1 … Spoke N
                                                        |
                                                        | deploys & manages
                                                        v
                                                  Capture Agents
                                                  (traffic sinks)
```

The Next.js API routes (`src/app/api/`) act as a thin HTTP-to-gRPC bridge.  This
keeps the browser from needing a gRPC client and lets us handle auth, caching,
and error mapping in one place.

### Hub gRPC Query API (what the UI consumes)

These are the three query RPCs defined in `proto/hub/v1/hub.proto` that power
the dashboard:

| RPC                | UI Route              | Description                                              |
|--------------------|-----------------------|----------------------------------------------------------|
| `ListCaptures`     | `GET /api/captures`   | All captures, optionally filtered by `spoke_id`/`namespace` |
| `GetCaptureStatus` | `GET /api/captures/:name` | Single capture detail (aggregated across spokes)     |
| `ListSpokes`       | `GET /api/spokes`     | All registered spokes with health state                  |

Storage backends are currently Kubernetes-native CRs and not aggregated by the
hub.  `GET /api/storages` returns them from the Kubernetes API (or mock data in
development).

### Hub vs Spoke — what lives where

| Concept             | Hub                                      | Spoke                                    |
|---------------------|------------------------------------------|------------------------------------------|
| **Role**            | Central control plane                    | Per-cluster data plane                   |
| **CRD**             | `CaptureHub` (cluster-scoped)            | `TrafficCapture`, `CaptureStorage` (namespaced) |
| **Runs**            | Single cluster (management plane)        | Each workload cluster                    |
| **gRPC server**     | Port 9443, serves Query + Lifecycle APIs | N/A (client only)                        |
| **Heartbeat**       | Receives (90 s timeout)                  | Sends every 30 s                         |
| **Capture agents**  | N/A                                      | Deploys Deployment + Service + HPA       |
| **Mirror filters**  | N/A                                      | Injects into HTTPRoute/GRPCRoute         |
| **UI visibility**   | Global dashboard, all spokes             | Per-spoke capture list via hub queries   |

The UI presents a **single-pane-of-glass view** over the entire multi-cluster
deployment.  When you view the Spokes page you see every spoke the hub knows
about.  When you drill into a capture you see data aggregated from whichever
spoke is running that capture.

### Data flow

1. **Spoke registers** with hub via `RegisterSpoke` (sends cluster name,
   Kubernetes version, capabilities like storage backends).
2. **Spoke heartbeats** every 30 s via `Heartbeat`, including
   `CaptureStatusSummary` for each active capture (phase, request count, bytes
   written, agent replica counts).
3. **Hub stores** this in an in-memory registry, marks spokes as
   `DISCONNECTED` if heartbeat is stale > 90 s.
4. **UI polls** the hub's Query API via the Next.js API routes to render the
   dashboard.

### Mock mode (local development)

When the `HUB_GRPC_ADDRESS` env var is **not set**, the hub client
(`src/lib/hub-client.ts`) returns mock data that mirrors realistic captures,
spokes, and storage backends.  This lets you iterate on the UI without a running
Kubernetes cluster:

```bash
cd ui
npm run dev          # starts on http://localhost:3000 with mock data
```

To connect to a real hub:

```bash
HUB_GRPC_ADDRESS=hub.kapture-system.svc:9443 npm run dev
```

### Future: gRPC transport

The `hub-client.ts` module currently falls back to mock data.  The production
path will use `@grpc/grpc-js` to call the hub directly from the Next.js server.
The client will be configured with mTLS credentials loaded from Kubernetes
secrets (same mechanism the spoke uses).

## Project structure

```
ui/
├── src/
│   ├── app/
│   │   ├── api/                    # HTTP-to-gRPC bridge routes
│   │   │   ├── captures/
│   │   │   │   ├── route.ts        # GET /api/captures
│   │   │   │   └── [name]/
│   │   │   │       └── route.ts    # GET /api/captures/:name
│   │   │   ├── spokes/
│   │   │   │   └── route.ts        # GET /api/spokes
│   │   │   └── storages/
│   │   │       └── route.ts        # GET /api/storages
│   │   ├── captures/
│   │   │   ├── page.tsx            # Captures list
│   │   │   └── [id]/
│   │   │       └── page.tsx        # Capture detail
│   │   ├── spokes/
│   │   │   └── page.tsx            # Spokes overview
│   │   ├── storage/
│   │   │   └── page.tsx            # Storage backends
│   │   ├── layout.tsx              # Root layout with sidebar
│   │   ├── page.tsx                # Dashboard
│   │   └── globals.css
│   ├── components/
│   │   ├── sidebar.tsx             # Navigation sidebar
│   │   ├── stat-card.tsx           # Stat card component
│   │   └── status-badge.tsx        # Phase/health/storage badges
│   └── lib/
│       ├── hub-client.ts           # Hub gRPC client abstraction
│       ├── mock-data.ts            # Realistic mock data
│       └── types.ts                # TypeScript types (mirrors CRDs)
├── package.json
├── tsconfig.json
├── postcss.config.mjs
└── next.config.ts
```

## Development

```bash
npm install
npm run dev       # http://localhost:3000
npm run build     # production build
npm test          # run tests
```

## Testing

Tests use [Vitest](https://vitest.dev/) with React Testing Library.

```bash
npm test              # run all tests
npm run test:watch    # watch mode
```

Test coverage includes:
- **Hub client**: mock mode filtering, dashboard summary aggregation
- **API routes**: HTTP response shape, query parameter handling, 404 cases
- **Components**: status badge rendering, stat card rendering
