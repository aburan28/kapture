// k6 Replay Script for Kapture
//
// This script consumes captured traffic from the Kapture replay feed server
// and replays it against the target service at the originally recorded QPS.
//
// Usage:
//   # Start the feed server (handles storage reading + timing):
//   replay-engine --storage-type s3 --bucket my-captures \
//     --capture-id prod/api --rate-mode original --serve --addr :6565
//
//   # Run k6 against the target (feed server controls pacing):
//   k6 run --vus 50 --duration 0 k6/replay.js \
//     -e FEED_URL=http://localhost:6565 \
//     -e TARGET_URL=http://staging:8080
//
// The feed server paces requests at the original recorded rate. k6 VUs
// simply fetch the next request from the feed and send it, achieving
// "as recorded" QPS naturally. For large captures (100GB+), the feed
// server streams from storage — k6 never needs the full dataset in memory.
//
// Environment Variables:
//   FEED_URL    - URL of the replay feed server (default: http://localhost:6565)
//   TARGET_URL  - Target service base URL (default: http://localhost:8080)
//   BATCH_SIZE  - Requests per batch from feed (default: 10)
//   ACK_EVERY   - Acknowledge every N requests for checkpointing (default: 100)

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';
import encoding from 'k6/encoding';

// --- Configuration ---
const FEED_URL = __ENV.FEED_URL || 'http://localhost:6565';
const TARGET_URL = __ENV.TARGET_URL || 'http://localhost:8080';
const BATCH_SIZE = parseInt(__ENV.BATCH_SIZE || '10', 10);
const ACK_EVERY = parseInt(__ENV.ACK_EVERY || '100', 10);

// --- Custom Metrics ---
const replayedRequests = new Counter('kapture_replayed_requests');
const replayErrors = new Counter('kapture_replay_errors');
const replaySuccessRate = new Rate('kapture_replay_success_rate');
const replayLatency = new Trend('kapture_replay_latency', true);
const feedFetchLatency = new Trend('kapture_feed_fetch_latency', true);

// --- k6 Options ---
// Duration is 0 (infinite) — the test ends when the feed server signals
// replay complete (HTTP 410 Gone). Override VUs based on concurrency needs.
export const options = {
  scenarios: {
    replay: {
      executor: 'shared-iterations',
      // Use a large number; the test terminates when the feed is exhausted.
      iterations: 2147483647,
      vus: parseInt(__ENV.VUS || '50', 10),
      maxDuration: __ENV.MAX_DURATION || '168h', // 7 days default
    },
  },
  thresholds: {
    'kapture_replay_success_rate': ['rate>0.95'],
    'kapture_replay_latency': ['p(95)<5000'],
  },
  noConnectionReuse: false,
};

// Per-VU state for batched acknowledgment.
let vuAckCounter = 0;

// Fetch a batch of captured requests from the feed server.
function fetchBatch() {
  const start = Date.now();
  const res = http.get(`${FEED_URL}/batch?n=${BATCH_SIZE}`, {
    tags: { name: 'feed_fetch' },
    timeout: '60s',
  });
  feedFetchLatency.add(Date.now() - start);

  if (res.status === 410) {
    // Replay complete.
    return null;
  }

  if (res.status !== 200) {
    console.error(`Feed error: ${res.status} ${res.body}`);
    sleep(1);
    return [];
  }

  try {
    return JSON.parse(res.body);
  } catch (e) {
    console.error(`Failed to parse feed response: ${e}`);
    return [];
  }
}

// Replay a single captured request against the target.
function replayRequest(captured) {
  const url = `${TARGET_URL}${captured.path}`;
  const method = (captured.method || 'GET').toUpperCase();

  // Build headers from captured data.
  const headers = {};
  if (captured.headers) {
    for (const [key, values] of Object.entries(captured.headers)) {
      // Skip hop-by-hop headers that shouldn't be forwarded.
      const lower = key.toLowerCase();
      if (lower === 'host' || lower === 'transfer-encoding' || lower === 'connection') {
        continue;
      }
      headers[key] = Array.isArray(values) ? values[0] : values;
    }
  }

  // Add replay identification headers.
  headers['X-Kapture-Replay'] = 'true';
  headers['X-Kapture-Request-ID'] = captured.id || '';

  const params = {
    headers: headers,
    tags: {
      name: 'replay',
      method: method,
      path: captured.path,
    },
    timeout: '30s',
  };

  let body = null;
  if (captured.body && method !== 'GET' && method !== 'HEAD') {
    // Go's json.Marshal encodes []byte as base64. Decode to get original bytes.
    body = encoding.b64decode(captured.body, 'std', 's');
  }

  let res;
  switch (method) {
    case 'GET':
      res = http.get(url, params);
      break;
    case 'POST':
      res = http.post(url, body, params);
      break;
    case 'PUT':
      res = http.put(url, body, params);
      break;
    case 'PATCH':
      res = http.patch(url, body, params);
      break;
    case 'DELETE':
      res = http.del(url, body, params);
      break;
    case 'OPTIONS':
      res = http.options(url, body, params);
      break;
    default:
      res = http.request(method, url, body, params);
  }

  return res;
}

// Acknowledge processed requests to the feed server for checkpointing.
function acknowledgeRequests(count) {
  http.post(`${FEED_URL}/ack`, JSON.stringify({ count: count }), {
    headers: { 'Content-Type': 'application/json' },
    tags: { name: 'feed_ack' },
  });
}

// --- Main VU Code ---
export default function () {
  const batch = fetchBatch();

  // Feed exhausted — signal k6 to stop.
  if (batch === null) {
    // Flush any remaining un-acked requests before stopping.
    if (vuAckCounter > 0) {
      acknowledgeRequests(vuAckCounter);
      vuAckCounter = 0;
    }
    sleep(2);
    return;
  }

  if (batch.length === 0) {
    return;
  }

  for (const captured of batch) {
    const res = replayRequest(captured);

    replayedRequests.add(1);
    replayLatency.add(res.timings.duration);

    const success = res.status >= 200 && res.status < 500;
    replaySuccessRate.add(success);

    if (!success) {
      replayErrors.add(1);
    }

    check(res, {
      'status is not 5xx': (r) => r.status < 500,
    });

    // Periodic acknowledgment for checkpointing.
    vuAckCounter++;
    if (vuAckCounter >= ACK_EVERY) {
      acknowledgeRequests(vuAckCounter);
      vuAckCounter = 0;
    }
  }
}

// Final teardown hook: log replay completion.
export function teardown() {
  console.log('Replay session complete');
}

// --- Setup: Print configuration ---
export function setup() {
  console.log(`Kapture k6 Replay Engine`);
  console.log(`  Feed URL:   ${FEED_URL}`);
  console.log(`  Target URL: ${TARGET_URL}`);
  console.log(`  Batch Size: ${BATCH_SIZE}`);
  console.log(`  VUs:        ${options.scenarios.replay.vus}`);

  // Check feed server connectivity.
  const statusRes = http.get(`${FEED_URL}/status`, { timeout: '5s' });
  if (statusRes.status !== 200) {
    throw new Error(`Feed server not reachable at ${FEED_URL}: ${statusRes.status}`);
  }

  const status = JSON.parse(statusRes.body);
  console.log(`  Feed Status: buffer=${status.bufferLen}/${status.bufferCap}`);

  return { startTime: Date.now() };
}
