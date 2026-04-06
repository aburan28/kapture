// k6 Stress/Soak Replay Script for Kapture
//
// Variant of replay.js designed for long-running soak tests and stress testing.
// Uses ramping VUs to gradually increase load and includes more detailed
// metrics for capacity planning.
//
// Usage:
//   k6 run k6/replay-stress.js \
//     -e FEED_URL=http://localhost:6565 \
//     -e TARGET_URL=http://staging:8080 \
//     -e MAX_VUS=200

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend, Gauge } from 'k6/metrics';
import encoding from 'k6/encoding';

const FEED_URL = __ENV.FEED_URL || 'http://localhost:6565';
const TARGET_URL = __ENV.TARGET_URL || 'http://localhost:8080';
const BATCH_SIZE = parseInt(__ENV.BATCH_SIZE || '20', 10);
const MAX_VUS = parseInt(__ENV.MAX_VUS || '100', 10);

// Custom metrics.
const replayedRequests = new Counter('kapture_replayed_requests');
const replayErrors = new Counter('kapture_replay_errors');
const replaySuccessRate = new Rate('kapture_replay_success_rate');
const replayLatency = new Trend('kapture_replay_latency', true);
const feedFetchLatency = new Trend('kapture_feed_fetch_latency', true);
const feedBufferGauge = new Gauge('kapture_feed_buffer_size');
const currentRpsGauge = new Gauge('kapture_current_rps');
const statusCodeDist = {};

// Ramping stages for soak/stress testing.
export const options = {
  scenarios: {
    soak_replay: {
      executor: 'ramping-vus',
      startVUs: Math.max(1, Math.floor(MAX_VUS / 10)),
      stages: [
        { duration: '5m', target: Math.floor(MAX_VUS / 4) },   // Warm up
        { duration: '10m', target: Math.floor(MAX_VUS / 2) },  // Ramp to 50%
        { duration: __ENV.SOAK_DURATION || '24h', target: MAX_VUS },  // Soak at max
        { duration: '5m', target: 0 },                          // Cool down
      ],
      gracefulRampDown: '2m',
    },
  },
  thresholds: {
    'kapture_replay_success_rate': ['rate>0.90'],
    'kapture_replay_latency': ['p(95)<10000', 'p(99)<30000'],
    'http_req_failed': ['rate<0.10'],
  },
};

let vuAckCounter = 0;

function fetchBatch() {
  const start = Date.now();
  const res = http.get(`${FEED_URL}/batch?n=${BATCH_SIZE}`, {
    tags: { name: 'feed_fetch' },
    timeout: '60s',
  });
  feedFetchLatency.add(Date.now() - start);

  if (res.status === 410) {
    return null;
  }
  if (res.status !== 200) {
    sleep(1);
    return [];
  }

  try {
    return JSON.parse(res.body);
  } catch (e) {
    return [];
  }
}

function replayRequest(captured) {
  const url = `${TARGET_URL}${captured.path}`;
  const method = (captured.method || 'GET').toUpperCase();

  const headers = {};
  if (captured.headers) {
    for (const [key, values] of Object.entries(captured.headers)) {
      const lower = key.toLowerCase();
      if (lower === 'host' || lower === 'transfer-encoding' || lower === 'connection') {
        continue;
      }
      headers[key] = Array.isArray(values) ? values[0] : values;
    }
  }

  headers['X-Kapture-Replay'] = 'true';
  headers['X-Kapture-Request-ID'] = captured.id || '';
  headers['X-Kapture-Timestamp'] = captured.timestamp || '';

  const params = {
    headers: headers,
    tags: { name: 'replay', method: method },
    timeout: '30s',
  };

  let body = null;
  if (captured.body && method !== 'GET' && method !== 'HEAD') {
    // Go's json.Marshal encodes []byte as base64. Decode to get original bytes.
    body = encoding.b64decode(captured.body, 'std', 's');
  }

  return http.request(method, url, body, params);
}

// Periodically poll feed status for monitoring.
function pollFeedStatus() {
  try {
    const res = http.get(`${FEED_URL}/status`, {
      tags: { name: 'feed_status' },
      timeout: '5s',
    });
    if (res.status === 200) {
      const status = JSON.parse(res.body);
      feedBufferGauge.add(status.bufferLen);
      currentRpsGauge.add(status.currentRps);
    }
  } catch (e) {
    // Ignore status poll errors.
  }
}

export default function () {
  // Poll feed status occasionally (1 in 50 iterations).
  if (Math.random() < 0.02) {
    pollFeedStatus();
  }

  const batch = fetchBatch();

  if (batch === null) {
    sleep(5);
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
      'no server errors': (r) => r.status < 500,
      'response time < 5s': (r) => r.timings.duration < 5000,
    });

    vuAckCounter++;
    if (vuAckCounter >= 100) {
      http.post(`${FEED_URL}/ack`, JSON.stringify({ count: vuAckCounter }), {
        headers: { 'Content-Type': 'application/json' },
        tags: { name: 'feed_ack' },
      });
      vuAckCounter = 0;
    }
  }
}

export function setup() {
  console.log(`Kapture k6 Soak/Stress Replay`);
  console.log(`  Feed URL:   ${FEED_URL}`);
  console.log(`  Target URL: ${TARGET_URL}`);
  console.log(`  Max VUs:    ${MAX_VUS}`);
  console.log(`  Batch Size: ${BATCH_SIZE}`);

  const statusRes = http.get(`${FEED_URL}/status`, { timeout: '5s' });
  if (statusRes.status !== 200) {
    throw new Error(`Feed server not reachable at ${FEED_URL}`);
  }

  return { startTime: Date.now() };
}

export function teardown(data) {
  const elapsed = (Date.now() - data.startTime) / 1000;
  console.log(`Soak test completed after ${elapsed.toFixed(0)}s`);
}
