// Kapture replay feeder script for the k6 engine adapter.
//
// Generated/embedded by internal/engines/k6. A trimmed-down version of
// k6/replay.js: pulls captured requests from the adapter's local feed
// endpoint and sends them to the target. No thresholds — pass/fail is
// judged by the Kapture host from the summary export, and thresholds
// would turn k6's exit code into a false failure signal.
import http from 'k6/http';
import encoding from 'k6/encoding';

const FEED_URL = __ENV.FEED_URL || 'http://127.0.0.1:6565';
const TARGET_URL = __ENV.TARGET_URL || 'http://localhost:8080';
const BATCH_SIZE = parseInt(__ENV.BATCH_SIZE || '10', 10);

export const options = {
  scenarios: {
    replay: {
      executor: 'shared-iterations',
      iterations: 2147483647,
      vus: parseInt(__ENV.VUS || '10', 10),
      maxDuration: __ENV.MAX_DURATION || '168h',
    },
  },
  noConnectionReuse: false,
};

// Per-VU request queue.
let queue = [];
let feedDone = false;

function refill() {
  const res = http.get(`${FEED_URL}/batch?n=${BATCH_SIZE}`);
  if (res.status === 410) {
    feedDone = true;
    return;
  }
  if (res.status !== 200) {
    return;
  }
  const items = res.json();
  if (Array.isArray(items) && items.length > 0) {
    queue = queue.concat(items);
  } else {
    feedDone = true;
  }
}

export default function () {
  if (queue.length === 0) {
    if (feedDone) {
      // Feed exhausted: burn the remaining iterations quickly.
      return;
    }
    refill();
    if (queue.length === 0) {
      return;
    }
  }

  const item = queue.shift();
  const url = `${TARGET_URL}${item.path || '/'}`;
  const params = { headers: {} };
  if (item.headers) {
    for (const name in item.headers) {
      // Host is derived from the URL; k6 sets it itself.
      if (name.toLowerCase() === 'host') continue;
      const values = item.headers[name];
      params.headers[name] = Array.isArray(values) ? values.join(', ') : values;
    }
  }
  params.headers['X-Kapture-Replay'] = 'true';
  params.headers['X-Kapture-Request-ID'] = item.id || '';

  let body = null;
  if (item.body) {
    body = encoding.b64decode(item.body);
  }

  http.request(item.method || 'GET', url, body, params);
}
