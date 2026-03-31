package hub

import (
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"strings"
	"time"

	hubv1 "github.com/kapture-io/kapture/proto/hub/v1"
)

var dashboardTemplate = template.Must(template.New("dashboard").Funcs(template.FuncMap{
	"formatBytes": func(v int64) string {
		units := []string{"B", "KB", "MB", "GB", "TB"}
		value := float64(v)
		unit := units[0]
		for i := 0; i < len(units)-1 && value >= 1024; i++ {
			value /= 1024
			unit = units[i+1]
		}
		if unit == "B" {
			return fmt.Sprintf("%d %s", v, unit)
		}
		return fmt.Sprintf("%.1f %s", value, unit)
	},
	"formatTime": func(t time.Time) string {
		if t.IsZero() {
			return "never"
		}
		return t.Local().Format("Jan 2 15:04:05 MST")
	},
	"since": func(t time.Time) string {
		if t.IsZero() {
			return "never"
		}
		d := time.Since(t).Round(time.Second)
		if d < 0 {
			d = 0
		}
		return d.String() + " ago"
	},
	"phaseLabel": func(phase hubv1.CapturePhase) string {
		switch phase {
		case hubv1.CapturePhase_CAPTURE_PHASE_PENDING:
			return "Pending"
		case hubv1.CapturePhase_CAPTURE_PHASE_ACTIVE:
			return "Active"
		case hubv1.CapturePhase_CAPTURE_PHASE_COMPLETED:
			return "Completed"
		case hubv1.CapturePhase_CAPTURE_PHASE_FAILED:
			return "Failed"
		default:
			return "Unknown"
		}
	},
	"phaseClass": func(phase hubv1.CapturePhase) string {
		switch phase {
		case hubv1.CapturePhase_CAPTURE_PHASE_ACTIVE:
			return "active"
		case hubv1.CapturePhase_CAPTURE_PHASE_PENDING:
			return "pending"
		case hubv1.CapturePhase_CAPTURE_PHASE_FAILED:
			return "failed"
		default:
			return "neutral"
		}
	},
	"stateLabel": func(state hubv1.AgentState) string {
		switch state {
		case hubv1.AgentState_AGENT_STATE_CONNECTED:
			return "Connected"
		case hubv1.AgentState_AGENT_STATE_DISCONNECTED:
			return "Disconnected"
		default:
			return "Unknown"
		}
	},
	"stateClass": func(state hubv1.AgentState) string {
		switch state {
		case hubv1.AgentState_AGENT_STATE_CONNECTED:
			return "active"
		case hubv1.AgentState_AGENT_STATE_DISCONNECTED:
			return "failed"
		default:
			return "neutral"
		}
	},
	"join": strings.Join,
}).Parse(`<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta http-equiv="refresh" content="5">
  <title>Kapture Hub</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f5efe5;
      --panel: rgba(255, 252, 246, 0.88);
      --panel-strong: #fffaf2;
      --ink: #18212a;
      --muted: #6d7581;
      --border: rgba(24, 33, 42, 0.12);
      --accent: #b64d21;
      --accent-soft: rgba(182, 77, 33, 0.12);
      --good: #276749;
      --good-soft: rgba(39, 103, 73, 0.12);
      --warn: #975a16;
      --warn-soft: rgba(151, 90, 22, 0.12);
      --bad: #b83232;
      --bad-soft: rgba(184, 50, 50, 0.12);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "IBM Plex Sans", "Avenir Next", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top right, rgba(182, 77, 33, 0.14), transparent 34%),
        radial-gradient(circle at left center, rgba(24, 33, 42, 0.08), transparent 32%),
        linear-gradient(180deg, #f8f3ea 0%, var(--bg) 100%);
    }
    main {
      max-width: 1200px;
      margin: 0 auto;
      padding: 32px 20px 48px;
    }
    .hero {
      display: grid;
      gap: 20px;
      margin-bottom: 24px;
    }
    .hero-copy h1 {
      margin: 0;
      font-family: "IBM Plex Serif", Georgia, serif;
      font-size: clamp(2.2rem, 5vw, 3.6rem);
      line-height: 0.96;
      letter-spacing: -0.04em;
    }
    .hero-copy p {
      margin: 10px 0 0;
      color: var(--muted);
      max-width: 60ch;
      line-height: 1.5;
    }
    .summary {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 14px;
    }
    .card, .panel {
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 18px;
      backdrop-filter: blur(14px);
      box-shadow: 0 14px 34px rgba(24, 33, 42, 0.05);
    }
    .card {
      padding: 18px;
    }
    .eyebrow {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      font-size: 0.8rem;
      text-transform: uppercase;
      letter-spacing: 0.14em;
      color: var(--muted);
    }
    .metric {
      margin-top: 10px;
      font-size: 2rem;
      font-weight: 700;
      letter-spacing: -0.04em;
    }
    .layout {
      display: grid;
      gap: 18px;
    }
    .panel {
      overflow: hidden;
    }
    .panel-head {
      display: flex;
      justify-content: space-between;
      align-items: baseline;
      gap: 16px;
      padding: 18px 20px 0;
    }
    .panel-head h2 {
      margin: 0;
      font-size: 1.1rem;
      letter-spacing: -0.02em;
    }
    .panel-head p {
      margin: 0;
      color: var(--muted);
      font-size: 0.9rem;
    }
    table {
      width: 100%;
      border-collapse: collapse;
    }
    th, td {
      padding: 14px 20px;
      text-align: left;
      border-top: 1px solid var(--border);
      vertical-align: top;
    }
    th {
      font-size: 0.78rem;
      text-transform: uppercase;
      letter-spacing: 0.12em;
      color: var(--muted);
      font-weight: 600;
      background: rgba(255, 255, 255, 0.3);
    }
    td strong {
      display: block;
      font-size: 0.97rem;
    }
    td span {
      color: var(--muted);
      font-size: 0.88rem;
    }
    .badge {
      display: inline-flex;
      align-items: center;
      border-radius: 999px;
      padding: 6px 10px;
      font-size: 0.78rem;
      font-weight: 700;
      letter-spacing: 0.03em;
    }
    .badge.active { color: var(--good); background: var(--good-soft); }
    .badge.pending { color: var(--warn); background: var(--warn-soft); }
    .badge.failed { color: var(--bad); background: var(--bad-soft); }
    .badge.neutral { color: var(--ink); background: rgba(24, 33, 42, 0.08); }
    .empty {
      padding: 24px 20px 28px;
      color: var(--muted);
      line-height: 1.5;
    }
    .footer {
      margin-top: 16px;
      color: var(--muted);
      font-size: 0.88rem;
    }
    @media (max-width: 720px) {
      main { padding: 20px 14px 34px; }
      .panel { overflow-x: auto; }
      th, td { padding: 12px 14px; }
    }
  </style>
</head>
<body>
  <main>
    <section class="hero">
      <div class="hero-copy">
        <div class="eyebrow">Hub Dashboard</div>
        <h1>In-progress captures across your agents.</h1>
        <p>The hub refreshes every five seconds and surfaces the captures that are still pending or actively recording, alongside the health of each connected agent cluster.</p>
      </div>
      <div class="summary">
        <div class="card">
          <div class="eyebrow">Connected Agents</div>
          <div class="metric">{{ .ConnectedAgents }}</div>
        </div>
        <div class="card">
          <div class="eyebrow">In-progress Captures</div>
          <div class="metric">{{ .InProgressCount }}</div>
        </div>
        <div class="card">
          <div class="eyebrow">Known Agents</div>
          <div class="metric">{{ len .Agents }}</div>
        </div>
      </div>
    </section>

    <section class="layout">
      <div class="panel">
        <div class="panel-head">
          <div>
            <h2>In-progress captures</h2>
            <p>Pending and active captures only.</p>
          </div>
        </div>
        {{ if .InProgressCaptures }}
        <table>
          <thead>
            <tr>
              <th>Capture</th>
              <th>Agent</th>
              <th>Phase</th>
              <th>Replicas</th>
              <th>Requests</th>
              <th>Bytes</th>
            </tr>
          </thead>
          <tbody>
            {{ range .InProgressCaptures }}
            <tr>
              <td>
                <strong>{{ .Namespace }}/{{ .Name }}</strong>
                <span>Cluster {{ .ClusterName }}</span>
              </td>
              <td>
                <strong>{{ .AgentID }}</strong>
                <span>{{ .ClusterName }}</span>
              </td>
              <td><span class="badge {{ phaseClass .Phase }}">{{ phaseLabel .Phase }}</span></td>
              <td><strong>{{ .ReadyReplicas }}/{{ .DesiredReplicas }}</strong><span>ready / desired</span></td>
              <td>{{ .CapturedRequests }}</td>
              <td>{{ formatBytes .BytesWritten }}</td>
            </tr>
            {{ end }}
          </tbody>
        </table>
        {{ else }}
        <div class="empty">No captures are currently in progress.</div>
        {{ end }}
      </div>

      <div class="panel">
        <div class="panel-head">
          <div>
            <h2>Agents</h2>
            <p>Connection state and active capture load per cluster agent.</p>
          </div>
        </div>
        {{ if .Agents }}
        <table>
          <thead>
            <tr>
              <th>Cluster</th>
              <th>Agent ID</th>
              <th>State</th>
              <th>Active Captures</th>
              <th>Last Heartbeat</th>
              <th>Capabilities</th>
            </tr>
          </thead>
          <tbody>
            {{ range .Agents }}
            <tr>
              <td><strong>{{ .Name }}</strong></td>
              <td>{{ .ID }}</td>
              <td><span class="badge {{ stateClass .State }}">{{ stateLabel .State }}</span></td>
              <td>{{ .ActiveCaptures }}</td>
              <td>
                <strong>{{ formatTime .LastHeartbeat }}</strong>
                <span>{{ since .LastHeartbeat }}</span>
              </td>
              <td>{{ if .Capabilities }}{{ join .Capabilities ", " }}{{ else }}<span>none</span>{{ end }}</td>
            </tr>
            {{ end }}
          </tbody>
        </table>
        {{ else }}
        <div class="empty">No agents have registered with the hub yet.</div>
        {{ end }}
      </div>
    </section>

    <div class="footer">Last refresh: {{ formatTime .GeneratedAt }}</div>
  </main>
</body>
</html>`))

func newDashboardServer(server *Server) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if err := dashboardTemplate.Execute(w, server.DashboardSnapshot()); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
	mux.HandleFunc("/api/dashboard", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(server.DashboardSnapshot())
	})
	return &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
}
