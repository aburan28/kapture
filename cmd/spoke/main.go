package main

import (
	"context"
	"crypto/tls"
	"flag"
	"os"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	gwapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	capturev1alpha1 "github.com/kapture-io/kapture/api/v1alpha1"
	"github.com/kapture-io/kapture/internal/spoke"
	hubv1 "github.com/kapture-io/kapture/proto/hub/v1"
)

var scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(capturev1alpha1.AddToScheme(scheme))
	utilruntime.Must(gwapiv1.Install(scheme))
}

func main() {
	var (
		metricsAddr     string
		healthProbeAddr string
		hubAddress      string
		spokeName       string
		clusterID       string
		cell            string
		leaderElect     bool
	)

	flag.StringVar(&metricsAddr, "metrics-addr", ":8080", "The address the metrics endpoint binds to.")
	flag.StringVar(&healthProbeAddr, "health-probe-addr", ":8081", "The address the health probe endpoint binds to.")
	flag.StringVar(&hubAddress, "hub-address", envOr("HUB_ADDRESS", ""), "The gRPC address of the hub (optional, spoke works standalone if empty).")
	flag.StringVar(&spokeName, "spoke-name", envOr("SPOKE_NAME", ""), "The name of this spoke cluster (defaults to hostname).")
	flag.StringVar(&clusterID, "cluster-id", envOr("CLUSTER_ID", ""), "The cluster identifier for this spoke.")
	flag.StringVar(&cell, "cell", envOr("CELL_NAME", ""), "The deployment cell this spoke registers under (optional).")
	flag.BoolVar(&leaderElect, "leader-elect", false, "Enable leader election for controller manager.")

	tlsFiles := spoke.HubTLSFiles{}
	flag.StringVar(&tlsFiles.CertFile, "hub-tls-cert", envOr("HUB_TLS_CERT_FILE", ""), "Client certificate for hub mTLS (PEM).")
	flag.StringVar(&tlsFiles.KeyFile, "hub-tls-key", envOr("HUB_TLS_KEY_FILE", ""), "Client key for hub mTLS (PEM).")
	flag.StringVar(&tlsFiles.CAFile, "hub-tls-ca", envOr("HUB_TLS_CA_FILE", ""), "CA bundle that signed the hub server certificate (PEM).")
	flag.StringVar(&tlsFiles.ServerName, "hub-tls-server-name", envOr("HUB_TLS_SERVER_NAME", ""), "Expected hub server certificate name (defaults to the dial host).")
	opts := zap.Options{Development: true}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	if agentImage := envOr("AGENT_IMAGE", ""); agentImage != "" {
		spoke.CaptureAgentImage = agentImage
	}
	if replayImage := envOr("REPLAY_ENGINE_IMAGE", ""); replayImage != "" {
		spoke.ReplayEngineImage = replayImage
	}

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	setupLog := ctrl.Log.WithName("setup")

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		HealthProbeBindAddress: healthProbeAddr,
		LeaderElection:         leaderElect,
		LeaderElectionID:       "spoke.capture.gateway.io",
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	// Set up hub client (optional)
	var hubClient *spoke.HubClient
	if hubAddress != "" {
		if spokeName == "" {
			hostname, _ := os.Hostname()
			spokeName = hostname
		}
		if clusterID == "" {
			clusterID = spokeName
		}

		var hubTLS *tls.Config
		if tlsFiles.Configured() {
			var err error
			hubTLS, err = spoke.BuildHubTLSConfig(tlsFiles)
			if err != nil {
				setupLog.Error(err, "invalid hub TLS configuration")
				os.Exit(1)
			}
			setupLog.Info("hub connection will use TLS",
				"clientCert", tlsFiles.CertFile != "", "ca", tlsFiles.CAFile != "")
		}

		hubClient = spoke.NewHubClient(spoke.HubClientConfig{
			HubAddress: hubAddress,
			SpokeName:  spokeName,
			ClusterID:  clusterID,
			Cell:       cell,
			TLSConfig:  hubTLS,
			Logger:     setupLog,
		})

		replayDirectives := &spoke.ReplayDirectiveHandler{
			Client: mgr.GetClient(),
			Log:    ctrl.Log.WithName("replay-directives"),
		}

		hubClient.OnDirective = func(resp *hubv1.WatchDirectivesResponse) {
			if d := resp.GetDirective(); d != nil {
				setupLog.Info("received capture directive from hub",
					"action", d.Action,
					"capture", d.CaptureName,
					"namespace", d.CaptureNamespace,
				)
			}
			if rd := resp.GetReplayDirective(); rd != nil {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				if err := replayDirectives.Handle(ctx, rd); err != nil {
					setupLog.Error(err, "failed to apply replay directive",
						"action", rd.Action,
						"loadTest", rd.LoadTestName,
						"namespace", rd.LoadTestNamespace,
					)
				}
			}
		}

		// Garbage-collect shards whose CaptureLoadTest no longer exists
		// on the hub — the backstop for STOP directives lost in a hub
		// crash window (see verification/tla/KaptureLoadTest.tla).
		orphanGC := &spoke.OrphanShardGC{
			Client: mgr.GetClient(),
			Log:    ctrl.Log.WithName("orphan-gc"),
		}
		hubClient.OnActiveLoadTests = func(ctx context.Context, keys []*hubv1.LoadTestKey) {
			orphanGC.Collect(ctx, keys)
		}

		// Include replay shard statuses in heartbeats so the hub's load
		// test view survives lost status reports.
		hubClient.ReplaySummaries = func(ctx context.Context) []*hubv1.ReplayStatusSummary {
			var replays capturev1alpha1.TrafficReplayList
			if err := mgr.GetClient().List(ctx, &replays); err != nil {
				// The manager cache may not be started yet; skip this beat.
				return nil
			}
			var summaries []*hubv1.ReplayStatusSummary
			for i := range replays.Items {
				if summary := spoke.ReplaySummaryFromStatus(&replays.Items[i]); summary != nil {
					summaries = append(summaries, summary)
				}
			}
			return summaries
		}

		ctx := context.Background()
		if err := hubClient.Connect(ctx); err != nil {
			setupLog.Error(err, "failed to connect to hub, continuing in standalone mode")
			hubClient = nil
		} else {
			heartbeatSec, err := hubClient.Register(ctx)
			if err != nil {
				setupLog.Error(err, "failed to register with hub, continuing in standalone mode")
				hubClient.Close()
				hubClient = nil
			} else {
				interval := time.Duration(heartbeatSec) * time.Second
				if interval <= 0 {
					interval = 30 * time.Second
				}
				hubClient.StartHeartbeat(ctx, interval)
				hubClient.WatchDirectives(ctx)
				setupLog.Info("hub connection established", "heartbeatInterval", interval)
			}
		}
	} else {
		setupLog.Info("no hub address configured, running in standalone mode")
	}

	if err := (&spoke.TrafficCaptureReconciler{
		Client:    mgr.GetClient(),
		Scheme:    mgr.GetScheme(),
		HubClient: hubClient,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "TrafficCapture")
		os.Exit(1)
	}

	if err := (&spoke.CaptureStorageReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "CaptureStorage")
		os.Exit(1)
	}

	if err := (&spoke.TrafficReplayReconciler{
		Client:    mgr.GetClient(),
		Scheme:    mgr.GetScheme(),
		HubClient: hubClient,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "TrafficReplay")
		os.Exit(1)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("starting spoke controller manager",
		"hub-address", hubAddress,
		"spoke-name", spokeName,
	)
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}

	// Graceful shutdown: deregister from hub
	if hubClient != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := hubClient.Deregister(shutdownCtx); err != nil {
			setupLog.Error(err, "failed to deregister from hub")
		}
		if err := hubClient.Close(); err != nil {
			setupLog.Error(err, "failed to close hub client")
		}
	}
}

func envOr(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}
