// Command plugin-installer copies replay engine plugin binaries into a
// shared volume. It is the entrypoint contract for initContainer-based
// plugin delivery: run any image containing plugins at PLUGIN_SOURCE_DIR
// (default /plugins) with this binary as the command, mount the replay
// worker's plugin emptyDir at PLUGIN_TARGET_DIR (default /target), and the
// worker finds its engines without them being baked into its own image.
//
// Copies are atomic (temp file + rename), so a running worker watching the
// directory hot-reloads cleanly when a newer plugin image re-runs the
// installer.
package main

import (
	"fmt"
	"os"

	"github.com/kapture-io/kapture/internal/replayengine"
)

func main() {
	src := envOr("PLUGIN_SOURCE_DIR", "/plugins")
	dst := envOr("PLUGIN_TARGET_DIR", "/target")

	n, err := replayengine.CopyPlugins(src, dst)
	if err != nil {
		fmt.Fprintf(os.Stderr, "plugin install failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("installed %d plugin files from %s to %s\n", n, src, dst)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
