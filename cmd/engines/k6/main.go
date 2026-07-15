// Command kapture-engine-k6 runs Grafana k6 as a Kapture replay engine
// plugin. Requires the k6 binary on PATH (or "binary" in the engine
// config).
package main

import (
	"fmt"
	"os"

	"github.com/kapture-io/kapture/internal/engines/k6"
	"github.com/kapture-io/kapture/pkg/replayengine"
)

func main() {
	if err := replayengine.Serve(k6.New()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
