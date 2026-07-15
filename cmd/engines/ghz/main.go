// Command kapture-engine-ghz runs the ghz gRPC load tool as a Kapture
// replay engine plugin. Requires the ghz binary on PATH (or "binary" in
// the engine config). See internal/engines/ghz for the replay semantics —
// ghz replays the captured method mix, not byte-exact payload sequences.
package main

import (
	"fmt"
	"os"

	"github.com/kapture-io/kapture/internal/engines/ghz"
	"github.com/kapture-io/kapture/pkg/replayengine"
)

func main() {
	if err := replayengine.Serve(ghz.New()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
