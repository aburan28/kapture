// Command kapture-engine-builtin is the builtin HTTP replay engine as a
// standalone plugin binary. The same implementation runs in-process inside
// replay-engine by default; this binary exists so the builtin engine can
// also be exercised through the subprocess ABI (conformance) and swapped
// like any other plugin.
package main

import (
	"fmt"
	"os"

	"github.com/kapture-io/kapture/internal/engines/builtin"
	"github.com/kapture-io/kapture/pkg/replayengine"
)

func main() {
	if err := replayengine.Serve(builtin.New()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
