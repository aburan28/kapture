#!/usr/bin/env bash
# Runs TLC on the TLA+ specs in verification/tla/.
#
# Requires java (17+). Downloads tla2tools.jar on first use.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TLA_DIR="${REPO_ROOT}/verification/tla"
TOOLS_DIR="${REPO_ROOT}/.cache/tla"
TLA_TOOLS_JAR="${TLA_TOOLS_JAR:-${TOOLS_DIR}/tla2tools.jar}"
TLA_TOOLS_VERSION="${TLA_TOOLS_VERSION:-1.8.0}"
TLA_TOOLS_URL="https://github.com/tlaplus/tlaplus/releases/download/v${TLA_TOOLS_VERSION}/tla2tools.jar"

if ! command -v java >/dev/null 2>&1; then
    echo "error: java is required to run TLC" >&2
    exit 1
fi

if [ ! -f "${TLA_TOOLS_JAR}" ]; then
    echo "downloading tla2tools.jar ${TLA_TOOLS_VERSION}..."
    mkdir -p "$(dirname "${TLA_TOOLS_JAR}")"
    curl -fsSL -o "${TLA_TOOLS_JAR}" "${TLA_TOOLS_URL}"
fi

run_tlc() {
    local spec="$1"
    echo ""
    echo "=== TLC: ${spec} ==="
    # -deadlock: terminal protocol states intentionally have no successor.
    # TLC exits non-zero on invariant or property violations.
    java -XX:+UseParallelGC -cp "${TLA_TOOLS_JAR}" tlc2.TLC \
        -workers auto \
        -deadlock \
        -config "${TLA_DIR}/${spec}.cfg" \
        "${TLA_DIR}/${spec}.tla"
}

run_tlc Sharding
run_tlc KaptureLoadTest

echo ""
echo "All TLA+ models checked successfully."
