#!/usr/bin/env bash
# Run cargo test and surface each failing test as a GitHub annotation, so
# failures are visible on the checks page without opening the raw log.
set -uo pipefail
log=$(mktemp)
cargo test --no-fail-fast "$@" 2>&1 | tee "$log"
status=${PIPESTATUS[0]}
if [[ $status -ne 0 ]]; then
  grep -E "^test .* \.\.\. FAILED$" "$log" | sed -E 's/^test (.*) \.\.\. FAILED$/::error title=test failed::\1/' || true
  grep -E "panicked at" -A3 "$log" | head -40 | while IFS= read -r line; do
    echo "::error title=panic::${line//%/%25}"
  done
fi
exit "$status"
