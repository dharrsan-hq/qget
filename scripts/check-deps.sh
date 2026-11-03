#!/usr/bin/env bash
# Architecture rules enforced in CI (PRD §9.2):
#   LIB-02  qget-core depends only on pure-computation crates (allow-list).
#   LIB-08  the default qget build does not depend on iroh.
set -euo pipefail
cd "$(dirname "$0")/.."

allowed=(
  arrayvec blake3 block-buffer bytes cfg-if constant_time_eq cpufeatures crc32c
  crypto-common digest generic-array hex proc-macro2 qget-core quote serde
  serde_core serde_derive sha2 syn thiserror thiserror-impl typenum unicode-ident
  # build-time helpers (blake3, crc32c, generic-array build scripts)
  cc find-msvc-tools shlex rustc_version semver version_check
)

fail=0
while read -r dep; do
  [[ -z "$dep" ]] && continue
  if [[ ! " ${allowed[*]} " =~ " ${dep} " ]]; then
    echo "LIB-02 violation: qget-core depends on '$dep' (add it to the allow-list only if it performs no I/O)"
    fail=1
  fi
done < <(cargo tree -p qget-core -e normal,build --prefix none --format '{p}' | awk '{print $1}' | sort -u)

if cargo tree -p qget-cli -e normal --prefix none --format '{p}' | grep -q '^iroh '; then
  echo "LIB-08 violation: the default qget-cli build depends on iroh"
  fail=1
fi

if [[ $fail -eq 0 ]]; then
  echo "dependency rules OK"
fi
exit $fail
