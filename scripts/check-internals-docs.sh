#!/usr/bin/env bash
# Keep docs/internals true to the code (docs/internals/README.md, "Keeping
# these documents true").
#
#   scripts/check-internals-docs.sh [BASE [HEAD]]
#
# Every docs/internals/*.md starts with `<!-- covers: path path dir/ -->`.
# - Covered paths must exist (catches renames and deletions).
# - With BASE: if BASE..HEAD changes a covered path but not the document that
#   covers it, fail. A commit trailer `Internals-docs: unchanged` in the range
#   says the documents were checked and are still correct.
# - Source files no document covers are listed as warnings.
set -euo pipefail
cd "$(dirname "$0")/.."

base="${1:-}"
head="${2:-HEAD}"
docs_dir=docs/internals
fail=0

note() { # level message [file]
  if [[ -n "${GITHUB_ACTIONS:-}" ]]; then
    echo "::$1${3:+ file=$3}::$2"
  else
    echo "$1: $2"
  fi
}

# doc → its covered paths, one "doc<TAB>path" line each.
covers=$(
  for doc in "$docs_dir"/*.md; do
    sed -n 's/^<!-- covers: \(.*\) -->$/\1/p' "$doc" | tr ' ' '\n' |
      awk -v d="$doc" 'NF { print d "\t" $0 }'
  done
)

# Covers a file path? Entries ending in / cover a whole directory.
covered_by() { # path → documents covering it
  awk -F'\t' -v p="$1" '
    ($2 ~ /\/$/ && index(p, $2) == 1) || $2 == p { print $1 }' <<<"$covers"
}

# 1. Every covered path exists.
while IFS=$'\t' read -r doc path; do
  [[ -z "$doc" ]] && continue
  if [[ ! -e "$path" ]]; then
    note error "$doc covers '$path', which does not exist (renamed or deleted?)" "$doc"
    fail=1
  fi
done <<<"$covers"

# 2. Source files no document explains.
while read -r src; do
  [[ -z "$(covered_by "$src")" ]] && note warning "no docs/internals document covers $src" "$src"
done < <(git ls-files 'crates/*/src/*.rs' 'crates/*/src/**/*.rs' | grep -v '/tests\.rs$' | sort -u)

# 3. Changed code must come with its document.
if [[ -n "$base" && ! "$base" =~ ^0+$ ]] && git rev-parse -q --verify "$base^{commit}" >/dev/null; then
  changed=$(git diff --name-only "$base" "$head")
  if git log --format=%B "$base..$head" | grep -qi '^Internals-docs: *unchanged'; then
    echo "check-internals-docs: 'Internals-docs: unchanged' trailer found; skipping the change check"
  else
    stale=$(
      while read -r f; do
        [[ -z "$f" || "$f" == "$docs_dir"/* ]] && continue
        for doc in $(covered_by "$f"); do
          grep -qxF "$doc" <<<"$changed" || printf '%s\t%s\n' "$doc" "$f"
        done
      done <<<"$changed" | sort -u
    )
    if [[ -n "$stale" ]]; then
      while IFS=$'\t' read -r doc f; do
        note error "$f changed but $doc did not; update it, or add the commit trailer 'Internals-docs: unchanged' if it is still correct" "$doc"
      done <<<"$stale"
      fail=1
    fi
  fi
fi

if [[ $fail -eq 0 ]]; then
  echo "internals docs OK"
fi
exit $fail
