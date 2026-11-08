#!/usr/bin/env bash
# Render the landing page into OUT_DIR (default _site/), filling in the
# release version and signing-key fingerprint.
#   scripts/build-site.sh 0.3.0 [OUT_DIR]
set -euo pipefail
cd "$(dirname "$0")/.."
version="${1:?usage: build-site.sh <version> [out-dir]}"
out="${2:-_site}"
key=packaging/qget-signing-key.asc

fp=$(gpg --show-keys --with-colons "$key" 2>/dev/null | awk -F: '/^fpr/{print $10; exit}')
[[ ${#fp} -eq 40 ]] || { echo "could not read fingerprint from $key" >&2; exit 1; }
spaced=$(echo "$fp" | sed -E 's/(.{4})/\1 /g; s/ $//; s/^(.{24}) /\1  /')

mkdir -p "$out"
sed -e "s/{{VERSION}}/$version/g" \
    -e "s/{{FINGERPRINT_SPACED}}/$spaced/g" \
    -e "s/{{FINGERPRINT}}/$fp/g" \
    site/index.html > "$out/index.html"
cp "$key" "$out/key.asc"
cp install.sh "$out/install.sh"
cp site/author.jpg "$out/author.jpg"
cp site/tip-qr.svg "$out/tip-qr.svg"
if grep -q '{{' "$out/index.html"; then
  echo "unfilled placeholder in index.html" >&2
  exit 1
fi
echo "site → $out (v$version, key $fp)"
