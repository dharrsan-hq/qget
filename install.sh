#!/bin/sh
# qget installer: downloads the latest (or a given) qget release for this
# machine, verifies its SHA-256 checksum, and installs it without sudo.
#
#   curl -fsSL https://github.com/dharrsan-hq/qget/releases/latest/download/install.sh | sh
#   curl -fsSL …/install.sh | sh -s -- --version v0.2.0 --dir /usr/local/bin
#
# Installs:  $DIR/qget                        (default DIR: ~/.local/bin)
#            ~/.local/share/man/man1/qget.1
#            shell completions under ~/.local/share
set -eu

REPO="dharrsan-hq/qget"
VERSION=""
DIR="${QGET_INSTALL_DIR:-$HOME/.local/bin}"

say() { printf 'qget-install: %s\n' "$*" >&2; }
die() { say "error: $*"; exit 1; }

while [ $# -gt 0 ]; do
  case "$1" in
    --version) VERSION="${2:?--version needs a value}"; shift 2 ;;
    --dir) DIR="${2:?--dir needs a value}"; shift 2 ;;
    -h|--help) sed -n '2,12p' "$0" 2>/dev/null || true; exit 0 ;;
    *) die "unknown option $1" ;;
  esac
done

need() { command -v "$1" >/dev/null 2>&1 || die "'$1' is required"; }
need uname; need tar; need mkdir
if command -v curl >/dev/null 2>&1; then
  fetch() { curl -fsSL "$1" -o "$2"; }
  # Where a URL finally redirects to.
  final_url() { curl -fsSLI -o /dev/null -w '%{url_effective}' "$1"; }
elif command -v wget >/dev/null 2>&1; then
  fetch() { wget -q "$1" -O "$2"; }
  final_url() { wget -q -S --max-redirect=0 -O /dev/null "$1" 2>&1 | sed -n 's/^ *[Ll]ocation: *//p' | tail -n 1 | tr -d '\r'; }
else
  die "curl or wget is required"
fi

case "$(uname -s)" in
  Linux) os=linux ;;
  Darwin) os=macos ;;
  *) die "unsupported OS $(uname -s) (qget supports Linux and macOS)" ;;
esac
case "$(uname -m)" in
  x86_64|amd64) arch=x86_64 ;;
  aarch64|arm64) arch=aarch64 ;;
  *) die "unsupported architecture $(uname -m)" ;;
esac

if [ -z "$VERSION" ]; then
  # github.com/<repo>/releases/latest redirects to …/releases/tag/<version>.
  # (Unlike the GitHub API, this has no per-IP rate limit.)
  latest=$(final_url "https://github.com/$REPO/releases/latest") || true
  VERSION="${latest##*/tag/}"
  case "$latest" in
    */tag/v*) ;;
    *) die "could not determine the latest release (got '${latest:-nothing}'); pass --version vX.Y.Z" ;;
  esac
fi

name="qget-$VERSION-$os-$arch"
base="https://github.com/$REPO/releases/download/$VERSION"
tmp=$(mktemp -d 2>/dev/null || mktemp -d -t qget)
trap 'rm -rf "$tmp"' EXIT INT TERM

say "downloading $name.tar.gz"
fetch "$base/$name.tar.gz" "$tmp/$name.tar.gz" || die "download failed (does $VERSION have a $os-$arch build?)"
fetch "$base/$name.tar.gz.sha256" "$tmp/$name.tar.gz.sha256" || die "checksum download failed"

expected=$(cut -d' ' -f1 < "$tmp/$name.tar.gz.sha256")
if command -v sha256sum >/dev/null 2>&1; then
  actual=$(sha256sum "$tmp/$name.tar.gz" | cut -d' ' -f1)
elif command -v shasum >/dev/null 2>&1; then
  actual=$(shasum -a 256 "$tmp/$name.tar.gz" | cut -d' ' -f1)
else
  die "sha256sum or shasum is required to verify the download"
fi
[ "$expected" = "$actual" ] || die "checksum mismatch: expected $expected, got $actual"
say "checksum verified"

tar -xzf "$tmp/$name.tar.gz" -C "$tmp"
src="$tmp/$name"
mkdir -p "$DIR"
cp "$src/qget" "$DIR/qget.new" && chmod 0755 "$DIR/qget.new" && mv "$DIR/qget.new" "$DIR/qget"

share="${XDG_DATA_HOME:-$HOME/.local/share}"
mkdir -p "$share/man/man1" "$share/bash-completion/completions" \
  "$share/zsh/site-functions" "$share/fish/vendor_completions.d"
cp "$src/qget.1" "$share/man/man1/qget.1"
cp "$src/qget.bash" "$share/bash-completion/completions/qget"
cp "$src/_qget" "$share/zsh/site-functions/_qget"
cp "$src/qget.fish" "$share/fish/vendor_completions.d/qget.fish"

say "installed qget $VERSION to $DIR/qget"
case ":$PATH:" in
  *":$DIR:"*) ;;
  *) say "note: $DIR is not on your PATH; add:  export PATH=\"$DIR:\$PATH\"" ;;
esac
say "zsh completions: add  fpath=(\"$share/zsh/site-functions\" \$fpath)  before compinit if needed"
"$DIR/qget" --version >/dev/null 2>&1 || say "warning: the installed binary did not run on this system"
