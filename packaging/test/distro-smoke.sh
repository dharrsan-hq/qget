#!/bin/sh
# Install a qget package inside a distro container, use it, uninstall it.
#   docker run --rm -v "$PWD/pkgs:/pkgs:ro" -v "$PWD/packaging/test:/t:ro" \
#     debian:12 sh /t/distro-smoke.sh deb
# The package must pull in its own dependencies (glibc, CA certificates).
set -eu
kind="$1"
cd /pkgs

case "$kind" in
  deb)
    export DEBIAN_FRONTEND=noninteractive
    apt-get update -qq
    apt-get install -y -qq ./qget_*_"$(dpkg --print-architecture)".deb
    remove() { apt-get remove -y -qq qget; }
    files() { dpkg -L qget; }
    ;;
  rpm)
    dnf install -y -q ./qget-*."$(uname -m)".rpm
    remove() { dnf remove -y -q qget; }
    files() { rpm -ql qget; }
    ;;
  arch)
    pacman -Sy --noconfirm --needed archlinux-keyring >/dev/null
    pacman -U --noconfirm ./qget-*-"$(uname -m)".pkg.tar.zst
    remove() { pacman -R --noconfirm qget; }
    files() { pacman -Qlq qget; }
    ;;
  *) echo "usage: distro-smoke.sh deb|rpm|arch" >&2; exit 2 ;;
esac

echo "--- installed: $(qget --version)"
test -x /usr/bin/qget
# Check what the package installs via its own file list: minimal container
# images (Ubuntu, Arch) skip extracting man pages and docs on purpose.
for f in /usr/share/man/man1/qget.1.gz /usr/share/bash-completion/completions/qget \
         /usr/share/fish/vendor_completions.d/qget.fish; do
  files | grep -qx "$f" || { echo "package does not ship $f" >&2; exit 1; }
done
files | grep -q '/usr/share/zsh/.*/_qget$' || { echo "package does not ship zsh completion" >&2; exit 1; }

# Real HTTPS download: proves TLS works with the distro's CA store.
qget -q -O /tmp/sums https://github.com/dharrsan-hq/qget/releases/download/v0.1.1/SHA256SUMS
grep -q "linux-x86_64" /tmp/sums
echo "--- https download ok"

# Stream-extract over HTTPS: a release archive extracted while downloading.
qget -q -x /tmp/x \
  --checksum "sha256:$(grep ' qget-v0.1.1-linux-x86_64.tar.gz$' /tmp/sums | cut -d' ' -f1)" \
  https://github.com/dharrsan-hq/qget/releases/download/v0.1.1/qget-v0.1.1-linux-x86_64.tar.gz
test -f /tmp/x/qget-v0.1.1-linux-x86_64/LICENSE
echo "--- verified stream-extract ok"

qget hash --algo sha256 /tmp/sums
remove
! command -v qget >/dev/null
echo "--- $kind: install, use, remove ok"
