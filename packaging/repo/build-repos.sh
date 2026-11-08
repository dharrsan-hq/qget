#!/usr/bin/env bash
# Build signed apt, dnf and pacman repositories from a directory of packages.
#
#   GNUPGHOME=… SIGNING_KEY=<fingerprint> SIGNING_PASSPHRASE_FILE=… \
#     packaging/repo/build-repos.sh <packages-dir> <out-dir> [base-url]
#
# <packages-dir> holds qget_*.deb, qget-*.rpm and qget-*.pkg.tar.zst for one or
# more versions and architectures. Each repository is built with its distro's
# own tools inside a container (debian, fedora, archlinux), using a copy of the
# signing keyring; the passphrase is read from a file, never from arguments.
#
# Output layout (served at the site root):
#   apt/   dists/stable/{InRelease,Release,Release.gpg,main/binary-*/Packages*}  pool/main/q/qget/*.deb
#   rpm/   qget.repo  x86_64/ aarch64/  (*.rpm signed, repodata/repomd.xml.asc)
#   arch/  x86_64/ aarch64/  (qget.db + .sig, *.pkg.tar.zst + .sig)
set -euo pipefail
pkgs=$(realpath "${1:?packages dir}")
out=$(realpath -m "${2:?output dir}")
base="${3:-https://qget.dharrsan.com}"
: "${GNUPGHOME:?set GNUPGHOME to the keyring holding the signing key}"
: "${SIGNING_KEY:?set SIGNING_KEY to the key fingerprint}"
: "${SIGNING_PASSPHRASE_FILE:?set SIGNING_PASSPHRASE_FILE}"
gnupg=$(realpath "$GNUPGHOME")
pass=$(realpath "$SIGNING_PASSPHRASE_FILE")
here=$(dirname "$(realpath "$0")")
mkdir -p "$out"

# Run a step in a container with the keyring copied to /root/.gnupg and gpg
# configured for non-interactive signing.
in_container() {
  local image="$1"; shift
  docker run --rm -i \
    -v "$pkgs:/pkgs:ro" -v "$out:/out" -v "$gnupg:/gnupg-src:ro" -v "$pass:/run/pass:ro" \
    -e SIGNING_KEY="$SIGNING_KEY" -e BASE="$base" -e HOST_UID="$(id -u)" -e HOST_GID="$(id -g)" \
    "$image" bash -euo pipefail -c '
      setup_gpg() {
        mkdir -p /root/.gnupg && cp -r /gnupg-src/. /root/.gnupg/ && chmod -R go-rwx /root/.gnupg
        printf "batch\npinentry-mode loopback\npassphrase-file /run/pass\nno-permission-warning\n" >> /root/.gnupg/gpg.conf
        printf "allow-loopback-pinentry\n" >> /root/.gnupg/gpg-agent.conf
      }
      '"$*"'
      chown -R "$HOST_UID:$HOST_GID" /out'
}

echo "== apt"
in_container debian:12 '
  export DEBIAN_FRONTEND=noninteractive
  apt-get update -qq && apt-get install -y -qq apt-utils gnupg >/dev/null
  setup_gpg
  rm -rf /out/apt && mkdir -p /out/apt/pool/main/q/qget
  cp /pkgs/*.deb /out/apt/pool/main/q/qget/
  cd /out/apt
  for a in amd64 arm64; do
    d=dists/stable/main/binary-$a; mkdir -p $d
    apt-ftparchive --arch $a packages pool > $d/Packages
    gzip -9nk $d/Packages
  done
  apt-ftparchive \
    -o APT::FTPArchive::Release::Origin=qget -o APT::FTPArchive::Release::Label=qget \
    -o APT::FTPArchive::Release::Suite=stable -o APT::FTPArchive::Release::Codename=stable \
    -o APT::FTPArchive::Release::Architectures="amd64 arm64" -o APT::FTPArchive::Release::Components=main \
    -o APT::FTPArchive::Release::Description="qget packages" \
    release dists/stable > /tmp/Release && mv /tmp/Release dists/stable/Release
  gpg -u "$SIGNING_KEY" --clearsign -o dists/stable/InRelease dists/stable/Release
  gpg -u "$SIGNING_KEY" -abs -o dists/stable/Release.gpg dists/stable/Release
  echo "apt: $(ls pool/main/q/qget | wc -l) packages"'

echo "== rpm"
in_container fedora:latest '
  dnf install -y -q createrepo_c rpm-sign gnupg2 >/dev/null
  setup_gpg
  rm -rf /out/rpm && mkdir -p /out/rpm
  gpg --armor --export "$SIGNING_KEY" > /tmp/key.asc && rpm --import /tmp/key.asc
  for a in x86_64 aarch64; do
    ls /pkgs/qget-*.$a.rpm >/dev/null 2>&1 || continue
    mkdir -p /out/rpm/$a && cp /pkgs/qget-*.$a.rpm /out/rpm/$a/
    rpmsign --define "_gpg_name $SIGNING_KEY" --define "_gpg_sign_cmd_extra_args --pinentry-mode loopback --passphrase-file /run/pass" \
      --addsign /out/rpm/$a/*.rpm >/dev/null
    rpm -K /out/rpm/$a/*.rpm | grep -v "digests signatures OK" && { echo "rpm signature check failed"; exit 1; } || true
    createrepo_c -q /out/rpm/$a
    gpg -u "$SIGNING_KEY" --detach-sign --armor /out/rpm/$a/repodata/repomd.xml
  done
  cat > /out/rpm/qget.repo <<EOF
[qget]
name=qget
baseurl=$BASE/rpm/\$basearch
enabled=1
gpgcheck=1
repo_gpgcheck=1
gpgkey=$BASE/key.asc
EOF
  echo "rpm: $(find /out/rpm -name "*.rpm" | wc -l) packages"'

echo "== arch"
in_container archlinux:latest '
  setup_gpg
  rm -rf /out/arch && mkdir -p /out/arch
  for a in x86_64 aarch64; do
    ls /pkgs/qget-*-$a.pkg.tar.zst >/dev/null 2>&1 || continue
    mkdir -p /out/arch/$a && cp /pkgs/qget-*-$a.pkg.tar.zst /out/arch/$a/
    cd /out/arch/$a
    for p in *.pkg.tar.zst; do gpg -u "$SIGNING_KEY" --detach-sign --no-armor "$p"; done
    GPGKEY="$SIGNING_KEY" repo-add -q --sign --key "$SIGNING_KEY" qget.db.tar.gz *.pkg.tar.zst
    # repo-add makes symlinks; static hosting needs real files.
    for l in qget.db qget.db.sig qget.files qget.files.sig; do [ -L "$l" ] && cp --remove-destination "$(readlink "$l")" "$l"; done
  done
  echo "arch: $(find /out/arch -name "*.pkg.tar.zst" | wc -l) packages"'

gpg --homedir "$gnupg" --armor --export "$SIGNING_KEY" > "$out/key.asc"
echo "repositories → $out"
