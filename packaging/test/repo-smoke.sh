#!/bin/sh
# Add the qget repository the way the website tells users to, install qget,
# use it, then upgrade-check and remove it.
#   docker run --rm --network host -v "$PWD/packaging/test:/t:ro" -e BASE=https://qget.dharrsan.com \
#     debian:12 sh /t/repo-smoke.sh deb
set -eu
kind="$1"
BASE="${BASE:-https://qget.dharrsan.com}"
FPR="${FPR:-A03F3628DC0F7B4A2E9AE313AB43FFC72673B3AA}"

case "$kind" in
  deb)
    export DEBIAN_FRONTEND=noninteractive
    apt-get update -qq && apt-get install -y -qq curl ca-certificates >/dev/null
    # --- the website's instructions ---
    curl --create-dirs -fsSLo /etc/apt/keyrings/qget.asc "$BASE/key.asc"
    echo "deb [signed-by=/etc/apt/keyrings/qget.asc] $BASE/apt stable main" > /etc/apt/sources.list.d/qget.list
    apt-get update -qq && apt-get install -y -qq qget
    # ---
    apt-cache policy qget | head -3
    remove() { apt-get remove -y -qq qget; }
    ;;
  rpm)
    dnf install -y -q curl >/dev/null 2>&1 || true
    curl -fsSLo /etc/yum.repos.d/qget.repo "$BASE/rpm/qget.repo"
    dnf install -y qget
    rpm -q qget --qf '%{NAME}-%{VERSION} signed: %{SIGPGP:pgpsig}%{RSAHEADER:pgpsig}\n'
    remove() { dnf remove -y -q qget; }
    ;;
  arch)
    # Real installs create pacman's local master key during setup; the Docker image doesn't.
    pacman-key --list-secret-keys 2>/dev/null | grep -q '^sec' || pacman-key --init >/dev/null 2>&1
    pacman -Sy --noconfirm --needed archlinux-keyring >/dev/null
    curl -fsSL "$BASE/key.asc" | pacman-key --add /dev/stdin
    pacman-key --lsign-key "$FPR"
    printf '\n[qget]\nServer = %s/arch/$arch\n' "$BASE" >> /etc/pacman.conf
    pacman -Syu --noconfirm qget
    remove() { pacman -R --noconfirm qget; }
    ;;
  *) echo "usage: repo-smoke.sh deb|rpm|arch" >&2; exit 2 ;;
esac

echo "--- installed from repo: $(qget --version)"
qget hash --algo sha256 /etc/hostname
remove
! command -v qget >/dev/null
echo "--- $kind repo: add, install, use, remove ok"
