#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${XENORA_APP_DIR:-/opt/xenora}"
PYTHON_BIN="${PYTHON_BIN:-python3}"

if [[ "$(id -u)" != "0" ]]; then
  echo "Run this installer as root: sudo bash setup_vps.sh"
  exit 1
fi

apt-get update
apt-get install -y python3 python3-venv python3-pip curl ca-certificates

mkdir -p "$APP_DIR"
cp -a ./. "$APP_DIR/"
cd "$APP_DIR"

if [[ ! -f .env ]]; then
  cp .env.example .env
  echo "Created $APP_DIR/.env"
  echo "Edit BOT_TOKEN and XENORA_PUBLIC_BASE_URL before starting XENORA."
fi

$PYTHON_BIN -m venv .venv
.venv/bin/pip install --upgrade pip
.venv/bin/pip install -r requirements.txt

# Point systemd at the virtualenv Python.
sed "s#ExecStart=/usr/bin/python3 /opt/xenora/main.py#ExecStart=$APP_DIR/.venv/bin/python $APP_DIR/main.py#; s#WorkingDirectory=/opt/xenora#WorkingDirectory=$APP_DIR#; s#EnvironmentFile=/opt/xenora/.env#EnvironmentFile=$APP_DIR/.env#" xenora.service > /etc/systemd/system/xenora.service
systemctl daemon-reload
systemctl enable xenora

echo
systemctl restart xenora
systemctl --no-pager --full status xenora || true

echo
 echo "XENORA installed in $APP_DIR"
echo "Edit $APP_DIR/.env if needed, then: systemctl restart xenora"
echo "Website URLs use your configured public domain, or a Xenora-branded nip.io fallback."
