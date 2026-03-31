#!/usr/bin/env bash
set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
  echo "Execute como root: sudo bash scripts/vps_setup.sh"
  exit 1
fi

DEPLOY_USER="${SUDO_USER:-${USER:-ubuntu}}"
DEPLOY_HOME="$(getent passwd "${DEPLOY_USER}" | cut -d: -f6)"
DEPLOY_PATH="${DEPLOY_PATH:-${DEPLOY_HOME}/meucandidato}"

apt-get update
apt-get install -y ca-certificates curl gnupg lsb-release ufw jq fail2ban apt-transport-https
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
chmod a+r /etc/apt/keyrings/docker.gpg

cat >/etc/apt/sources.list.d/docker.list <<EOF
deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "${VERSION_CODENAME}") stable
EOF

apt-get update
apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
systemctl enable --now docker
usermod -aG docker "${DEPLOY_USER}"

mkdir -p /etc/docker
cat >/etc/docker/daemon.json <<'EOF'
{
  "log-driver": "json-file",
  "log-opts": {
    "max-size": "25m",
    "max-file": "5"
  }
}
EOF

systemctl restart docker

mkdir -p "${DEPLOY_PATH}/deploy/nginx/www"
mkdir -p "${DEPLOY_PATH}/data/curated"
chown -R "${DEPLOY_USER}:${DEPLOY_USER}" "${DEPLOY_PATH}"

ufw default deny incoming
ufw default allow outgoing
ufw allow 22/tcp
ufw allow 80/tcp
ufw allow 443/tcp
ufw --force enable

apt-get autoremove -y
apt-get autoclean -y

echo "Setup inicial concluido."
echo "Reinicie a sessao do usuario ${DEPLOY_USER} para aplicar o grupo docker."
echo "Diretorio de deploy preparado em: ${DEPLOY_PATH}"
