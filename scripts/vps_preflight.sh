#!/usr/bin/env bash
# VPS preflight: is this machine ready for the Gaming Analytics stack?
#   bash scripts/vps_preflight.sh
# Prints facts and a verdict per item; changes nothing.
set -u

ok()   { printf '  \033[32mOK\033[0m    %s\n' "$*"; }
warn() { printf '  \033[33mWARN\033[0m  %s\n' "$*"; }
bad()  { printf '  \033[31mFAIL\033[0m  %s\n' "$*"; }

echo "== System"
if [ -r /etc/os-release ]; then . /etc/os-release; ok "OS: ${PRETTY_NAME:-unknown}"; fi
ok "Kernel: $(uname -r), arch $(uname -m)"
cpus=$(nproc 2>/dev/null || echo 1)
[ "$cpus" -ge 2 ] && ok "CPU cores: $cpus" || warn "CPU cores: $cpus (2+ recommended)"

mem_mb=$(awk '/MemTotal/ {print int($2/1024)}' /proc/meminfo)
swap_mb=$(awk '/SwapTotal/ {print int($2/1024)}' /proc/meminfo)
if   [ "$mem_mb" -ge 7500 ]; then ok   "RAM: ${mem_mb} MB (enough for every profile incl. OpenMetadata)"
elif [ "$mem_mb" -ge 3500 ]; then warn "RAM: ${mem_mb} MB (core stack + Airflow + dashboard; skip OpenMetadata)"
else                              bad  "RAM: ${mem_mb} MB (under 4 GB: Airflow will struggle)"; fi
[ "$swap_mb" -gt 0 ] && ok "Swap: ${swap_mb} MB" || warn "Swap: none (add 2-4 GB if RAM is under 8 GB)"

disk_gb=$(df -Pk . | awk 'NR==2 {print int($4/1024/1024)}')
[ "$disk_gb" -ge 15 ] && ok "Free disk here: ${disk_gb} GB" || warn "Free disk here: ${disk_gb} GB (15 GB+ recommended)"

echo "== Tools"
if command -v docker >/dev/null 2>&1; then
  ok "Docker: $(docker --version)"
  if docker info >/dev/null 2>&1; then ok "Docker daemon reachable as $(id -un)"
  else bad "Docker daemon not reachable: start it or add $(id -un) to the docker group"; fi
  if docker compose version >/dev/null 2>&1; then ok "Compose: $(docker compose version --short)"
  else bad "docker compose v2 plugin missing (apt install docker-compose-plugin)"; fi
else
  bad "Docker not installed (see docs/deploy_vps.md, step 2)"
fi
command -v git >/dev/null 2>&1 && ok "git: $(git --version)" || bad "git not installed"
command -v python3 >/dev/null 2>&1 && ok "python3: $(python3 --version 2>&1)" || warn "python3 not installed (only needed for scripts/generate_env.py)"

echo "== Ports this stack binds on 127.0.0.1"
port_in_use() {
  if command -v ss >/dev/null 2>&1; then
    ss -ltnH "sport = :$1" 2>/dev/null | grep -q .
  else  # no ss: try to connect
    (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null
  fi
}
for port in 5432 5433 8080 8501 8585; do
  if port_in_use "$port"; then
    owner=$( (ss -ltnpH "sport = :$port" 2>/dev/null | awk '{print $6}' | head -1) || true)
    bad "port $port already in use ${owner:+($owner)} -> change it in .env (POSTGRES_PORT, ANALYTICS_PORT, AIRFLOW_PORT, DASHBOARD_PORT, OPENMETADATA_PORT)"
  else
    ok "port $port free"
  fi
done

echo "== Outbound network (images, packages, APIs)"
for url in https://registry-1.docker.io/v2/ https://pypi.org/simple/ https://github.com https://api.rawg.io/api/; do
  code=$(curl -s -o /dev/null -m 10 -w '%{http_code}' "$url") || code=000
  [ "$code" != "000" ] && ok "$url -> HTTP $code" || warn "$url unreachable"
done

echo "== Time zone"
ok "$(date '+%Y-%m-%d %H:%M %Z (UTC%z)')  (containers use UTC; DAG schedules use Asia/Ulaanbaatar)"
