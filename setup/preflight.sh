#!/usr/bin/env bash
# Musin preflight (F1.1). Run inside WSL2 Ubuntu from the repo root:
#   bash setup/preflight.sh
# Checks the machine against PRD §7.2 / §8.5 and creates the $MUSIN_DATA
# directory tree (idempotent). Never downloads or deletes anything.
set -uo pipefail

fail=0
ok()   { printf '  \033[32mOK\033[0m    %s\n' "$*"; }
warn() { printf '  \033[33mWARN\033[0m  %s\n' "$*"; }
bad()  { printf '  \033[31mFAIL\033[0m  %s\n' "$*"; fail=1; }

echo "Musin preflight"

# 1. Running in WSL2 / Linux, not native Windows.
if grep -qi microsoft /proc/version 2>/dev/null; then
  ok "running inside WSL2 ($(. /etc/os-release && echo "$PRETTY_NAME"))"
elif [[ "$(uname -s)" == Linux ]]; then
  ok "running on Linux"
else
  bad "not running in WSL2/Linux; open an Ubuntu (WSL) shell"
fi

# 2. .env present with every §8.5 key set.
if [[ ! -f .env ]]; then
  bad ".env missing: cp .env.example .env and fill it in"
else
  set -a; source .env; set +a
  env_fail=0
  for key in MUSIN_DATA NEO4J_PASSWORD NEO4J_HTTP_PORT NEO4J_BOLT_PORT \
             SPARK_DRIVER_MEMORY NEO4J_HEAP_SIZE NEO4J_PAGECACHE_SIZE; do
    [[ -n "${!key:-}" ]] || { bad ".env: $key is empty"; env_fail=1; }
  done
  if [[ -n "${NEO4J_PASSWORD:-}" && ${#NEO4J_PASSWORD} -lt 8 ]]; then
    bad ".env: NEO4J_PASSWORD must be at least 8 characters (Neo4j 5 rule)"; env_fail=1
  fi
  (( env_fail )) || ok ".env has all PRD §8.5 keys"
fi

# 3. Data root: absolute, inside the WSL filesystem, with ≥100 GB free.
data="${MUSIN_DATA:-}"
if [[ -z "$data" ]]; then
  bad "MUSIN_DATA not set"
elif [[ "$data" == *YOUR_WSL_USER* ]]; then
  bad "MUSIN_DATA still has the .env.example placeholder; set it to e.g. $HOME/musin-data"
elif [[ "$data" != /* ]]; then
  bad "MUSIN_DATA must be an absolute path (got '$data')"
elif [[ "$data" == /mnt/* ]]; then
  bad "MUSIN_DATA is on the Windows filesystem ($data); use e.g. $HOME/musin-data"
else
  mkdir -p "$data"/{dumps,raw,clean,graph,import,tmp} "$data"/neo4j/{data,logs}
  ok "data dirs ready under $data"
  # Inside WSL, df shows the virtual disk's MAXIMUM size, not real space. The
  # virtual disk is a file on the Windows drive, so that drive's free space is
  # the real limit (checked via /mnt/c when present).
  host=$([[ -d /mnt/c ]] && echo /mnt/c || echo "$data")
  free_gb=$(df -BG --output=avail "$host" | tail -1 | tr -dc '0-9')
  if (( free_gb >= 100 )); then ok "${free_gb} GB free on the host drive ($host)"
  else warn "only ${free_gb} GB free on the host drive ($host); full runs need ≥100 GB (RULES §10)"; fi
fi

# 4. Memory visible to WSL (set by %USERPROFILE%\.wslconfig).
mem_gb=$(awk '/MemTotal/ {printf "%d", $2/1024/1024 + 0.5}' /proc/meminfo)
if (( mem_gb >= 17 )); then ok "${mem_gb} GB RAM visible to WSL (full mode possible)"
else warn "${mem_gb} GB RAM visible to WSL: sample mode only, or install setup/wslconfig.example"; fi

# 5. Docker reachable from this shell (Docker Desktop → Settings → Resources → WSL integration).
if docker info >/dev/null 2>&1; then
  ok "docker $(docker version --format '{{.Server.Version}}'), $(docker compose version --short 2>/dev/null | sed 's/^/compose /')"
else
  bad "docker not reachable: start Docker Desktop and enable WSL integration for this distro"
fi

# 6. UID matches the image's user (1000), so container files are owned by you.
if [[ "$(id -u)" != 1000 ]]; then
  warn "your UID is $(id -u), not 1000: build with  docker compose build --build-arg HOST_UID=$(id -u) spark"
fi

echo
if (( fail )); then echo "Preflight FAILED: fix the items above."; exit 1; fi
echo "Preflight passed."
