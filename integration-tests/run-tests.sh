#!/usr/bin/env bash
# Run YDB integration tests: starts local YDB + MinIO, runs tests, cleans up.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

YDB_IMAGE="ghcr.io/ydb-platform/local-ydb:25.4"
YDB_CONTAINER="ydb-it"
YDB_PORT=2136
YDB_URL="jdbc:ydb:grpc://localhost:${YDB_PORT}/local"
YDB_DATA_DIR="/tmp/ydb-it-data"
YDB_CONFIG_DIR="/tmp/ydb-it-config"
WAIT_TIMEOUT=120

MINIO_IMAGE="quay.io/minio/minio:latest"
MINIO_CONTAINER="minio-it"
MINIO_PORT=9000

# ── select container runtime (podman preferred, fallback to docker) ──────────
if command -v podman &>/dev/null; then
    RT=podman
elif command -v docker &>/dev/null; then
    RT=docker
else
    echo "ERROR: neither podman nor docker found" >&2; exit 1
fi

# ── helpers ──────────────────────────────────────────────────────────────────
log()  { echo "[run-tests] $*"; }
die()  { echo "[run-tests] ERROR: $*" >&2; exit 1; }

cleanup() {
    log "Cleaning up containers ..."
    $RT rm -f "${YDB_CONTAINER}" >/dev/null 2>&1 || true
    $RT rm -f "${MINIO_CONTAINER}" >/dev/null 2>&1 || true
    rm -rf "${YDB_DATA_DIR}" "${YDB_CONFIG_DIR}"
}
trap cleanup EXIT

# ── pull images ──────────────────────────────────────────────────────────────
for img in "${YDB_IMAGE}" "${MINIO_IMAGE}"; do
    log "Pulling ${img} ..."
    $RT pull "${img}"
done

# ── stop existing containers ─────────────────────────────────────────────────
$RT rm -f "${YDB_CONTAINER}" >/dev/null 2>&1 || true
$RT rm -f "${MINIO_CONTAINER}" >/dev/null 2>&1 || true
rm -rf "${YDB_DATA_DIR}" "${YDB_CONFIG_DIR}"
mkdir -p "${YDB_DATA_DIR}" "${YDB_CONFIG_DIR}"

# ── start MinIO (S3 for external table tests) ────────────────────────────────
log "Starting MinIO ..."
$RT run -d \
    --name "${MINIO_CONTAINER}" \
    --network=host \
    -e MINIO_ROOT_USER=minioadmin \
    -e MINIO_ROOT_PASSWORD=minioadmin \
    "${MINIO_IMAGE}" server /data --address ":${MINIO_PORT}" >/dev/null

# ── Step 1: start YDB WITHOUT data dir mount to generate default config ──────
# https://ydb.tech/docs/ru/reference/docker/start?version=v25.2#pereopredelenie-fajla-konfiguracii
log "Step 1: Starting YDB ${YDB_IMAGE} to generate config (no data mount) ..."
$RT run -d \
    --rm \
    --name "${YDB_CONTAINER}" \
    --hostname localhost \
    -p 2135:2135 \
    -p 2136:2136 \
    -p 8765:8765 \
    -e YDB_USE_IN_MEMORY_PDISKS=false \
    -e YDB_PDISK_SIZE=68719476736 \
    -e GRPC_PORT="${YDB_PORT}" \
    "${YDB_IMAGE}" >/dev/null

log "Waiting for YDB (health_check) ..."
for i in $(seq 1 "${WAIT_TIMEOUT}"); do
    if $RT exec "${YDB_CONTAINER}" /health_check >/dev/null 2>&1; then
        log "YDB ready after ${i}s"
        break
    fi
    if [ "${i}" -eq "${WAIT_TIMEOUT}" ]; then
        $RT logs "${YDB_CONTAINER}" >&2
        die "YDB did not start within ${WAIT_TIMEOUT}s"
    fi
    sleep 1
done

# ── Step 2: copy generated config out of container ───────────────────────────
log "Step 2: Extracting generated config via ${RT} cp ..."
$RT cp "${YDB_CONTAINER}:/ydb_data/cluster/kikimr_configs/config.yaml" "${YDB_CONFIG_DIR}/config.yaml"

# ── Step 3: stop container, remove data dir ──────────────────────────────────
log "Step 3: Stopping container and cleaning data dir ..."
$RT stop "${YDB_CONTAINER}" >/dev/null 2>&1 || true
rm -rf "${YDB_DATA_DIR}"

# ── Step 4: patch config ─────────────────────────────────────────────────────
log "Step 4: Patching config: enabling feature flags and replication gRPC service ..."
PATCHED_CONFIG="${YDB_CONFIG_DIR}/config.yaml"

if ! grep -q 'enable_resource_pools' "${PATCHED_CONFIG}"; then
    sed -i '/^feature_flags:/a\  enable_resource_pools: true\n  enable_external_data_sources: true\n  enable_streaming_queries: true\n  enable_topic_transfer: true' "${PATCHED_CONFIG}"
fi
if ! grep -q '^\s*- replication' "${PATCHED_CONFIG}"; then
    sed -i '/^  services:$/a\  - replication' "${PATCHED_CONFIG}"
fi

# ── Step 5: run with patched config, mount data dir and config dir ───────────
mkdir -p "${YDB_DATA_DIR}"
log "Step 5: Starting YDB with patched config ..."
$RT run -d \
    --rm \
    --name "${YDB_CONTAINER}" \
    --hostname localhost \
    -p 2135:2135 \
    -p 2136:2136 \
    -p 8765:8765 \
    -v "${YDB_DATA_DIR}:/ydb_data" \
    -v "${YDB_CONFIG_DIR}:/ydb_config" \
    -e YDB_USE_IN_MEMORY_PDISKS=false \
    -e YDB_PDISK_SIZE=68719476736 \
    -e GRPC_PORT="${YDB_PORT}" \
    "${YDB_IMAGE}" \
    --config-path /ydb_config/config.yaml >/dev/null

log "Waiting for YDB to restart with updated config ..."
for i in $(seq 1 "${WAIT_TIMEOUT}"); do
    if $RT exec "${YDB_CONTAINER}" /health_check >/dev/null 2>&1; then
        log "YDB restarted after ${i}s"
        break
    fi
    if [ "${i}" -eq "${WAIT_TIMEOUT}" ]; then
        $RT logs "${YDB_CONTAINER}" >&2
        die "YDB did not restart within ${WAIT_TIMEOUT}s"
    fi
    sleep 1
done

# ── wait for ydbd to start and system tables to be active ────────────────────
# health_check passes when gRPC is up, but schema-shard tablets (which enforce
# feature flags) and system tables like .sys/streaming_queries take longer.
# Poll until .sys/resource_pools is queryable — same guarantee as DBeaver tests.
log "Waiting for feature flags and system tables to be active ..."
for i in $(seq 1 "${WAIT_TIMEOUT}"); do
    if $RT exec "${YDB_CONTAINER}" /ydb \
        --endpoint "grpc://localhost:${YDB_PORT}" \
        --database /local \
        yql -s "SELECT 1 FROM \`.sys/resource_pools\` LIMIT 1" \
        >/dev/null 2>&1; then
        log "System tables active after ${i}s"
        break
    fi
    if [ "${i}" -eq "${WAIT_TIMEOUT}" ]; then
        $RT logs "${YDB_CONTAINER}" >&2
        die "System tables did not become active within ${WAIT_TIMEOUT}s"
    fi
    sleep 1
done

# ── build ydb-plugin-core (shared code used by tests) ────────────────────────
log "Building ydb-plugin-core ..."
mvn install -f "${SCRIPT_DIR}/../ydb-plugin-core/pom.xml" -q

# ── run tests ────────────────────────────────────────────────────────────────
log "Running integration tests against ${YDB_URL} ..."
EXIT_CODE=0
mvn verify -f "${SCRIPT_DIR}/pom.xml" \
    -Dydb.jdbc.url="${YDB_URL}" \
    -Ds3.url="http://localhost:${MINIO_PORT}" \
    || EXIT_CODE=$?

exit "${EXIT_CODE}"
