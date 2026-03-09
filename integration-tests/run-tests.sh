#!/usr/bin/env bash
# Run YDB integration tests: starts local YDB + MinIO, runs tests, cleans up.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

YDB_IMAGE="ghcr.io/ydb-platform/local-ydb:25.4"
YDB_CONTAINER="ydb-it"
YDB_PORT=2136
YDB_URL="jdbc:ydb:grpc://localhost:${YDB_PORT}/local"
YDB_DATA_DIR="/tmp/ydb-it-data"
YDB_CONFIG_PATH="/ydb_data/cluster/kikimr_configs/config.yaml"
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

port_open() {
    # Use bash built-in /dev/tcp — works everywhere without nc
    (echo >/dev/tcp/localhost/"$1") 2>/dev/null
}

cleanup() {
    log "Cleaning up containers ..."
    $RT rm -f "${YDB_CONTAINER}" >/dev/null 2>&1 || true
    $RT rm -f "${MINIO_CONTAINER}" >/dev/null 2>&1 || true
    rm -rf "${YDB_DATA_DIR}"
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
rm -rf "${YDB_DATA_DIR}"
mkdir -p "${YDB_DATA_DIR}"

# ── start MinIO (S3 for external table tests) ────────────────────────────────
log "Starting MinIO ..."
$RT run -d \
    --name "${MINIO_CONTAINER}" \
    --network=host \
    -e MINIO_ROOT_USER=minioadmin \
    -e MINIO_ROOT_PASSWORD=minioadmin \
    "${MINIO_IMAGE}" server /data --address ":${MINIO_PORT}" >/dev/null

# ── start YDB (first time) to generate default config ────────────────────────
# YDB_USE_IN_MEMORY_PDISKS=false is required: Topics, Views, and Resource Pools
# do not work with in-memory pdisks. YDB_PDISK_SIZE is in bytes (64 GB sparse).
log "Starting YDB ${YDB_IMAGE} to generate config ..."
$RT run -d \
    --name "${YDB_CONTAINER}" \
    --network=host \
    -v "${YDB_DATA_DIR}:/ydb_data" \
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

# ── patch config, then re-run with --config-path ─────────────────────────────
# local_ydb deploy regenerates config.yaml on every start UNLESS --config-path
# is given — in that case it copies the provided file instead of generating.
# We patch the generated config on the host, save it to a separate path,
# then re-run the container pointing --config-path at that file (mounted via -v).
log "Patching config: enabling feature flags and replication gRPC service ..."
GENERATED_CONFIG="${YDB_DATA_DIR}/cluster/kikimr_configs/config.yaml"
PATCHED_CONFIG="/tmp/ydb-it-patched-config.yaml"
cp "${GENERATED_CONFIG}" "${PATCHED_CONFIG}"

if ! grep -q 'enable_resource_pools' "${PATCHED_CONFIG}"; then
    sed -i '/^feature_flags:/a\  enable_resource_pools: true\n  enable_external_data_sources: true\n  enable_streaming_queries: true\n  enable_topic_transfer: true' "${PATCHED_CONFIG}"
fi
if ! grep -q '^\s*- replication' "${PATCHED_CONFIG}"; then
    sed -i '/^  services:$/a\  - replication' "${PATCHED_CONFIG}"
fi

$RT rm -f "${YDB_CONTAINER}" >/dev/null 2>&1
rm -rf "${YDB_DATA_DIR}"
mkdir -p "${YDB_DATA_DIR}"

log "Restarting YDB with patched config ..."
$RT run -d \
    --name "${YDB_CONTAINER}" \
    --network=host \
    -v "${YDB_DATA_DIR}:/ydb_data" \
    -v "${PATCHED_CONFIG}:/tmp/ydb-custom-config.yaml:ro" \
    -e YDB_USE_IN_MEMORY_PDISKS=false \
    -e YDB_PDISK_SIZE=68719476736 \
    -e GRPC_PORT="${YDB_PORT}" \
    "${YDB_IMAGE}" \
    --config-path /tmp/ydb-custom-config.yaml >/dev/null

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
