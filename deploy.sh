#!/bin/bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
DBEAVER_APP="/Applications/DBeaver.app"
DBEAVER_ECLIPSE="${DBEAVER_APP}/Contents/Eclipse"
DROPINS_DIR="${DBEAVER_ECLIPSE}/dropins"
DBEAVER_INI="${DBEAVER_ECLIPSE}/dbeaver.ini"
BUNDLES_INFO="${DBEAVER_ECLIPSE}/configuration/org.eclipse.equinox.simpleconfigurator/bundles.info"
BUNDLED_JRE_REL="../Eclipse/jre/Contents/Home/lib/libjli.dylib"
DEBUG_PORT=5005

CORE_PLUGIN="org.jkiss.dbeaver.ext.ydb"
UI_PLUGIN="org.jkiss.dbeaver.ext.ydb.ui"
FEATURE="org.jkiss.dbeaver.ext.ydb.feature"

# --- Functions ---

usage() {
    echo "Usage: $0 [--skip-build] [--restart] [--clean] [--debug]"
    echo ""
    echo "  --skip-build   Skip Maven build, deploy existing artifacts"
    echo "  --restart      Restart DBeaver after deployment"
    echo "  --clean        Remove deployed plugin from DBeaver"
    echo "  --debug        Launch DBeaver with remote debugging (port 5005)"
    echo ""
    exit 0
}

find_jar() {
    local dir="$1"
    local pattern="$2"
    find "$dir" -maxdepth 1 -name "${pattern}_*.jar" -print -quit 2>/dev/null
}

# Register plugin bundles in DBeaver's bundles.info so OSGi loads them.
# The simpleconfigurator reads this file to know which bundles to start.
# Dropins alone are not always reliable after cache resets.
register_bundles() {
    if [ ! -f "$BUNDLES_INFO" ]; then
        echo "WARNING: bundles.info not found, skipping registration."
        return
    fi

    # Remove old ydb entries
    sed -i '' '/org\.jkiss\.dbeaver\.ext\.ydb/d' "$BUNDLES_INFO"

    # Find deployed JARs and extract versions
    local core_jar_name ui_jar_name core_ver ui_ver
    core_jar_name=$(basename "$(ls "${TARGET}/plugins/${CORE_PLUGIN}_"*.jar 2>/dev/null | head -1)")
    ui_jar_name=$(basename "$(ls "${TARGET}/plugins/${UI_PLUGIN}_"*.jar 2>/dev/null | head -1)")
    core_ver="${core_jar_name#${CORE_PLUGIN}_}"
    core_ver="${core_ver%.jar}"
    ui_ver="${ui_jar_name#${UI_PLUGIN}_}"
    ui_ver="${ui_ver%.jar}"

    # Add entries pointing to dropins
    echo "${CORE_PLUGIN},${core_ver},dropins/ydb/plugins/${core_jar_name},4,false" >> "$BUNDLES_INFO"
    echo "${UI_PLUGIN},${ui_ver},dropins/ydb/plugins/${ui_jar_name},4,false" >> "$BUNDLES_INFO"

    echo "Registered bundles in bundles.info (${core_ver})"
}

# --- Parse args ---

SKIP_BUILD=false
RESTART=false
CLEAN=false
DEBUG=false

for arg in "$@"; do
    case "$arg" in
        --skip-build) SKIP_BUILD=true ;;
        --restart)    RESTART=true ;;
        --clean)      CLEAN=true ;;
        --debug)      DEBUG=true; RESTART=true ;;
        --help|-h)    usage ;;
        *) echo "Unknown option: $arg"; usage ;;
    esac
done

# --- Clean mode ---

if [ "$CLEAN" = true ]; then
    echo "Removing YDB plugin from DBeaver..."
    rm -rf "${DROPINS_DIR}/ydb"
    # Remove bundle registrations
    if [ -f "$BUNDLES_INFO" ]; then
        sed -i '' '/org\.jkiss\.dbeaver\.ext\.ydb/d' "$BUNDLES_INFO"
    fi
    echo "Done. Restart DBeaver to apply."
    exit 0
fi

# --- Validate DBeaver installation ---

if [ ! -d "$DBEAVER_ECLIPSE" ]; then
    echo "ERROR: DBeaver not found at ${DBEAVER_APP}"
    echo "Set DBEAVER_APP environment variable or edit this script."
    exit 1
fi

# --- Build ---

if [ "$SKIP_BUILD" = false ]; then
    echo "Building plugin..."
    cd "$PROJECT_DIR"
    mvn clean verify -DskipTests -q
    echo "Build complete."
fi

# --- Find built artifacts ---

REPO_PLUGINS="${PROJECT_DIR}/repository/target/repository/plugins"
REPO_FEATURES="${PROJECT_DIR}/repository/target/repository/features"

CORE_JAR=$(find_jar "$REPO_PLUGINS" "$CORE_PLUGIN")
UI_JAR=$(find_jar "$REPO_PLUGINS" "$UI_PLUGIN")
FEATURE_JAR=$(find_jar "$REPO_FEATURES" "$FEATURE")

if [ -z "$CORE_JAR" ] || [ -z "$UI_JAR" ]; then
    echo "ERROR: Built plugin JARs not found in ${REPO_PLUGINS}"
    echo "Run without --skip-build first."
    exit 1
fi

# --- Deploy to dropins ---

TARGET="${DROPINS_DIR}/ydb"
rm -rf "$TARGET"
mkdir -p "${TARGET}/plugins" "${TARGET}/features"

cp "$CORE_JAR" "${TARGET}/plugins/"
cp "$UI_JAR"   "${TARGET}/plugins/"
[ -n "$FEATURE_JAR" ] && cp "$FEATURE_JAR" "${TARGET}/features/"

echo "Deployed to ${TARGET}:"
ls -lh "${TARGET}/plugins/"
ls -lh "${TARGET}/features/" 2>/dev/null

# --- Register bundles in OSGi configuration ---

register_bundles

# --- Restart DBeaver ---

if [ "$RESTART" = true ]; then
    echo "Restarting DBeaver..."
    osascript -e 'quit app "DBeaver"' 2>/dev/null || true
    sleep 2

    DEBUG_OPT="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:${DEBUG_PORT}"

    # Always remove debug agent lines
    sed -i '' '/^-agentlib:jdwp=/d' "$DBEAVER_INI"

    if [ "$DEBUG" = true ]; then
        # DBeaver ships a stripped JRE without libjdwp, so we need a full JDK
        SYSTEM_JDK="$(/usr/libexec/java_home -v 21 2>/dev/null || true)"
        if [ -z "$SYSTEM_JDK" ]; then
            SYSTEM_JDK="$(/usr/libexec/java_home 2>/dev/null || true)"
        fi

        if [ -z "$SYSTEM_JDK" ] || [ ! -f "${SYSTEM_JDK}/lib/libjdwp.dylib" ]; then
            echo "ERROR: No JDK with debug support (libjdwp) found."
            echo "Install a full JDK (e.g. OpenJDK 21) to use --debug."
            exit 1
        fi

        # Point DBeaver at the system JDK that has libjdwp
        JDK_LIBJLI="${SYSTEM_JDK}/lib/libjli.dylib"
        sed -i '' "s|^${BUNDLED_JRE_REL}$|${JDK_LIBJLI}|" "$DBEAVER_INI"

        # Add debug agent right after -vmargs line
        sed -i '' "s/^-vmargs$/&\\
${DEBUG_OPT}/" "$DBEAVER_INI"

        echo "Using JDK: ${SYSTEM_JDK}"
        echo "Added remote debug agent (port ${DEBUG_PORT})"

        open "$DBEAVER_APP"
        echo "DBeaver started with remote debugging on port ${DEBUG_PORT}."
        echo "Attach debugger with: localhost:${DEBUG_PORT}"
    else
        # Restore bundled JRE path if it was changed
        sed -i '' "s|.*/lib/libjli.dylib|${BUNDLED_JRE_REL}|" "$DBEAVER_INI"

        open "$DBEAVER_APP"
        echo "DBeaver restarted."
    fi
else
    echo ""
    echo "Restart DBeaver to load the updated plugin."
fi
