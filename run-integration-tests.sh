#!/usr/bin/env bash
# Convenience wrapper: delegates to the canonical test runner.
set -euo pipefail
exec "$(dirname "${BASH_SOURCE[0]}")/integration-tests/run-tests.sh" "$@"
