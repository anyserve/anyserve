#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

check() {
    local matches status=0
    matches=$(git grep --untracked -n "$@") || status=$?
    if (( status == 0 )); then
        printf '%s\n' "$matches"
        return 1
    fi
    if (( status == 1 )); then
        return 0
    fi
    return "$status"
}

# Join fragments so the checker does not flag its own pattern list.
check -e 'kernel''::' -e 'control''plane' -e 'Frame''Plane' \
    -e 'State''Store' -e 'kv_''cache' -e 'max_''retained_bytes' \
    -- . ':!target'
# The server-owned receipt column is the sole allowed occurrence.
check -e 'result_''digest' -- . ':!target' ':!crates/anyserve-core/src/journal.rs'
