#!/usr/bin/env bash
set -e
exec "$(dirname "$0")/scripts/query-kagent.sh" "$@"
