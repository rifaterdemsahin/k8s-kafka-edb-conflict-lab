#!/usr/bin/env bash
set -e
exec "$(dirname "$0")/scripts/test-conflict.sh" "$@"
