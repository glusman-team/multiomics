#!/usr/bin/env bash
# Verify every python package's uv.lock is in sync with its pyproject.toml.
# Used by prek (pre-push) and CI. Cheap: no venv, metadata check only.
set -euo pipefail
cd "$(dirname "$0")/.."

status=0
for dir in dagster/libs/*/ dagster/projects/*/; do
  [ -f "$dir/pyproject.toml" ] || continue
  echo "uv lock --check: $dir"
  (cd "$dir" && uv lock --check) || status=1
done

exit "$status"
