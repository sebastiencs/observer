#!/usr/bin/env bash
set -euo pipefail

root="$(git rev-parse --show-toplevel)"
cd "$root"

chmod +x .githooks/pre-commit
git config core.hooksPath .githooks

echo "Git hooks enabled: core.hooksPath=.githooks"
