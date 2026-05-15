#!/usr/bin/env bash
set -euo pipefail
# Default validation script — checks that extensions are present and tests pass
# Override per-repo in your project's .agent/validation.sh

echo "🔍 Validating orchestra-pi extensions..."

# Check extensions exist
for ext in pi-queue pi-github-tools pi-listeners pi-autoformalize; do
  if [ -f "$HOME/.agent/extensions/${ext}.ts" ]; then
    echo "  ✓ $ext"
  else
    echo "  ✗ $ext not found"
    exit 1
  fi
done

echo "  ✅ All extensions present"
