#!/usr/bin/env bash
set -euo pipefail

# orchestra-pi install script
# Copies extensions and creates state directories under ~/.agent/

EXTENSIONS_DIR="${HOME}/.pi/agent/extensions"
STATE_DIRS=(
  "${HOME}/.agent/orchestra-queue"
  "${HOME}/.agent/orchestra-listeners"
  "${HOME}/.agent/orchestra-listener-state"
  "${HOME}/.agent/orchestra-logs"
)

echo "📦 Installing orchestra-pi extensions..."

# Create extensions directory
mkdir -p "$EXTENSIONS_DIR"

# Copy each extension package
for pkg in packages/pi-queue packages/pi-github-tools packages/pi-listeners packages/pi-autoformalize; do
  if [ -d "$pkg" ]; then
    src="$pkg/src/$(basename "$pkg").ts"
    if [ -f "$src" ]; then
      cp "$src" "$EXTENSIONS_DIR/"
      echo "  ✓ $(basename "$pkg").ts"
    else
      echo "  ⚠ $pkg: no source file at $src (skipping)"
    fi
  else
    echo "  ⚠ $pkg directory not found (skipping)"
  fi
done

# Create state directories
for dir in "${STATE_DIRS[@]}"; do
  mkdir -p "$dir"
done
echo "  ✓ State directories created"

# Create log file if it doesn't exist
touch "${HOME}/.agent/orchestra-logs/daemon.log"
echo "  ✓ daemon.log ready"

echo ""
echo "✅ Installation complete!"
echo ""
echo "Extensions installed to: $EXTENSIONS_DIR"
echo "State directories: ~/.agent/orchestra-*/"
echo ""
echo "View daemon logs from tmux: tail -f ~/.agent/orchestra-logs/daemon.log"
