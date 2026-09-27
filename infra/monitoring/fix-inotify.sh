#!/usr/bin/env bash
# fix-inotify.sh — Idempotent fix for "too many open files" on K3s node
# Run on 10.0.0.2 with sudo
# The error "failed to create fsnotify watcher: too many open files" means
# the kernel limit for inotify watchers is too low for the number of containers.

set -euo pipefail

echo "=== Fixing inotify limits ==="

SYSCTL_CONF="/etc/sysctl.d/99-inotify.conf"

cat > "$SYSCTL_CONF" << 'EOF'
# Increase inotify limits for container-heavy workloads
fs.inotify.max_user_watches = 524288
fs.inotify.max_user_instances = 1024
fs.file-max = 2097152
EOF

sysctl --system > /dev/null 2>&1

echo "Current values:"
echo "  fs.inotify.max_user_watches  = $(sysctl -n fs.inotify.max_user_watches)"
echo "  fs.inotify.max_user_instances = $(sysctl -n fs.inotify.max_user_instances)"
echo "  fs.file-max                   = $(sysctl -n fs.file-max)"
echo ""
echo "=== Done. Changes are persistent across reboots ==="
