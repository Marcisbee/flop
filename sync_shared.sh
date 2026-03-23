#!/bin/sh
# Copies shared assets into Go source tree for go:embed.
# Source of truth: ../shared/
# Run via: go generate ./...
set -e
cd "$(dirname "$0")"
cp ../shared/admin/login.html internal/server/login.html
cp ../shared/admin/setup.html internal/server/setup.html
cp ../shared/admin/admin.html internal/server/admin.html
