#!/usr/bin/env bash
# Rebuild sqlite3.c / sqlite3.h from canonical SQLite sources with
# SQLITE_ENABLE_UPDATE_DELETE_LIMIT enabled during amalgamation generation.
set -euo pipefail

SQLITE_VERSION="${SQLITE_VERSION:-3.53.4}"
SQLITE_YEAR="${SQLITE_YEAR:-2026}"
# 3.53.4 -> 3530400
SQLITE_NUM=$(SQLITE_VERSION="$SQLITE_VERSION" python3 - <<'PY'
import os
parts = os.environ["SQLITE_VERSION"].split(".")
major, minor, patch = int(parts[0]), int(parts[1]), int(parts[2]) if len(parts) > 2 else 0
print(f"{major}{minor:02d}{patch:02d}00")
PY
)

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
WORK_DIR="$(mktemp -d)"
trap 'rm -rf "$WORK_DIR"' EXIT

ZIP_NAME="sqlite-src-${SQLITE_NUM}.zip"
SRC_DIR="sqlite-src-${SQLITE_NUM}"
URL="https://www.sqlite.org/${SQLITE_YEAR}/${ZIP_NAME}"

echo "Downloading ${URL} ..."
curl -fsSL -A 'kdb3-sqlite-upgrade' -o "${WORK_DIR}/${ZIP_NAME}" "${URL}"

echo "Unpacking ..."
unzip -q "${WORK_DIR}/${ZIP_NAME}" -d "${WORK_DIR}"

echo "Configuring and building amalgamation (UPDATE_DELETE_LIMIT=1) ..."
cd "${WORK_DIR}/${SRC_DIR}"
CFLAGS='-DSQLITE_ENABLE_UPDATE_DELETE_LIMIT=1' ./configure >/dev/null
make sqlite3.c

echo "Installing into ${SCRIPT_DIR} ..."
cp -f sqlite3.c sqlite3.h "${SCRIPT_DIR}/"

VERSION_LINE=$(grep -E '^#define SQLITE_VERSION ' "${SCRIPT_DIR}/sqlite3.h" | head -1)
echo "Done. ${VERSION_LINE}"
