#!/bin/bash
# Hourly database backup script for JobHarvest
# Replaces the single backup file each time (no accumulation)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
BACKUP_FILE="$SCRIPT_DIR/jobharvest_latest.dump"
SCHEMA_FILE="$SCRIPT_DIR/create_db.sql"
CONTAINER="jobharvest-postgres"
DB_USER="jobharvest"
DB_NAME="jobharvest"

# Check if postgres container is running
if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') - PostgreSQL container not running, skipping backup"
    exit 0
fi

echo "$(date '+%Y-%m-%d %H:%M:%S') - Starting database backup..."

# Schema dump (always up to date)
docker exec "$CONTAINER" pg_dump -U "$DB_USER" -d "$DB_NAME" --schema-only --no-owner --no-privileges > "$SCHEMA_FILE.tmp" 2>/dev/null
mv "$SCHEMA_FILE.tmp" "$SCHEMA_FILE"

# Full backup (compressed custom format, replaces previous)
docker exec "$CONTAINER" pg_dump -U "$DB_USER" -d "$DB_NAME" -Fc --compress=9 > "$BACKUP_FILE.tmp" 2>/dev/null
mv "$BACKUP_FILE.tmp" "$BACKUP_FILE"

SIZE=$(du -h "$BACKUP_FILE" | cut -f1)
echo "$(date '+%Y-%m-%d %H:%M:%S') - Backup complete: $SIZE"
