#!/bin/bash
# Backup script — run daily via cron
# Usage: ./scripts/backup.sh [/path/to/backup/dir]

set -euo pipefail
BACKUP_DIR="${1:-/opt/backup}"
DATE=$(date +%Y%m%d_%H%M%S)
DEST="$BACKUP_DIR/$DATE"
mkdir -p "$DEST"

echo "[$(date)] Starting backup to $DEST"

# 1. PostgreSQL dump
if docker compose ps -q postgres &>/dev/null; then
    echo "  Dumping PostgreSQL..."
    docker compose exec -T postgres pg_dump -U postgres instagram_booking > "$DEST/db.sql"
    gzip "$DEST/db.sql"
    echo "  PostgreSQL dump: done"
fi

# 2. Export Supabase CRM data (optional, requires SERVICE_KEY)
# curl -s -H "apikey: $SUPABASE_SERVICE_KEY" \
#   "https://rnjkilyiqnqiyqhwqdly.supabase.co/rest/v1/appointments?select=*&limit=5000" \
#   > "$DEST/supabase_appointments.json"

# 3. State files (Instagram poller)
for f in /opt/instagram-bot/data/*.json; do
    if [ -f "$f" ]; then
        cp "$f" "$DEST/"
        echo "  Copied $(basename "$f")"
    fi
done

# 4. Compress
cd "$BACKUP_DIR"
tar -czf "$DATE.tar.gz" "$DATE"
rm -rf "$DATE"
echo "  Packed: $DATE.tar.gz"

# 5. Cleanup old backups (keep 30 days)
find "$BACKUP_DIR" -name "*.tar.gz" -mtime +30 -delete
echo "  Cleaned backups older than 30 days"

echo "[$(date)] Backup complete: $DEST.tar.gz"
