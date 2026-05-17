#!/bin/bash
# Prototype backup/restore tool for AnalyticsDB catalog.
# Currently supports SQLite and JSON catalog backends.

set -e

BACKUP_DIR="./analyticsdb-backup-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$BACKUP_DIR"

echo "AnalyticsDB Catalog Backup Tool"
echo "=============================="

# Backup SQLite catalog
if [ -f "analyticsdb-catalog.db" ]; then
    echo "Backing up SQLite catalog..."
    cp analyticsdb-catalog.db "$BACKUP_DIR/"
    echo "  -> $BACKUP_DIR/analyticsdb-catalog.db"
fi

# Backup JSON catalog
if [ -f "cluster-catalog.json" ]; then
    echo "Backing up JSON catalog..."
    cp cluster-catalog.json "$BACKUP_DIR/"
    echo "  -> $BACKUP_DIR/cluster-catalog.json"
fi

# Backup cluster config
if [ -f "cluster-config.json" ]; then
    echo "Backing up cluster config..."
    cp cluster-config.json "$BACKUP_DIR/"
    echo "  -> $BACKUP_DIR/cluster-config.json"
fi

echo ""
echo "Backup completed to: $BACKUP_DIR"

# Restore instructions
echo ""
echo "To restore:"
echo "  1. Stop AnalyticsDB nodes."
echo "  2. Replace catalog files with backups."
echo "  3. Restart nodes."
echo ""
echo "RPO (Recovery Point Objective): Last backup time."
echo "RTO (Recovery Time Objective): Time to replace files and restart (~minutes)."
