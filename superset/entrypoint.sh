#!/bin/bash
set -e

echo "Running migrations..."
superset db upgrade

echo "Creating admin user..."
superset fab create-admin --username admin --firstname simon --lastname jaramillo --email admin@admin.com --password admin || true

echo "Starting Superset..."
superset run -h 0.0.0.0 -p 8088
superset init