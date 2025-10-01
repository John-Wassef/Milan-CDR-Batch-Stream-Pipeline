#!/bin/bash
set -e

# Initialize DB (migrate ensures proper setup)
airflow db migrate

# Create default admin user (if not exists)
airflow users create \
  --username admin \
  --firstname John \
  --lastname Wassef \
  --role Admin \
  --email admin@example.com \
  --password admin || true

# Run scheduler in background
airflow scheduler &

# Run webserver in foreground
exec airflow webserver --port 8080
