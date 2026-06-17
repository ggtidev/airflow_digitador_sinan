---
name: docker-airflow
description: Use when managing Docker Compose services, Airflow DAGs, container operations, or deployment. Covers docker-compose.yaml, airflow.cfg, init scripts, and production deploy guide.
---

# Docker & Airflow Management

## Services (`docker-compose.yaml`)

```yaml
airflow-postgres    # postgres:15 — Airflow metadata DB (healthcheck: pg_isready)
airflow-redis       # redis:7 — Celery backend
airflow-init        # apache/airflow:2.8.4 — DB migrate + user create (runs airflow-init.sh)
airflow-webserver   # apache/airflow:2.8.4 — UI on port 8081
airflow-scheduler   # apache/airflow:2.8.4 — Task scheduler
postgres_sinan      # postgres:16 — SINAN notifications DB on port 5433
```

**Network:** `airflow-net` (external, name: `prod`)
**Executor:** LocalExecutor
**Volumes:** `airflow-postgres-db`, `sinan_pgdata`

## Common Operations

```bash
# Start all services
docker compose up -d

# Initialize Airflow (first time)
docker compose up airflow-init

# View logs
docker compose logs -f airflow-scheduler
docker compose logs -f airflow-webserver

# Stop all
docker compose down

# Restart specific service
docker compose restart airflow-webserver

# Reset sinan_api database
docker compose down
docker compose rm -f postgres_sinan
docker volume rm airflow-docker_sinan_pgdata
docker compose up -d postgres_sinan
```
