# Prometheus Monitoring Integration

This document covers the Prometheus setup added to `docker_compose.yaml` for the
DA5402W MLOps recommendation pipeline (Kafka → Spark → DB, with MLflow and
Airflow for training/orchestration).

## Status

**Structurally verified, not yet functionally tested.**

- ✅ YAML parses cleanly (`docker compose config` / `yaml.safe_load`)
- ✅ No circular dependencies in the `depends_on` graph
- ⬜ Not yet confirmed: containers actually start, Prometheus actually scrapes
  successfully, Spark's metrics servlet responds as expected on your machine

Run through [Validation Steps](#validation-steps) below before trusting this in daily use.

## Design decisions

These were chosen deliberately, not defaults — see rationale for each:

| Decision | Reason |
|---|---|
| Bare Prometheus only, no Grafana/Alertmanager | Machine has 8GB RAM — every extra container competes for the same memory as Kafka/Spark/Airflow/MLflow. Prometheus's own UI at `:9090` is enough to see metrics without the added weight. |
| Only Kafka + Spark scraped for now | Airflow has no native Prometheus endpoint (needs a StatsD exporter add-on). MLflow has no native endpoint either (needs a separate scraper). Both are deferred until the core pipeline is confirmed working. |
| `kafka-exporter` included despite "no new sections" request | Kafka itself exposes zero Prometheus metrics — there's no way to monitor Kafka without something reading its JMX metrics and exposing them. This is the one exception, kept as small and single-purpose as possible. |
| `depends_on: prometheus` on every service | Starting *any one* of kafka, spark, mlflow, postgres, airflow-init, airflow-webserver, or airflow-scheduler auto-starts Prometheus with it — nobody has to remember a separate `docker compose up prometheus`. |
| No `condition: service_healthy` on the depends_on | Plain `depends_on` only guarantees start *order*, not full readiness. This is fine here — Prometheus retries scrape targets until they respond, so a few seconds of "target down" at boot is expected and harmless. |

## Files

```
docker_compose.yaml          # existing file, edited in place — no new compose files
configs/
  ├── prometheus.yml         # scrape config: kafka-exporter + spark-master only
  └── metrics.properties     # enables Spark's built-in Prometheus servlet
```

### What changed in `docker_compose.yaml`

- Added `prometheus` and `kafka-exporter` as new services (kept next to the
  `spark` block, not in a separate monitoring section)
- Added `depends_on: [prometheus]` to: `kafka`, `spark`, `mlflow`, `postgres`,
  `airflow-init`, `airflow-webserver`, `airflow-scheduler`
- Added `depends_on: [kafka]` to `kafka-exporter` (must start after the broker)
- Added a volume mount to `spark` for `configs/metrics.properties`
- Nothing else touched — ports, existing volumes, commands, and environment
  variables for every original service are unchanged

## Setup

1. Copy the `configs/` folder into your repo root, alongside `docker_compose.yaml`.
2. When running Spark jobs, include the metrics config flag:
   ```
   docker exec -it spark-master /opt/spark/bin/spark-submit \
     --conf spark.metrics.conf=/opt/spark/conf/metrics.properties \
     ...rest of your existing spark-submit command...
   ```
   Without this flag, Spark won't expose anything for Prometheus to scrape —
   Prometheus will just show that target as down, not error out.

## Validation steps

Run these in order. If any step fails, capture the exact error — that's a much
better basis for a fix than reasoning about the YAML in the abstract.

1. **Validate the compose file itself**
   ```
   docker compose config
   ```
   Should print the fully resolved config with no errors.

2. **Bring up the new pieces on their own first**
   ```
   docker compose up -d prometheus kafka-exporter
   docker compose ps
   ```
   Confirms the two new services work in isolation before mixing them into
   the existing working Kafka/Spark setup.

3. **Trigger it the intended way**
   ```
   docker compose up -d kafka
   docker compose ps
   ```
   Should show `kafka`, `kafka-exporter`, and `prometheus` all `Up` — even
   though only `kafka` was named on the command line.

4. **Check scrape targets**

   Open `localhost:9090/targets` in a browser.
   - `kafka` job → should go green (UP) within ~30 seconds
   - `spark-streaming` job → will show DOWN until you run a Spark job with the
     `metrics.conf` flag from step 2 above — that's expected, not a bug

5. **Confirm nothing else broke**
   ```
   docker compose up -d
   ```
   Verify Airflow webserver still comes up on `localhost:8080` and MLflow
   still comes up on `localhost:5000`, same as before these changes.

## Known limitations / not yet done

- **Airflow metrics**: not wired up. Would need a StatsD exporter sidecar
  (Airflow → StatsD → Prometheus) added to `docker_compose.yaml` and Airflow's
  `AIRFLOW__METRICS__STATSD_ON` config enabled.
- **MLflow metrics**: not wired up. MLflow has no built-in Prometheus endpoint;
  would need a custom exporter or scraping its REST API directly.
- **No alerting**: Alertmanager was intentionally left out. Query Prometheus
  directly via its UI/API for now (`localhost:9090`).
- **No dashboards**: Grafana was intentionally left out for the same RAM
  reason. Prometheus's own `/graph` page is functional for now.
- **`depends_on` is start-order only**: it does not guarantee Prometheus is
  *ready* to scrape by the time the target container is up. In practice this
  self-corrects within a scrape interval or two.

## Resource note (8GB RAM / limited disk)

If things feel sluggish with everything running at once:

```
# Stop Airflow's 4 containers when you're only working on Kafka/Spark/monitoring
docker compose stop postgres airflow-init airflow-webserver airflow-scheduler

# Bring them back only when you need to test the orchestration layer
docker compose start postgres airflow-webserver airflow-scheduler
```
