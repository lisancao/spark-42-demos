# Spark 4.2 demos

A self-contained workspace for 5 Apache Spark 4.2 demos. Its Docker network, ports, volumes, and
Compose namespace are isolated from the live `~/lakehouse-stack` infrastructure and
`~/repos/safe-spark-agents`.

```text
spark-42-demos/
├── demos/01_metrics_views/   # Metric definitions evaluated at the query's grain
└── demos/02_spark_connect/   # Decoupled clients, servers, and pipeline migration
```

## Containment contract

| Concern   | This sandbox                             | Avoided collision                          |
|-----------|------------------------------------------|--------------------------------------------|
| Network   | bridge `spark42demos_net`                | lakehouse-stack runs on **host** network   |
| Ports     | Connect `15099`, UI `8190` (localhost)   | 8070–8087, 7078, 15002 all in use          |
| Volumes   | `spark42demos_*`                         | never shares lakehouse-stack volumes       |
| Project   | `-p spark42demos`                        | own compose namespace                      |
| Image     | `lakehouse/spark:5.0.0-snapshot-cdc`     | reused read-only; no rebuild of shared img |

The stack is parked. Bringing it up is opt-in and never part of setup:

```bash
cd ~/Documents/spark_content/spark_42_demos
docker compose -p spark42demos -f compose/docker-compose.yml up -d      # start
docker compose -p spark42demos -f compose/docker-compose.yml down -v    # full teardown
```

## The 5 demos

The brainstorm and rationale live in Obsidian:
`obsidian_vault/spark_content/Spark 4.2 — 5 Demos Brainstorm.md`

| # | Demo                    | Feature                        | Priority |
|---|-------------------------|--------------------------------|----------|
| 1 | Metrics Views           | Native semantic layer          | **#1**   |
| 2 | Spark Connect           | Decoupled client / embed in AI | 2        |
| 3 | New Spark SQL           | QUALIFY, time_bucket, cursors  | 3        |
| 4 | RTM in PySpark          | ms-latency stateless streaming | 4        |
| 5 | Vector Search / NEAREST BY | Top-K similarity retrieval  | 5 (rec.) |

Each `demos/NN_*/` dir is a stub to be filled once the lineup is locked.

> No stable Spark 4.2 image exists yet (as of 2026-07-20). The snapshot image carries the
> 4.2/5.0 features; swap `SPARK_IMAGE` in `compose/.env` when an official image ships.
