# Demo 2 — Spark Connect: *"same code, laptop to AI agent"*

Spark 4.2's decoupled client/server (gRPC + Arrow). The client is a thin pure-Python wheel —
no local JVM, no local Spark. The DataFrame code is identical to what you'd run locally; only
the connection string moves. Then embed that session in an **AI tool** that answers questions
with Demo 1's governed metric.

## What it shows (both verified live against the sandbox 4.2 server)

- **`thin_client.py`** — connects over `sc://`, prints the *server's* Spark version
  (`5.0.0-SNAPSHOT`), runs ordinary `groupBy/agg` DataFrame code that executes remotely and
  streams back as Arrow. No local Spark in the process.
- **`ai_tool.py`** — a `MetricTool` holding a Connect session, wired as the kind of tool an LLM
  calls. It queries `delivery_metrics` (Demo 1) via `MEASURE()`, so the AI gets the *same*
  aggregation-safe numbers a dashboard would — governance follows the metric into the AI layer.

```
Q: What's our overall conversion rate?  → 0.0923   (not the 0.238 lie)
Q: How many active users this month?    → 9994
Q: Which region converts best?          → remote
```

## Project layout

```
02_spark_connect/
  pyproject.toml                 # thin client: pyspark[connect] only
  .env.example                   # -> .env (SPARK_REMOTE=sc://localhost:15099)
  src/spark_connect_demo/
    config.py                    # get_spark() — remote-only, one URL
    thin_client.py               # identical DataFrame code, executed remotely
    ai_tool.py                   # embed the session in an AI tool (reuses Demo 1's metric)
```

## Run it

```bash
cd demos/02_spark_connect
python -m venv .venv && source .venv/bin/activate
pip install -e .
cp .env.example .env

# make sure the sandbox 4.2 server is up (from spark_42_demos/):
#   docker compose -p spark42demos -f compose/docker-compose.yml up -d

python -m spark_connect_demo.thin_client
python -m spark_connect_demo.ai_tool        # needs Demo 1 to have created delivery_metrics
```

`ai_tool.py` depends on Demo 1 having created `delivery_metrics` on the server (run
`metrics_views_demo.run_footgun` once). It guards for that and tells you if it's missing.

## 4.2 Spark Connect improvements worth naming (from the announcement)

Better RDD-API compatibility, DataFrame inputs to `spark.read.*`, improved YARN cluster-mode.
The demo leans on the *embedding* story rather than these APIs; mention them as "and it's more
complete in 4.2" rather than building a whole beat around each.

## Reel script (3 min)

1. **Hook (0:00):** two terminals. Left: a local SparkSession. Right: `thin_client.py`. Same
   DataFrame code. "One of these has no Spark installed." Reveal it's the right one — just a URL.
2. **Thin client (0:45):** `pip show pyspark` weight vs a full Spark dist; print the *server's*
   version from the client to prove the work is remote.
3. **Embed (1:30):** `ai_tool.py` — "now put that session inside an AI assistant." Show the three
   Q&A answers coming back.
4. **Payoff (2:20):** the AI's conversion rate is 9.2%, not the 24% lie — because it called the
   Demo 1 metric view. "Your semantic layer just governed your AI." Ties the series together.
