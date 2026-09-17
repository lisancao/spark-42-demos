# Spark Connect — Claude design handoff (video visuals)

Handoff for generating the on-screen visuals for a Spark Connect video. It favors **many small,
single-idea visuals over a few complex ones**: each card below teaches exactly one fact, is trivially
renderable, and carries a ready-to-paste generation prompt. Every code snippet, number, config value,
RPC name, and JIRA id is transcribed verbatim from this repo with a `file:line` (or `blog §`)
provenance tag, so nothing has to be invented and each visual can be checked against the source.

- **Target reader:** general data-engineering audience. Accurate, not pedantic.
- **Source of truth:** Apache Spark 4.2.0, `demos/02_spark_connect/`. Blog = `blog_spark_connect.md`.
- **Format:** 1920×1080 (16:9), motion. Default to the dark surface; code-only cards may use light.
- **Hard rule:** do not invent a symbol, number, file, or call edge. If a card needs a detail not
  tagged here, leave it out or ask.

---

## Global design system

### Canvas
1920×1080, 96px safe margins. **One idea per card.** Big type; the code or the single fact is the
subject. Two surfaces, both hand-built: light `#fcfcfb`, dark `#1a1a19`.

### Color roles (fixed across every card — never repaint a role)
| Role | Light | Dark |
|---|---|---|
| **Client** (JVM-free Python) | `#2a78d6` | `#3987e5` |
| **Server / driver** (Connect server) | `#eb6834` | `#d95926` |
| **Executors** | `#1baf7a` | `#199e70` |
| **Plan** (protobuf, client→server) | `#4a3aa7` | `#9085e9` |
| **Arrow** (results, server→client) | `#eda100` | `#c98500` |
| **Removed / Classic-only / error** | `#e34948` | `#e66767` |
| **Added / OK / new-in-4.2** | `#008300` | `#008300` |

Rules: never color-alone (every colored mark also has a text label / icon, grayscale-legible); text
uses ink tokens (`#0b0b0b`/`#ffffff` primary, `#52514e`/`#c3c2b7` secondary), not the role color; a
persistent Client/Server/Executors legend appears on any card that shows more than one role.
Type: monospace for code (keep exact wording, indentation, casing, field numbers), humanist sans for
labels, sentence case, no emoji, no decorative arrows in text.

### Five reusable card templates
Every catalog card names one of these, so the generator stays consistent and each stays simple.

- **T1 · Mini-diagram** — 2–4 labeled nodes + directional edges. Client left, server center,
  executors right. Animate flow along edges (plan violet →, Arrow yellow ←).
- **T2 · Code-card** — one mono code block on a rounded card, a title, and a dimmed `file:line`
  caption. Reveal by logical line group.
- **T3 · Before/after** — two mono panels side by side; removed lines get red rail + `-`, added get
  green rail + `+`, unchanged stay ink. Caption = the one-line takeaway.
- **T4 · Stat card** — one big number + a short label + a dimmed source. Optionally a tiny sparkline
  or bar. For real comparisons use a proper bar (see the dataviz note).
- **T5 · Gotcha card** — a red header strip "Silent / Surprising", the trigger (small mono), the
  verbatim error text, and a one-line "why". These are the backbone of the gotchas reel.

### Motion
Build, don't dump. Reveal one node / one line group at a time. Animate direction of data flow. Hold
the final frame on the single concrete fact (a number, a signature, an error string). Consistent
camera: client left, server center, executors right, everywhere.

### dataviz note (for the few real charts)
Only the footprint comparison (C-3) and any quantitative bar are true charts — build those per the
`dataviz` method: one hue light→dark for magnitude, a 2px surface gap between fills, direct labels,
a table view, validated in light and dark. Everything else here is a diagram or a code card, not a
chart.

---

## The atomic visual catalog

Grouped by theme. Each card: **ID · title** — purpose · template · grounded content · prompt.

### A. Architecture foundations

**A-1 · Classic: one host** — why coupling exists. · T1 ·
Content: one host box containing "Python" (client blue) linked by a short "Py4J" connector to "Driver
JVM" (orange), which fans down to 3 "Executor" nodes (aqua). Source: blog §1, `blog:37`.
Prompt: *T1 mini-diagram, dark. One host box titled "Spark Classic": Python (blue) —Py4J— Driver JVM
(orange) — 3 Executors (aqua). Caption "one process lifetime, one host". Provenance `blog §1`.*

**A-2 · Connect: split across a boundary** — the core change. · T1 ·
Content: "Python (no JVM)" (blue) on its own host; a dashed host boundary; a gRPC channel to "Connect
server = driver" (orange) → 3 executors (aqua). Source: blog §1, `blog:43`.
Prompt: *T1, dark. Left host: "Python — no JVM" (blue). Dashed vertical host boundary. gRPC channel
labeled "sc://host:15002" to "Connect server = driver" (orange) → 3 executors (aqua). Legend.*

**A-3 · The session lives on the server** — the one fact everything follows from. · T4 ·
Content: big line "The session lives on the server; the client holds a reference." Sub: catalogs,
config, names all resolve server-side. Source: `blog:116`.

**A-4 · Plan in, Arrow out** — the whole protocol in four steps. · T1 (vertical flow) ·
Content, verbatim `blog:108–111`:
```text
DataFrame transformations build a protobuf plan
  └── ExecutePlan sends the plan over gRPC
        └── server resolves, optimizes (Catalyst), executes
              └── Arrow batches stream back to the client
```
Prompt: *T1 vertical 4-step flow, dark. Step 1 client builds a protobuf "plan" tile (violet); step 2
"ExecutePlan / gRPC" arrow right to server (orange); step 3 server inner stages "resolve → Catalyst →
execute"; step 4 yellow "Arrow batches" stream left to client. Reveal one step at a time.*

**A-5 · Only result rows cross the wire** — data movement is minimal. · T4 ·
Content: "Only result rows travel to the client, as Arrow." Source `blog:967`. Pair with C-8's 5M→7.

**A-6 · What Connect gives up** — the trade. · T2 ·
Content: red chips `SparkContext`, `RDDs`, `direct driver-JVM access`. Source `blog:284`.

### B. The protocol (code-heavy)

**B-1 · DataFrame ops are protobuf** — the mapping. · T2 ·
Content, verbatim `Relation` message `blog:204–218` (Read/Project/Filter/Join/Aggregate/Sort/Limit
with the `// spark.read / .select() / …` comments). Prompt: *T2 code-card of the exact protobuf; then
draw a thin line from each field to its DataFrame method on the right. Reveal pairs one at a time.*

**B-2 · Twelve RPCs** — the whole service is small. · T4+grid ·
Content: big "12", then a 12-chip grid grouped: Plan (`ExecutePlan`, `AnalyzePlan`) · Config
(`Config`) · Artifacts (`AddArtifacts`, `ArtifactStatus`) · Execution lifecycle (`Interrupt`,
`ReattachExecute`, `ReleaseExecute`, `GetStatus`) · Session (`ReleaseSession`, `CloneSession`) ·
Errors (`FetchErrorDetails`). Source Table 2-2 `blog:221–245`. Make `ExecutePlan` largest; tag
`GetStatus` "new in 4.2".

**B-3 · Spotlight: ExecutePlan** — the one that does the work. · T4 · "Execute a plan; results stream
back as Arrow." `blog:228`.
**B-4 · Spotlight: ReleaseExecute** — why buffered results get discarded. · T4 · `blog:246`.
**B-5 · Spotlight: FetchErrorDetails** — why a JVM-less client shows a JVM stack trace. · T4 · `blog:245`.

**B-6 · Run a query in 4 messages** — the client core. · T1 vertical · Content, from `blog:12`§12
steps: (1) build a `Plan` (Relation or Command); (2) send `ExecutePlanRequest` (plan + session_id +
user_context, optional operation_id); (3) read `ExecutePlanResponse` stream — rows in `arrow_batch`,
plus `schema`/`metrics`; (4) finish on `result_complete`, else `ReattachExecute` then `ReleaseExecute`.

**B-7 · Reattach after a drop** — resilience. · T1 · client sends `ExecutePlan`; edge breaks mid Arrow
stream; server (orange) stays lit; client `ReattachExecute` by operation ID; stream resumes.
Source `blog:250`, SPARK-55406.

**B-8 · Plan compression is negotiated** — not fixed. · T2/T4 · "threshold + algorithm fetched from
the server; zstandard only if installed; disabled for the session if the server can't read a
compressed plan." Source `blog:255`.

**B-9 · explain() prints the server's plan** — no client optimizer. · T2 · verbatim physical-plan
snippet (`AdaptiveSparkPlan … HashAggregate…`), `blog` §2 "Observing the Division of Labor".

**B-10 · The six protocol files** — what a client implements. · T2 small table · Table 12-1
`blog:~1244`: `base.proto` (RPCs, Plan), `relations.proto` (Relation tree), `expressions.proto`,
`commands.proto`, `types.proto`, `catalog.proto`.

### C. Install & footprint (stat-card heavy)

**C-1 · Four look-alike distributions** — only one has no JVM. · T2 small table · Table 3-1
`blog:~300`: `pyspark` (481 MB, 456 MB JARs, Py4J, no remote), `pyspark[connect]`, `pyspark-connect`
(metapackage → installs full), `pyspark-client` (19 MB, no JARs, no Py4J, remote only).
**C-2 · pyspark-client** — the JVM-free one. · T4 · "19 MB · no JARs · no JDK · sc:// only". `blog:~305`.
**C-3 · Where the space goes** — the real footprint (TRUE CHART, dataviz). · stacked bar · env sizes:
`pyspark[connect]` 820 MB vs `pyspark-client` 359 MB; components pyarrow 154 / pandas 65 / numpy 41 MB.
Source Table 3-2 / fig3-1 `blog:~330`. Prompt: *dataviz stacked bar, one hue ramp, two bars 820 vs
359 MB, segments JARs / pyspark/ / deps; direct-label MB; table view; light+dark.*
**C-4 · 96% / 100%** — the reduction. · T4 · "pyspark/ 481→19 MB (96%); Spark JARs 456→0 MB (100%)."
**C-5 · Not actually small** — the honest caveat. · T4 · "pyarrow+pandas+numpy = 260 MB of required
deps, so the client env is 359 MB, not tiny." `blog:337`.
**C-6 · Which am I running?** — the structural test. · T2 · verbatim: no `pyspark/jars` dir + `py4j`
find_spec is False. `blog:~360`.

### D. Server & connection

**D-1 · Start the server** — one script. · T2 · `./sbin/start-connect-server.sh` (+ untar). `blog §4`.
**D-2 · --wait in a container** — or it daemonizes and the container exits. · T4 · "`--wait` sets
`SPARK_NO_DAEMONIZE=1`". `blog §4`.
**D-3 · Connection string anatomy** — the parts. · T1/annotation · `sc://host:port/;name=value` with
scheme, host, port (default 15002), empty path, `;`-separated params. fig5-1 `blog §5`.
**D-4 · Connection params** — the seven. · T2 small table · Table 5-1: `token` (enables TLS),
`use_ssl` false, `user_agent` `_SPARK_CONNECT_PYTHON`, `session_id` random UUID, `grpc_max_message_size`
134217728 (128 MiB). `blog §5`.
**D-5 · api.mode selects the path** — classic | connect. · T1 · fig6-1: one unchanged `job.py`;
`classic` → Py4J+driver; `connect` → Spark starts a Connect server. `blog §6`, `blog:539`.
**D-6 · Server config defaults** — the grpc.* knobs. · T2 small table · Table 4-1 `blog:410–415`:
`binding.address` (none; all interfaces / 4.0.0), `binding.port` 15002, `port.maxRetries` 0,
`maxInboundMessageSize` 134217728, `arrow.maxBatchSize` 4m.

### E. Security beats

**E-1 · The default binds everything** — the risk. · T3 boundary · State A: `binding.address` unset →
open red boundary "all interfaces". State B: `binding.address=127.0.0.1` → closed green boundary.
Verbatim config `start-connect-server.sh:15,29`; note `TOPOLOGIES.md:168`.
**E-2 · No built-in auth** — run behind a proxy. · T4 · "Spark Connect has no built-in authentication;
intended to run behind an authenticating gRPC proxy. Do not expose port 15002 directly." `blog §11`/§9.
**E-3 · A token enables TLS** — bearer never sent in the clear. · T4 · `blog §5`.
**E-4 · SPARK-57336: the 401** — a stale Scala client. · T5 gotcha · "older Scala client sent its
token in an `Authentication` header, not `Authorization` → 401 behind a proxy. Fixed 4.2.0/4.1.3/4.0.4."
`blog §9`.

### F. Topologies & the wrong-server trap

**F-1 · Six topologies** — same client code everywhere. · T2 small table · Table 4-2 `blog §4`:
in-process, same-host, remote standalone, Kubernetes, YARN cluster (SPARK-55239), multi-cluster.
**F-2 · Port 15002 collision (STRONG gotcha)** — you silently hit the WRONG server. · T5 gotcha ·
Verbatim `TOPOLOGIES.md:99–140`: set `binding.port=15014`, but the `local[*]` client still connects
to `localhost:15002`, lands on an unrelated **Spark 4.1.0** server, **no error raised** — the test
suite runs against the wrong Spark version. Prompt: *T5, dark. Client (blue) arrow to port 15002
holding a stray "4.1.0" server (orange, red-outlined); the intended 15014 server sits idle beside it;
big label "no error raised".*
**F-3 · doctor.py catches it** — verify the version. · T2 · `python tools/doctor.py --remote
sc://localhost:15002 --expect-version 4.2` → `FAIL`, exit 1. `TOPOLOGIES.md:140`.
**F-4 · Load-balancing breaks sessions** — affinity or one pod per tenant. · T1 · K8s Service across
2 replicas; a client's session lives on pod A; a request routed to pod B misses it. `blog §4`;
ClusterIP vs LoadBalancer `TOPOLOGIES.md:237`.

### G. API differences (§7)

**G-1 · The unsupported surface** — enumerable, not vague. · T2 · red chips: `_jvm`, `_jsc`,
`sparkContext`, `newSession`, `.rdd`, plus `setLogLevel`/`addPyFile`/broadcast vars/accumulators via
`SparkContext`. `blog §7`.
**G-2 · enableHiveSupport() is silent (gotcha)** — it doesn't raise. · T5 · verbatim
`[CANNOT_MODIFY_STATIC_CONFIG]` UserWarning; `catalogImplementation` stays `in-memory`. `blog §7`.
**G-3 · Observation, not accumulators** — for aggregate metrics only. · T2 · verbatim `Observation("rows")`
+ `df.observe(...)` + `obs.get["n"]` (a property, indexed). `blog §7`.
**G-4 · broadcast: hint vs variable** — one works, one doesn't. · T2 2-row · `functions.broadcast(df)`
Supported; `SparkContext.broadcast(value)` Unavailable. Table 7-1 `blog §7`.
**G-5 · getOrCreate returns the cached session (gotcha)** — even with a different remote. · T5 ·
verbatim `a=…remote("sc://good…"); b=…remote("sc://127.0.0.1:1").getOrCreate(); assert b is a` passes.
Use `builder.create()`. `blog §7`.
**G-6 · New in 4.2** — gaps Connect closed. · T2 list · `zipWithIndex`, `toJSON`, `read.json/xml/csv`
accept a DataFrame, `emptyDataFrame(schema)`, `head/take/tail` avoid full scans. Table 7-2 `blog §7`.

### H. Eager vs lazy (§8) — silent behavior changes

**H-1 · What moved from eager to lazy** — the table. · T2 small table · Table 8-1: schema analysis
Eager→Lazy; schema access Local→remote `AnalyzePlan`; temp views plan→name; UDF at def→at exec. `blog §8`.
**H-2 · Temp view resolves by name (gotcha)** — replacing it changes past DataFrames. · T3/T5 ·
verbatim: `first=spark.table("brands")` then replace `brands` with `b`; `first.count()` = a (Classic)
vs b (Connect). fig8-1 `blog §8`.
**H-3 · UDF captures at execution (gotcha)** — `123` vs `456`. · T5 · verbatim closure example. `blog §8`.
**H-4 · Deferred analysis error escapes the try (gotcha)** — no exception at the transformation. · T5 ·
verbatim `df.filter(col("typo")>6)` raises later, not in the `try`. `blog §8`.
**H-5 · Schema access = a round trip** — fix the loop. · T3 · before: `if name not in df.columns`
inside a loop (one `AnalyzePlan` each); after: read `df.columns` once + single `select`. `blog §8`.

### I. Arrow / UDF traps

**I-1 · ModuleNotFoundError: pyarrow (gotcha)** — UDFs fail on the executor. · T5 · Spark 4.2 enables
Arrow UDFs by default (SPARK-54555); official image lacks PyArrow → every Python UDF fails, while
`toPandas()` still works. `blog:732–739`.
**I-2 · Three fixes** — pick one. · T2 · `udf(..., useArrow=False)` (one UDF) · set
`spark.sql.execution.pythonUDF.arrow.enabled=false` (UDFs after) · add PyArrow to the image
(all UDFs — this project's Dockerfile: pyarrow 25.0.1, pandas 2.3.3). `blog:736–739`.
**I-3 · ArrowDeserializer binds by position** — duplicate column names. · T4 · SPARK-56007, `blog §7`.
**I-4 · Python minor versions must match (gotcha)** — client vs executors. · T5 · verbatim
`[PYTHON_VERSION_MISMATCH] … 3.10 than that in driver: 3.12`; image ships Python 3.10.12. `blog §7`.

### J. Migration (§9) — the code-heavy centerpiece (one card per layer)

**J-0 · The seven layers (map)** — one small table. · T2 · Table 9-1 `blog:892`: 0 session · 1 logic ·
2 sources/catalogs · 3 config · 4 deps · 5 exec/ops · 6 security.
**J-L0 · Layer 0 — session creation.** · T3 · before `SparkSession.builder.appName("orders-pipeline").getOrCreate()`
(`pipeline_before.py:20`) → after `builder.remote(remote).getOrCreate()` (`pipeline_after.py:26`).
Callout: "may need no code change with `spark.api.mode=connect`" `blog:877`.
**J-L1 · Layer 1 — pipeline logic (audit RDD).** · T3 · before `if events.rdd.isEmpty():`
(`pipeline_before.py:30`) → after `if events.isEmpty():` (`pipeline_after.py:38`).
**J-L2 · Layer 2 — sources & catalogs (paths resolve on the server).** · T3 · before
`INPUT = str(HERE / "input")` (`:13`) → after `INPUT = os.environ.get("PIPELINE_INPUT", "/demo/pipeline/input")` (`:15`).
**J-L3 · Layer 3 — config splits.** · T3 · before `.config("spark.sql.shuffle.partitions","8").enableHiveSupport()`
(`:22`) → after `spark.conf.set("spark.sql.shuffle.partitions","8")` (`:34`); server-only:
`spark.sql.extensions`, `spark.jars`, `spark.sql.catalog.iceberg`.
**J-L4 · Layer 4 — dependencies → addArtifact.** · T3 · before `spark.sparkContext.addPyFile(...geo.py)`
(`:27`) → after `spark.addArtifact(...geo.py, pyfile=True)` (`:35`).
**J-L5 · Layer 5 — execution & ops.** · T3 · before the `docker exec … spark-submit /scripts/_load_to_iceberg.py`
wrapper (`blog:1081`) → after `SPARK_REMOTE=sc://localhost:15002 python pipeline.py` (`blog:1088`).
**J-L6 · Layer 6 — security.** · T4 · "clients no longer need catalog or storage credentials; keep
catalog creds on server/driver, give executors only the storage access they need." `blog §9`.
**J-1 · Order of operations** — reversible first, credentials last. · T1 timeline · steps: (1) L2+L3
server+preflight, (2) L0 point one job with `spark.api.mode` [reversible], (3) L1 audit, (4) L4
addArtifact, (5) L5 logs/checkpoints, (6) L6 remove creds [not easily reversed, red]. `blog:~1130`.
**J-2 · client/server boundary** — what a client can and can't change. · T1 · fig9-1 `blog §9`:
`shuffle.partitions`→session; `addArtifact`→executors PYTHONPATH; `spark.sql.extensions` refused
(`CANNOT_MODIFY_STATIC_CONFIG`); `spark.jars` can't change classpath.
**J-3 · One line of 357 (stat)** — how little changes. · T4 · "1 of 357 lines fails: `setLogLevel`";
"21 blocking findings across 24 files, all the same call." `blog §9`.
**J-4 · preflight() (code)** — fail before writing if the catalog is missing. · T2 · verbatim
`preflight()` (`SHOW NAMESPACES` → `SystemExit`). `blog §9` / `pipeline_after.py:93`.

### K. Upgrade 4.1 → 4.2 (§10) — keep separate from the Connect move

**K-1 · Upgrade hazards** — the table. · T2 small table · Table 10-1: PyArrow 15.0.0→18.0.0;
`arrow.pyspark.enabled` default true; `pythonUDF.arrow.enabled` default true (SPARK-54555);
`pythonUDTF.arrow.enabled` true; PyPy dropped. `blog:1160`.
**K-2 · JDK 25 (gotcha)** — pre-4.2 client crashes. · T5 · "Arrow/Netty allocator failure on JDK 25;
`ExceptionInInitializerError` in `EmptyByteBuf.memoryAddress`; JDK 17/21 unaffected." SPARK-56955 `blog §11`.

### L. Case study — the worked pipeline

**L-1 · 357 lines · 10 tables (stat + medallion)** — the shape. · T4 · big "10 Iceberg tables";
medallion 5 bronze / 2 silver / 3 gold: bronze dim_categories/dim_brands/dim_items/dim_locations/orders;
silver orders_enriched/order_lifecycle; gold hourly_metrics/delivery_performance/brand_summary
(`case_study/lakehouse_stack/pipeline_after.py:257–423`).
**L-2 · Pipeline.run() call tree (verified)** — one real flow. · T2 tree · verbatim, grounded at
`pipeline_after.py:191`:
```text
Pipeline.run()                                   # :191
├── preflight(self.spark, self.catalog)          # :194
├── self._get_execution_order()                  # :195  topological sort from spark.table() deps
└── for table in order:
      ├── df = info['func']()                     # the @materialized_view function
      ├── df.write.mode("overwrite").saveAsTable(full_name)   # :222
      └── SELECT COUNT(*) … report row count      # :224
```

### M. Multi-language clients (§12)

**M-1 · Anatomy of any client** — plan builder → transport → decoder. · T1 · fig12-1 `blog §12`:
DataFrame calls → plan builder (Relation/Expression) → gRPC transport (session id, retries, reattach)
→ Connect server; Arrow back → decoder → RecordBatches → Polars.
**M-2 · Five clients today** — any language, no JVM. · T2 small table · Table 12-2: Python, Scala/Java,
Go v0.1.0, Swift 0.7.0, Rust 4.2.0.
**M-3 · Rust + Polars: 5,000,000 → 7** — divide by size. · T1 flow · Spark aggregates 5M rows to 7
groups; result crosses as Arrow IPC; becomes a Polars DataFrame locally. `blog §12`; run 0.26–0.34 s.
**M-4 · Version trap by another name (gotcha)** — a matching version number that still fails. · T5 ·
Rust crate labeled 4.2.0 includes 4.3.0's `Zip` relation (SPARK-57247): `DataFrame::zip` compiles,
but a 4.2.0 server rejects the plan — verbatim `[CONNECT_INVALID_PLAN.INVALID_ONE_OF_FIELD_NOT_SET] …
RELTYPE_NOT_SET`. "A successful build is not evidence the server supports a method." `blog §12`.

---

## Suggested video reels (sequence the atomic cards)

Pick a reel and play its cards in order; each is a fast single-idea beat.

- **Reel 1 — What & why (90s):** A-1 → A-2 → A-3 → A-4 → A-5 → A-6.
- **Reel 2 — The protocol (deep, code):** B-1 → B-2 → B-3/B-4/B-5 → B-6 → B-7 → B-8 → B-9 → B-10.
- **Reel 3 — The gotchas reel (all T5, high engagement):** F-2 → G-2 → G-5 → H-2 → H-3 → H-4 → I-1 →
  I-4 → K-2 → M-4 → E-4.
- **Reel 4 — Install & footprint:** C-1 → C-2 → C-3 → C-4 → C-5 → C-6.
- **Reel 5 — Migration (code centerpiece):** J-0 → J-L0 → J-L1 → J-L2 → J-L3 → J-L4 → J-L5 → J-L6 →
  J-1 → J-2 → J-3 → J-4 → L-1 → L-2.
- **Reel 6 — Operate it:** D-1 → D-2 → D-6 → E-1 → E-2 → F-1 → F-3 → F-4.
- **Reel 7 — Any language:** M-1 → M-2 → M-3 → M-4.

## Asset checklist for the generator
- [ ] One idea per card; big type; hold on the single concrete fact.
- [ ] Role colors fixed across all cards (Client blue, Server orange, Executors aqua, Plan violet,
      Arrow yellow, Removed/error red, Added/new green); never color-alone; text in ink tokens.
- [ ] Code verbatim (wording, indentation, casing, field numbers); each code/gotcha card shows its
      `file:line` or `blog §` caption.
- [ ] Light + dark rendered; dark is the video default.
- [ ] Only C-3 (and any real quantitative bar) is a chart — build it per the dataviz method.
- [ ] No invented file, symbol, count, or call edge — everything traces to a tag here.

## Provenance index
- `demos/02_spark_connect/blog_spark_connect.md` (all 12 sections; figures fig1-1…fig12-1; Tables
  2-1…12-3)
- `demos/02_spark_connect/pipeline/pipeline_before.py`, `pipeline/pipeline_after.py`
- `demos/02_spark_connect/case_study/lakehouse_stack/pipeline_after.py`
- `demos/02_spark_connect/setup/standalone/start-connect-server.sh`, `setup/TOPOLOGIES.md`,
  `setup/kubernetes/connect-server.yaml`

> `§` = blog section; `blog:N` = line N of `blog_spark_connect.md` at time of writing (a few marked
> `~` are approximate lines with verbatim text). Re-confirm against the working tree before final
> render.
