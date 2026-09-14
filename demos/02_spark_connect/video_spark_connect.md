# Video: Spark Connect in Apache Spark 4.2: Architecture, Setup, and Migration

**Alt Title:** Spark Connect in Apache Spark 4.2: From First Server to Migrated Pipeline
**Target Audience:** Platform and data engineers running self-hosted Spark who know of Spark Connect but have not yet migrated a workload to it
**Estimated Runtime:** 41–43 minutes
**Tone:** Technical and measured; a senior engineer presenting to peers. Assumes Spark fluency. Every figure on screen comes from a measured run against Spark 4.2.0.
**Spark Version:** 4.2.0 (released 2026-07-14)

---

## VIDEO OVERVIEW

The video explains why Spark Connect exists and how it works, then runs two demos against a real
stack.

**Demo A** covers setup in two parts: choosing a topology (six of them, from a server inside the
Python process to a Kubernetes Deployment) and starting a server; then validating it with a `doctor`
command and short examples that check what the server actually does.

**Demo B** migrates a small pipeline from Spark Classic to Spark Connect, with one change for each
migration layer, and verifies with a parity check that the output data is unchanged. The migration
of a 357-line production pipeline is summarized from the written case study.

Between the demos, the organizing idea: a Connect migration consists of seven independent layers, of
which only one is mandatory.

Everything runs against a Spark 4.2.0 cluster built from the official image, with
`pyspark-client==4.2.0` on Python 3.10. Every measurement on screen can be reproduced with a `make`
target or a run configuration in the companion project.

**Blog post:** [Spark Connect in Apache Spark 4.2: How It Works and How to Adopt It](blog_spark_connect.md)
**Demo project:** `spark_42_demos/demos/02_spark_connect/`. Open it as an IDE workspace; `launch.json`
lists the run configurations in video order.

### Reading This Script

This script is formatted for a teleprompter.

- **Spoken text** is in the quoted paragraphs, one breath-length paragraph per line. Nothing else is
  read aloud. Identifiers and numbers are written the way they are said; see the pronunciation guide
  in the production notes.
- **CUE** lines are presenter actions: run a command, pause on output, switch windows.
- **VISUAL** and **ON SCREEN** material is for the editor. Tables and code under ON SCREEN appear on
  screen but are never read.
- `video_spark_connect_teleprompter.txt` contains only the spoken text and cues, ready to load into a
  teleprompter app. Regenerate it after any edit with `make teleprompter`.

---

## STORYBOARD

### CHAPTER 0. COLD OPEN (0:00 - 1:30)

**VISUAL:** Two IDE windows side by side, `examples/01_classic_or_connect.py` open in both. Left, on a machine with a JDK, runs launch configuration 1 (Spark Classic) and prints a DataFrame. Right, on a machine without Java, runs launch configuration 2 (Spark Connect) and prints the same DataFrame. Zoom on the right window's terminal: `java: command not found`.

**CUE:** ROLL SIDE-BY-SIDE IDE FOOTAGE

> Two machines. The same file. The same output.
>
> One of these machines doesn't have Java installed. It doesn't have Spark installed either.
>
> It has a Python package of one point six megabytes, and a URL.
>
> That's Spark Connect.
>
> In this video, we'll look at why it exists and how it works.
>
> Then we'll stand up a server, migrate a pipeline onto it, and check that the migration didn't change a single row of output.

**CUE:** TITLE CARD

---

### CHAPTER 1. WHY CONNECT EXISTS, AND HOW IT WORKS (1:30 - 5:30)

**VISUAL:** `14_driver_coupling_and_timeline.svg`: left half: a PySpark application and its driver JVM on one host, joined by Py4J; right half: a client with no JVM connected over gRPC to a Connect server.

**SECTION 1A. THE DRIVER-COUPLED ARCHITECTURE**

**CUE:** SHOW DRIVER COUPLING DIAGRAM

> For most of Spark's history, an application and its Spark driver have been tied together.
>
> In PySpark, that's literal. Your Python process starts a driver JVM, and calls into it through a bridge called Py4J.
>
> So the application has to run close to the cluster. It has to carry a full Spark runtime and a JDK. And it shares the driver's lifetime.
>
> In June of twenty twenty-two, Martin Grund filed the Spark Improvement Proposal for Spark Connect.

**CUE:** SHOW SPARK-39375

> It names four limitations of that design.
>
> First, remote access. Apart from SQL, there was no built-in way to connect to a cluster remotely.
>
> Second, developer experience. The architecture didn't suit notebooks or modern code editors.
>
> Third, stability. On a shared driver, one user's out-of-memory error could take the cluster down for everyone.
>
> And fourth, upgrades. Client and platform dependencies shared one classpath, so you couldn't upgrade Spark independently of the applications using it.

**SECTION 1B. THE DESIGN DECISION**

> The proposal's answer was to separate the client from the driver, along a line Spark already had. The DataFrame API.
>
> A DataFrame program doesn't compute anything as you write it. It builds a plan.
>
> Spark Connect sends that plan, unresolved, to a server.
>
> The server resolves it, optimizes it with Catalyst, runs it, and streams the results back as Arrow.

**CUE:** SHOW TIMELINE

> Spark Connect shipped as a Python client in Spark three point four, in twenty twenty-three.
>
> Spark three point five added Scala and Go clients.
>
> Spark four point oh added the pure-Python package, pyspark dash client, and a setting called spark dot api dot mode.
>
> And Spark four point two, which is what we're running today, is largely about operating it in production.

**SECTION 1C. THE SESSION LIVES ON THE SERVER**

**CUE:** SHOW PLAN-IN, ARROW-OUT DIAGRAM

> Here's the most useful fact to hold on to.
>
> The Spark session lives on the server. The client just holds a reference to it.
>
> Most practical questions follow from that.
>
> Where are catalogs configured? On the server, because that's where names get resolved.
>
> How does the client know what tables exist? It asks the server.
>
> Which settings can a client change? The ones the server reads at run time. Values, not class names.

**ON SCREEN:**

```python
df = spark.table("iceberg.silver.orders_enriched")
```

> Take this line. The client doesn't know whether that table exists, or what columns it has.
>
> It sends the name, and the server resolves it.
>
> That's why a client with no Iceberg JARs can still query an Iceberg table.

**CUE:** RUN LAUNCH CONFIGURATION 3 (EXAMPLE 02, FIRST QUERY), THEN SCROLL TO THE EXPLAIN OUTPUT

> You can see the division of labor directly. This is df dot explain, run from a client with no optimizer.
>
> The physical plan, the adaptive execution node, the two-hundred-partition exchange. All of it came back from the server.

---

### CHAPTER 2. THE FOUR DISTRIBUTIONS (5:30 - 9:30)

**VISUAL:** `01_four_packages.svg`: four cards drawn to scale, with a dashed arrow from the 10 KB metapackage to the 429 MB distribution.

**CUE:** SHOW FOUR DISTRIBUTIONS GRAPHIC

> On PyPI, there are four distributions that all give you the pyspark namespace.
>
> Only one of them is a client without a JVM.

**SECTION 2A. THE TABLE**

**ON SCREEN:**

| Package | Artifact | `pyspark/` installed | JARs | Py4J |
|---|---|---|---|---|
| `pyspark` | 429 MB sdist | 481 MB | 456 MB, 276 JAR files | yes |
| `pyspark[connect]` | 429 MB plus extras | 481 MB | 456 MB | yes |
| `pyspark-connect` | 10 KB metapackage | installs `pyspark` | 456 MB | yes |
| `pyspark-client` | 1.60 MB | 19 MB | none | no |

**CUE:** FULL-SCREEN TABLE

> Here they are. Plain pyspark. Pyspark with the connect extra. Pyspark dash connect. And pyspark dash client.

**SECTION 2B. MEASURING IT**

**CUE:** RUN MAKE SETUP (SPEED UP THE FULL INSTALL)

> Let's look at two of them installed. One command builds both environments.

**CUE:** RUN LAUNCH CONFIGURATION 4 (EXAMPLE 03 WITH PYSPARK-CLIENT), THEN LAUNCH CONFIGURATION 5 (EXAMPLE 03 WITH FULL PYSPARK)

**ON SCREEN:**

```
distribution  pyspark-client 4.2.0
pyspark/      17 MB (sum of file sizes)
jars          none
py4j          absent
java          /usr/bin/java

distribution  pyspark 4.2.0
pyspark/      500 MB (sum of file sizes)
jars          276 files, 477 MB
py4j          present
java          /usr/bin/java
```

**CUE:** HOLD ON OUTPUT

> The same script inspects each environment.
>
> The client's pyspark directory adds up to seventeen megabytes. The full distribution's adds up to five hundred, and it includes two hundred seventy-six JAR files.
>
> Those are sums of file sizes. The table shows disk usage, so its numbers are slightly different.
>
> And on the client side, there's no JARs directory at all. That's a more reliable test than any size.

**SECTION 2C. PYSPARK DASH CONNECT IS NOT THE CLIENT**

**CUE:** RUN THE PYPI METADATA QUERY FOR PYSPARK-CONNECT, THEN HIGHLIGHT REQUIRES_DIST

**ON SCREEN:**

```
$ curl -s https://pypi.org/pypi/pyspark-connect/4.2.0/json | jq .info.requires_dist
['pyspark==4.2.0', 'pandas>=2.2.0', 'pyarrow>=18.0.0', 'grpcio>=1.76.0', 'grpcio-status>=1.76.0', 'googleapis-common-protos>=1.71.0', 'zstandard>=0.25.0', 'numpy>=1.21', 'pyyaml>=3.11']
```

> The name that causes confusion is pyspark dash connect.
>
> It's ten kilobytes of metadata. Its first dependency is the full pyspark distribution, and the rest are the Connect client's own dependencies.
>
> So installing it installs the full distribution, JARs and all.
>
> The one point six megabyte figure belongs to pyspark dash client. That package was introduced in Spark four point oh.
>
> The Connect overview page has recommended it since four point one. The four point oh page recommended pyspark with the connect extra.
>
> So older instructions aren't wrong. They're out of date.

**SECTION 2D. WHAT IT ACTUALLY SAVES**

**ON SCREEN:**

| | Full | `pyspark-client` | Reduction |
|---|---|---|---|
| Spark's Python payload | 481 MB | 19 MB | 96% |
| Spark JARs | 456 MB, 276 files | 0 | 100% |
| JDK on the client | yes | no | n/a |
| Environment with the Connect dependencies | 820 MB | 359 MB | 56% |

**CUE:** SHOW SAVINGS TABLE, THEN REVEAL THE LAST ROW

> So what does the client actually save?
>
> Spark's own Python payload drops by ninety-six percent. The JARs are gone entirely. And there's no JDK on the client.
>
> But a working client environment is still three hundred fifty-nine megabytes.
>
> That's because PyArrow alone is a hundred fifty-four megabytes. That's about eight times the size of the Spark client. And pandas and NumPy are required too.
>
> So the benefit isn't a small environment.
>
> It's no JVM, no JDK, and none of those two hundred seventy-six JAR files.

---

### CHAPTER 3. DEMO A, PART 1: SIX TOPOLOGIES (9:30 - 15:00)

**VISUAL:** IDE, `02_spark_connect` open as the workspace root. Expand the tree slowly: `examples/`, `pipeline/`, `setup/`, `tests/`.

**CUE:** IDE: EXPAND THE PROJECT TREE

> This is the companion project. Ten short examples, a small pipeline to migrate, server setup, and seventy-five tests.
>
> Setting up Spark Connect involves two decisions.
>
> How you start the server. And where it runs, relative to your data and your users.
>
> The second decision has the larger consequences.

**SECTION 3A. THE SIX TOPOLOGIES**

**CUE:** OPEN SETUP/TOPOLOGIES.MD

**ON SCREEN:**

| | Topology | Suited to |
|---|---|---|
| 1 | In-process (`spark.remote=local[*]`) | Tests, CI, a workstation with no cluster |
| 2 | Same host, separate process | Development against a session that outlives the client |
| 3 | Remote standalone cluster | A shared team cluster |
| 4 | Kubernetes | Multi-tenant platforms, per-team endpoints |
| 5 | YARN cluster mode (new in 4.2) | An existing Hadoop deployment |
| 6 | Multiple clusters | Routing by size, cost or region; blue-green upgrades |

> There are six topologies.
>
> In-process, for tests and CI.
>
> A separate process on the same host, for development.
>
> A remote standalone cluster, for a shared team.
>
> Kubernetes, for multi-tenant platforms.
>
> YARN cluster mode, new in four point two, for existing Hadoop deployments.
>
> And multiple clusters, for routing work between them.

**ON SCREEN:**

```python
spark = SparkSession.builder.remote(URL).getOrCreate()
```

> The client code is identical in all six. That's convenient.
>
> It also means nothing in your code tells you which topology you're on.
>
> So in the second half of this demo, we'll check it explicitly.

**SECTION 3B. TOPOLOGY ONE: A SERVER INSIDE THE PYTHON PROCESS**

**CUE:** RUN LAUNCH CONFIGURATION 6 (EXAMPLE 04, IN-PROCESS SERVER)

> Topology one. No container, and no cluster.
>
> Setting spark dot remote to local star starts a Connect server inside this Python process.
>
> And this is the real Connect code path. Watch. Spark context raises an error here, just as it does against a remote server.
>
> That makes this a good fit for Connect compatibility tests in CI. The tests run under the same restrictions as production, without a cluster.

**CUE:** ZOOM ON THE PORT CHECK, THEN THE SPARK VERSION LINE

> One detail matters in this mode.
>
> The client always connects to localhost, on port fifteen-oh-oh-two, whatever the binding port is set to.
>
> If another server already holds that port, the local server can't start, and you get a bind exception. That's the safe outcome.
>
> The unsafe one is moving the local server to a different port. The client still connects to fifteen-oh-oh-two, and you get a working session on the other server, with no error.
>
> So this example checks the port first, and stops with an explanation if it's taken.

**SECTION 3C. THE CLIENT WITHOUT A JVM CANNOT DO THIS**

**CUE:** RUN LAUNCH CONFIGURATION 7 (EXAMPLE 04 WITH PYSPARK-CLIENT)

**ON SCREEN:**

```
cannot start a local server: [INVALID_CONNECT_URL] Invalid URL for Spark Connect: The URL must start with 'sc://'. Please update the URL to follow the correct format, e.g., 'sc://hostname:port'.
```

> Now the same script, with pyspark dash client.
>
> It accepts only S-C URLs. It has no JVM, so there's no server for it to start.

**SECTION 3D. TOPOLOGY SIX: MULTIPLE CLUSTERS AND SESSION REUSE**

**CUE:** RUN LAUNCH CONFIGURATION 8 (EXAMPLE 05, SESSIONS)

**ON SCREEN:**

```
getOrCreate() with a different URL returned the same session: True
temporary view t: 3 rows in c, 7 rows in d
after d.stop(), c still reads 3 rows
create() with local[2]: [UNSUPPORTED_LOCAL_CONNECTION_STRING] Creating new SparkSessions with `local` connection string is not supported.
```

> Topology six. One client, several servers.
>
> Here we ask get-or-create for a session on port one.
>
> Nothing is listening on port one. And we get the first session back anyway. No error, and no warning.
>
> So in a client that works with several clusters, asking for the second cluster returns the first.
>
> And calling stop on that shared session ends it for everything else in the process.
>
> Use builder dot create instead, and each call gets its own session.

**CUE:** HOLD ON ISOLATION OUTPUT

> The same view name holds three rows in one session and seven in the other. Stopping one leaves the other running.
>
> One limit. Create won't accept a local connection string, so in-process mode still needs get-or-create.

**SECTION 3E. TOPOLOGIES THREE AND FOUR: PRODUCTION DEPLOYMENTS**

**CUE:** OPEN START-CONNECT-SERVER.SH, THEN SCROLL THE OPTIONS

> For production, the standalone launch script is a handful of options, each with a comment.
>
> Without a binding setting, a four point two server listens on every network interface. So this script sets the binding host explicitly, and defaults to loopback.
>
> And catalogs and extensions are configured here, on the server. Not on the client.
>
> You'll also see a warning in the server log. A class-not-found error, for the session extension.
>
> It looks serious, but it isn't. It refers to the server's bootstrap session, which starts before the extra JARs are on the classpath.
>
> Client sessions created afterward load the extension normally.
>
> I checked it with and without the extra driver classpath option. The warning is the same, and the extensions work either way.
>
> So check a session extension by using it. Not by reading the log.

**CUE:** OPEN CONNECT-SERVER.YAML, THEN STOP ON REPLICAS: 1

> On Kubernetes, the line to notice is replicas, one.
>
> Each Connect server holds its own session state.
>
> If a service balances traffic across three pods, a client's next request can land on a pod that doesn't have its session.
>
> So use session affinity, or one deployment per tenant.
>
> And Spark Connect has no authentication of its own. The documentation says to run it behind an authenticating proxy.
>
> Don't expose port fifteen-oh-oh-two directly.

---

### CHAPTER 3b. DEMO A, PART 2: VALIDATING THE SETUP (15:00 - 20:00)

**VISUAL:** Back to the terminal.

> Six topologies. Identical client code. Nothing in the application to tell them apart.
>
> So let's validate.

**SECTION 3bA. THE SERVER**

**CUE:** RUN MAKE UP, THEN OPEN COMPOSE.YAML AT THE SPARK-CONNECT SERVICE

**ON SCREEN:**

```yaml
spark-connect:
  entrypoint:
    - /opt/spark/sbin/start-connect-server.sh
    - --wait
    - --master
    - ${CONNECT_MASTER:-spark://spark-master:7077}
    - --conf
    - spark.connect.grpc.binding.host=0.0.0.0
    - --conf
    - spark.driver.host=spark-connect
    - --conf
    - spark.cores.max=1
```

> This starts a standalone master, one worker, and a Connect server that runs its work on that cluster.
>
> Three details in this command.
>
> Dash dash wait keeps the server in the foreground. Without it, the script moves to the background, and the container exits immediately.
>
> The Connect server takes one of the worker's two cores. That leaves one for a spark-submit job on the same cluster, which we'll need in chapter seven.
>
> And there's no packages option for the Connect JAR. Guides written for Spark three point five include one. Since Spark four point oh, the server ships in the distribution, so it isn't needed.

**SECTION 3bB. DOCTOR**

**CUE:** RUN LAUNCH CONFIGURATION 9 (DOCTOR)

**ON SCREEN:**

```
client
       distribution           pyspark-client 4.2.0
       jars                   none
       py4j                   absent
       python                 3.10.20
endpoint
       url                    sc://localhost:15095
  ok   reachable
server
  ok   version                4.2.0
capabilities
  ok   sql                    1
  ok   arrow collect          1000 rows
  ok   observe                10
  ok   python versions        both 3.10
  ok   add artifact           fails before addArtifact, succeeds after
server configuration (values of secret-like keys are masked)
       spark.api.mode                                 <unset>
       spark.sql.execution.arrow.pyspark.enabled      true
       spark.sql.execution.pythonUDF.arrow.enabled    true
       spark.sql.shuffle.partitions                   200
all checks passed
```

> This is the doctor. It doesn't assume what Connect supports. It asks the server you're connected to.

**CUE:** HOLD ON FULL OUTPUT

> The client is pyspark dash client. No JARs, and no Py4J.
>
> The server reports version four point two point oh.
>
> Every capability check passes, including a Python UDF, and adding an artifact.
>
> Now look at spark dot api dot mode. It's unset, and an application that doesn't set it runs on Spark Classic.
>
> Connect isn't the default in Spark four. The JIRA proposing that change is still open.
>
> And both Arrow settings are true. They became the default in four point two. Nothing here sets them.

**SECTION 3bC. PYTHON VERSIONS ARE STILL COUPLED**

**CUE:** RUN LAUNCH CONFIGURATION 10 (EXAMPLE 06, PYTHON VERSIONS)

**ON SCREEN:**

```
client Python 3.10.20, executor Python 3.10.12
```

> Removing the JVM from the client doesn't remove every coupling.
>
> A Python UDF is serialized on the client, and deserialized on an executor. And PySpark won't run across Python minor versions.
>
> Here the client runs Python three point ten point twenty, and the executors run three point ten point twelve. The minor versions match, so the UDF runs.

**CUE:** RERUN EXAMPLE 06 FROM A PYTHON 3.12 ENVIRONMENT

**ON SCREEN:**

```
client Python 3.12.3: [PYTHON_VERSION_MISMATCH] Python in worker has different version: 3.10 than that in driver: 3.12, PySpark cannot run with different minor versions.
```

> From Python three point twelve, the same example fails with a version mismatch.
>
> The official Spark four point two images ship Python three point ten. So this project pins its client to Python three point ten.

**CUE:** RUN THE DOCTOR AGAINST THE UNMODIFIED 4.2.0 IMAGE, THEN ZOOM ON THE TWO FAILURES

**ON SCREEN:**

```
  FAIL python versions        ModuleNotFoundError: No module named 'pyarrow'
  FAIL add artifact           ModuleNotFoundError: No module named 'pyarrow'
```

> There's a second catch in the official image.
>
> Spark four point two runs Python UDFs through Arrow by default, and the image doesn't include PyArrow.
>
> So against the unmodified image, every Python UDF fails. Module not found, pyarrow.
>
> The Dockerfile in this project adds PyArrow and pandas to the official image, and nothing else.

**SECTION 3bD. CONNECTION STRING VALIDATION**

**CUE:** RUN LAUNCH CONFIGURATION 11 (EXAMPLE 07, CONNECTION STRINGS)

**ON SCREEN:**

```
rejected  sc://localhost:15095/;user_agent=myapp/setup
          [INVALID_CONNECT_URL] Invalid URL for Spark Connect: The path component '/;user_agent=myapp/setup' must be empty. Please update the URL to follow the correct format, e.g., 'sc://hostname:port'.
accepted  sc://localhost:15095/;user_agent=myapp-setup  (Spark 4.2.0)
```

**CUE:** HOLD ON ERROR

> One more check. Connection strings.
>
> The error says the path must be empty. But this URL doesn't have a path.
>
> The problem is the slash inside the user agent value. The parser reads it as the start of a path, so the error names the path, not the parameter.
>
> With a dash instead of a slash, the same connection string is accepted.
>
> So validate parameter values before you build the URL.

**SECTION 3bE. SHIPPING PYTHON DEPENDENCIES**

**CUE:** BROWSER: PYTHON PACKAGE MANAGEMENT PAGE

> Finally, dependencies.
>
> This is the official page on shipping Python dependencies to Spark. Py-files, conda-pack, venv-pack, and PEX.
>
> It describes Spark Classic. It doesn't cover Spark Connect.
>
> None of these apply here. Py-files is a spark-submit option. And add-py-file lives on Spark context, which isn't available.
>
> The Connect mechanism is spark dot add-artifact, and it has three modes.

**CUE:** RUN LAUNCH CONFIGURATION 12 (EXAMPLE 08, ADDARTIFACT)

**ON SCREEN:**

```
before addArtifact: ModuleNotFoundError: No module named 'geo'
after addArtifact:
+---------+-----+-------+---------------+
|     city|  lat|    lng|km_from_toronto|
+---------+-----+-------+---------------+
| Montreal| 45.5| -73.57|          503.9|
|Vancouver|49.28|-123.12|         3359.3|
+---------+-----+-------+---------------+
```

> Notice how this check works.
>
> First, it confirms the UDF fails. Then it ships the dependency. Then it confirms the UDF succeeds.
>
> A test of the success case alone would also pass on a server that already had the module installed.
>
> It wouldn't be testing add-artifact at all.

---

### CHAPTER 4. SPARK DOT API DOT MODE (20:00 - 22:30)

**VISUAL:** `04_api_mode_switch.svg`: one source file, a switch, two execution paths.

**CUE:** SHOW API MODE DIAGRAM

> This one setting lets you migrate one job at a time, instead of everything at once.

**ON SCREEN:**

```bash
spark-submit --master spark://host:7077 --conf spark.api.mode=connect job.py
```

**SECTION 4A. HOW IT DIFFERS FROM SPARK DOT REMOTE**

> So how is it different from spark dot remote?
>
> Spark dot remote does one of two things. It connects to a server that's already running. Or it has Spark start a local one.
>
> What it can't do is take an existing cluster master URL, and start a server on that cluster.
>
> That's what spark dot api dot mode does.
>
> Your submit command keeps its master URL, and one setting decides the execution path.
>
> So you can move a job to Connect, watch it, and move it back. No branch, and no second copy of the pipeline.

**SECTION 4B. COMPARING BOTH MODES IN CI**

> It also means CI can run a job both ways, and compare the output.
>
> In chapter seven, we'll make that comparison meaningful.

---

### CHAPTER 5. API DIFFERENCES, AND WHAT 4.2 ADDED (22:30 - 27:00)

**VISUAL:** `10_connect_42_whats_new.svg`: JIRA grid, each cell carrying its ID.

**CUE:** SHOW JIRA GRID

> Now, the API differences. These come straight from the client source.

**SECTION 5A. UNSUPPORTED APIS**

**CUE:** SHOW BLOCKED ATTRIBUTE LIST

> The four point two Connect client rejects six session attributes, and five DataFrame attributes.
>
> Only two of them are likely to show up in application code. Spark context, and R-D-D.
>
> The rest are private handles into the JVM.
>
> New session is also on the list. It isn't prominent in the documentation, so it's worth knowing about.
>
> And RDDs are a permanent difference. The protocol has no way to represent a function that runs directly against RDD partitions.

**CUE:** RUN LAUNCH CONFIGURATION 13 (EXAMPLE 09, API DIFFERENCES), THEN ZOOM ON THE THREE ERRORS

> Here are three of them, raising the same error class. Spark context, the JVM handle, and R-D-D.

**SECTION 5B. ACCUMULATORS AND BROADCAST**

**CUE:** REPL: ACCUMULATOR RAISES

> Accumulators are created through Spark context. Spark context isn't available, so neither are accumulators.
>
> Here it is, raising.
>
> Broadcast needs more care.
>
> The broadcast join hint, in the functions module, works fine. A broadcast variable, created from Spark context, doesn't.
>
> And if you need to count something during a job, use DataFrame dot observe. That's the Connect replacement for an accumulator.

**SECTION 5C. WHAT 4.2 ADDED**

**ON SCREEN:**

```python
spark.range(4).zipWithIndex()          # SPARK-55229
spark.read.json(df_of_json_strings)    # SPARK-56253
spark.emptyDataFrame("a int")          # SPARK-56256: a method that takes a schema
```

**CUE:** SCROLL EXAMPLE 09 TO THE ZIPWITHINDEX, READ.JSON AND EMPTYDATAFRAME OUTPUT

> Spark four point two also closed several gaps.
>
> Zip-with-index now works over Connect. The JSON reader accepts a DataFrame. And empty DataFrame is available.
>
> Two details.
>
> The DataFrame-input readers cover JSON, XML, and CSV only. Not Parquet, and not ORC.
>
> And empty DataFrame is a method that takes a schema. It isn't a property.

---

### CHAPTER 6. THE SEVEN LAYERS OF A MIGRATION (27:00 - 33:00)

**VISUAL:** `07_migration_layers.svg`: the seven layers with a scope column.

**ON SCREEN:**

| Layer | Area | Scope of change |
|---|---|---|
| 0 | Session creation | One call |
| 1 | Pipeline logic | Audit; few changes |
| 2 | Data sources and catalogs | Configuration relocates to the server |
| 3 | Configuration | Divides between client and server |
| 4 | Dependencies | Replaced by `addArtifact` |
| 5 | Execution and operations | Submission, logging and lifecycle change |
| 6 | Security and identity | Credentials leave the client |

**CUE:** SHOW SEVEN LAYERS GRAPHIC

> A Connect migration isn't one change.
>
> It's seven independent layers, and only one of them is mandatory.
>
> Treating it as a single cutover is what makes it hard to predict.

**SECTION 6A. LAYER TWO: DATA SOURCES AND CATALOGS**

**CUE:** SHOW CONFIGURATION BOUNDARY GRAPHIC

> Layer two, data sources and catalogs, is mostly about the mental model. Not the code.
>
> Your Iceberg catalog configuration doesn't change at all. It just has to be on the server.
>
> The value of that setting is a Java class name. And a client without a JVM has no class loader to load it.
>
> Nothing is preloaded or cached on the client.
>
> The client never reads Iceberg metadata, Parquet files, or your object store. It sends a table name, and gets rows back as Arrow.

**SECTION 6B. LAYER THREE: VALUES, NOT CLASS NAMES**

**ON SCREEN:**

| Configuration | Settable from the client |
|---|---|
| `spark.sql.shuffle.partitions` | Yes: a value the server reads |
| `spark.sql.extensions` | No: read when the session is created |
| `spark.jars` | No: the classpath is fixed when the server starts |

> Layer three, configuration, follows one rule. A client can set values. It can't set class names.
>
> Shuffle partitions is a value, so a client can set it.
>
> Extensions and JARs are loaded when the server starts, so a client can't.
>
> One case produces no error at all.
>
> Setting the Iceberg catalog from the client is accepted silently. It fails later, when the catalog is first used. Possibly in the middle of a write.

**SECTION 6C. LAYER FOUR: WHAT STAYS IN THE SERVER IMAGE**

> Layer four, dependencies. Add-artifact ships Python code.
>
> It doesn't ship catalog JARs, native libraries, or session extensions. Those belong in the server image.
>
> And transitive JAR dependencies aren't resolved for you.

**SECTION 6D. LAYER SIX: CREDENTIALS**

> Layer six, security and identity.
>
> In the lakehouse-stack deployment from the case study, the catalog credentials are mounted into the master and every worker. That's a Postgres password, and object storage keys.
>
> Any job that's submitted can read them.
>
> After the migration, only the Connect server needs them. Laptops, CI runners, and notebooks don't hold catalog credentials at all.
>
> Remember, though, that Spark Connect has no built-in authentication. It belongs behind an authenticating proxy.

**SECTION 6E. ORDER OF OPERATIONS**

> The layers are numbered by where they sit in an application. But you carry them out in dependency order.
>
> Stand up the server first. Point one job at it. Run the audit. Move dependencies. Then update operations.
>
> And remove client credentials last, because that's the hardest step to reverse.

---

### CHAPTER 7. DEMO B: MIGRATING A PIPELINE (33:00 - 41:00)

**VISUAL:** `06_demo_b_before_after.svg`, then the IDE with `pipeline/` expanded.

**CUE:** SHOW BEFORE-AND-AFTER GRAPHIC

> Now the migration. This pipeline is small on purpose, so we can see every change.
>
> It reads three inputs: order events, stores, and brands. It writes three bronze tables, two silver tables, and two gold tables. Seven in all.
>
> The enrichment step uses a Python UDF, which computes each delivery's distance with a helper module.
>
> There are two versions of the entry point. Pipeline before runs on Spark Classic, submitted with spark-submit. Pipeline after runs the same pipeline over Spark Connect, as an ordinary Python program.
>
> Both call the same transformation functions, in lib slash transforms. Those don't change.

**SECTION 7A. AUDITING THE PIPELINE**

**CUE:** RUN LAUNCH CONFIGURATION 14 (AUDIT)

**ON SCREEN:**

```
pipeline_before.py:26:0 [BLOCKER/L5] setLogLevel()
pipeline_before.py:27:0 [BLOCKER/L4] addPyFile()
pipeline_before.py:30:3 [BLOCKER/L1] .rdd
pipeline_before.py:21:4 [BEHAVIOR/L3] enableHiveSupport()
examples/10_eager_and_lazy.py:25:0 [BEHAVIOR/L1] except AnalysisException around transformations
examples/10_eager_and_lazy.py:38:0 [BEHAVIOR/L1] temp view 't' registered 2 times

6 finding(s): 3 BLOCKER, 3 BEHAVIOR, 0 PERF
```

> Apache Spark doesn't ship a Connect compatibility checker. The official mechanism is a label in the API reference.
>
> So this project includes a static audit. It's a demo tool, not part of Apache Spark, and the source is in the repository.

**CUE:** ZOOM ON THE FOUR PIPELINE_BEFORE.PY FINDINGS

> In pipeline before, it finds four things.
>
> Set log level and add-py-file, which both go through Spark context. R-D-D dot is-empty. And enable Hive support, which doesn't raise under Connect, but doesn't do anything either.
>
> Each finding carries its layer. Set log level is layer five, add-py-file is layer four, the RDD call is layer one, and Hive support is layer three.
>
> In pipeline after, it finds nothing.

**SECTION 7B. FINDINGS A TEXT SEARCH CANNOT DETECT**

**CUE:** ZOOM ON THE TWO EXAMPLE 10 FINDINGS

> The audit also ran on example ten, and its two findings are a different kind.
>
> A try block that catches an analysis exception around a transformation. And a temporary view registered twice under the same name.
>
> Neither is a name you can search for. They depend on the structure of the program. That's why the audit parses the code.

**CUE:** RUN LAUNCH CONFIGURATION 15 (EXAMPLE 10 ON SPARK CLASSIC), THEN LAUNCH CONFIGURATION 16 (EXAMPLE 10 ON SPARK CONNECT)

**ON SCREEN:**

```
mode: classic
invalid column: error raised when the DataFrame is defined
view t replaced: the earlier DataFrame now counts 3 rows

mode: connect
invalid column: no error at definition; error raised when the action runs
view t replaced: the earlier DataFrame now counts 7 rows
```

> Here's what those findings mean when the code runs.
>
> On Spark Classic, the invalid column raises when the DataFrame is defined. On Spark Connect, it raises only when an action runs. So a handler around the definition never sees the error.
>
> And the view. Spark Classic captured the plan when the view was registered, so the earlier DataFrame still counts three rows.
>
> Spark Connect looks the view up by name when the query runs, so the same DataFrame now counts seven.

**SECTION 7C. THE DIFF**

**CUE:** IDE SIDE-BY-SIDE DIFF OF PIPELINE_BEFORE.PY AND PIPELINE_AFTER.PY

> Here's the whole diff. Twenty-one lines added and thirteen removed, counting the docstrings. Each change is on a line marked with its layer.
>
> Layer zero. The session comes from a small builder function, which returns a Connect session when spark remote is set.
>
> Layer one. R-D-D dot is-empty becomes is-empty.
>
> Layer three. Shuffle partitions is a value the server reads, so the client still sets it. Enable Hive support is gone.
>
> Layer four. Add-py-file becomes add-artifact.
>
> Layer five. Set log level is gone. The log level belongs to the server.
>
> No transformation logic changed. Connect changes where the code runs. Not the code itself.

**SECTION 7D. FILE PATHS RESOLVE ON THE SERVER**

**ON SCREEN:**

```python
INPUT = str(HERE / "input")                                         # before: a path on the host
INPUT = os.environ.get("PIPELINE_INPUT", "/demo/pipeline/input")    # after: a path on the server
```

**CUE:** HIGHLIGHT THE TWO INPUT LINES

> Layer two is the change the audit can't find.
>
> Under Connect, the server opens the input files. So the path has to be one the server can see.
>
> The cluster mounts the pipeline directory at slash demo slash pipeline, and the migrated version reads from there.
>
> Point it at the path on this laptop instead, and the run fails with path not found.

**ON SCREEN:**

```
AnalysisException: [PATH_NOT_FOUND] Path does not exist: file:/<project>/pipeline/input/order_events.jsonl. SQLSTATE: 42K03
```

> The audit can't catch that, because a path is just a string.

**SECTION 7E. VERIFYING THE OUTPUT**

**CUE:** RUN MAKE PIPELINE

**ON SCREEN:**

```
parity: /opt/spark/work-dir/out/classic  vs  /opt/spark/work-dir/out/connect
  bronze_events                ok       238 rows, hashes match
  bronze_stores                ok       4 rows, hashes match
  bronze_brands                ok       5 rows, hashes match
  silver_orders_enriched       ok       235 rows, hashes match
  silver_order_lifecycle       ok       55 rows, hashes match
  gold_delivery_performance    ok       4 rows, hashes match
  gold_brand_summary           ok       5 rows, hashes match
  7/7 tables identical
```

> A run that finishes without errors doesn't prove the behavior was preserved.
>
> Make pipeline runs the Classic version with spark-submit inside the cluster, runs the Connect version from this laptop, and then compares what they wrote.
>
> It compares every table in three ways. Schema, row count, and a content hash that doesn't depend on row order.
>
> The hash matters. A row count can't catch a result with the right number of rows, but the wrong values.
>
> And that's exactly what an unintended change to a join produces.
>
> The comparison has tests that build that case, to confirm it gets caught.

**CUE:** HOLD ON 7/7

> Seven of seven tables match.

**SECTION 7F. THE SAME LAYERS IN A PRODUCTION PIPELINE**

**CUE:** OPEN CASE_STUDY/LAKEHOUSE_STACK/README.MD AT THE PARITY RESULTS

**ON SCREEN:**

```
parity: iceberg_before  vs  iceberg
  bronze.dim_categories          ok       10 rows, hashes match
  bronze.orders                  ok       1,027,129 rows, hashes match
  silver.orders_enriched         ok       1,009,933 rows, hashes match
  ...
  gold.brand_summary             ok       21 rows, hashes match
  10/10 tables identical
```

> The same layers apply at a larger scale.
>
> The case study in the repository migrates a three-hundred-fifty-seven-line production pipeline that writes ten Iceberg tables.
>
> Its only blocking call was set log level. And its parity check found ten of ten tables identical, including more than a million bronze orders.
>
> One note on that case study. Apache Iceberg doesn't publish a runtime for Spark four point two yet, so it uses a local source build. That isn't a supported release, and none of the demos in this video depend on it.

---

### CHAPTER 8. OPERATING CONNECT, AND WHERE TO START (41:00 - 43:00)

**VISUAL:** `10_connect_42_whats_new.svg` again, with the operations cells highlighted.

**CUE:** SHOW JIRA GRID: OPERATIONS CELLS

> Most of the Connect work in four point two is about running a server in production.
>
> Get-status now has a server-side implementation, so health checks have a defined call to make.
>
> The Connect tab works in the History Server again. That's a bug fix, and it was also backported to four point one and four point oh.
>
> And an environment variable on the client now releases the server-side session when the client process exits. That's useful in notebooks and CI jobs.
>
> The JIRA numbers are on screen.
>
> One recommendation. Set the user agent in your connection strings from the start, as the pipeline in chapter seven does. It's how a client identifies itself to the server.

**SECTION 8A. DECIDING WHETHER TO MIGRATE**

**ON SCREEN:**

| If you... | Then |
|---|---|
| run notebooks against shared clusters | a strong candidate for Connect |
| embed Spark in a service or an agent | a strong candidate; the client without a JVM is the reason |
| mount credentials on every worker | layer 6 alone may justify it |
| depend on RDDs | keep those jobs on Spark Classic |
| are also upgrading from 4.1 to 4.2 | make the two changes separately |

**CUE:** SHOW DECISION TABLE

> So, should you migrate?
>
> If you run notebooks against shared clusters, you're a strong candidate.
>
> If you embed Spark in a service or an agent, you're a strong candidate too. A client without a JVM is the reason.
>
> If credentials are mounted on every worker, layer six alone may justify it.
>
> If you depend on RDDs, keep those jobs on Spark Classic.
>
> And if you're also upgrading from four point one to four point two, make those two changes separately.

**SECTION 8B. WHERE TO START**

> Here's a reasonable first step.
>
> Stand up a Connect server beside your existing cluster.
>
> Point one job at it with spark dot api dot mode, so you can move it back.
>
> And run an audit across your repository.
>
> That takes a morning, and it tells you how large your migration really is.

**CUE:** END CARD

**END CARD:** Repository link (`spark_42_demos/demos/02_spark_connect`), blog post link, Spark 4.2.0 release notes link.

---


## PRODUCTION NOTES

### Teleprompter

Load `video_spark_connect_teleprompter.txt` into the teleprompter app. It contains only chapter and
section markers, bracketed cues, and the spoken text, one paragraph per breath. Regenerate it with
`make teleprompter` after editing this file; the command also prints the spoken word count per
chapter.

**Pronunciation guide.** The spoken text writes identifiers the way they are said. Use this table to
match them to what appears on screen.

| Spoken text | On screen | Say |
|---|---|---|
| pyspark dash client | `pyspark-client` | "pie-spark dash client" |
| pyspark dash connect | `pyspark-connect` | "pie-spark dash connect" |
| pyspark with the connect extra | `pyspark[connect]` | |
| PyPI | PyPI | "pie-P-I" |
| Py4J | Py4J | "pie-four-jay" |
| spark dot api dot mode | `spark.api.mode` | |
| spark dot remote | `spark.remote` | |
| local star | `local[*]` | |
| S-C URL | an `sc://` connection string | "ess-see" |
| port fifteen-oh-oh-two | `15002` | |
| get-or-create | `SparkSession.builder.getOrCreate()` | |
| builder dot create | `SparkSession.builder.create()` | |
| Spark context | `SparkContext`, `spark.sparkContext` | |
| Spark context dot set log level | `spark.sparkContext.setLogLevel()` | |
| spark dot add-artifact | `spark.addArtifact()` | |
| add-py-file | `SparkContext.addPyFile()` | |
| py-files | `--py-files` | |
| dash dash wait | `--wait` | |
| df dot explain | `df.explain()` | |
| DataFrame dot observe | `DataFrame.observe()` | |
| zip-with-index | `DataFrame.zipWithIndex()` | |
| empty DataFrame | `SparkSession.emptyDataFrame()` | |
| get-status | the `GetStatus` RPC | |
| R-D-D | RDD | "are-dee-dee" |
| R-D-D dot is-empty | `events.rdd.isEmpty()` | |
| is-empty | `DataFrame.isEmpty()` | |
| enable Hive support | `enableHiveSupport()` | |
| pipeline before, pipeline after | `pipeline_before.py`, `pipeline_after.py` | |
| lib slash transforms | `pipeline/lib/transforms.py` | |
| slash demo slash pipeline | `/demo/pipeline` | |
| make setup, make up, make pipeline | `make setup`, `make up`, `make pipeline` | |
| example ten | `examples/10_eager_and_lazy.py` | |
| module not found, pyarrow | `ModuleNotFoundError: No module named 'pyarrow'` | |

**Spoken timing.** At 150 words per minute. Each chapter's window also contains demo footage and
pauses on output, so spoken time should stay well inside it.

| Chapter | Spoken words | Spoken time |
|---|---|---|
| CHAPTER 0. COLD OPEN (0:00 - 1:30) | 74 | 0:30 |
| CHAPTER 1. WHY CONNECT EXISTS, AND HOW IT WORKS (1:30 - 5:30) | 423 | 2:49 |
| CHAPTER 2. THE FOUR DISTRIBUTIONS (5:30 - 9:30) | 296 | 1:58 |
| CHAPTER 3. DEMO A, PART 1: SIX TOPOLOGIES (9:30 - 15:00) | 641 | 4:16 |
| CHAPTER 3b. DEMO A, PART 2: VALIDATING THE SETUP (15:00 - 20:00) | 549 | 3:40 |
| CHAPTER 4. SPARK DOT API DOT MODE (20:00 - 22:30) | 134 | 0:54 |
| CHAPTER 5. API DIFFERENCES, AND WHAT 4.2 ADDED (22:30 - 27:00) | 220 | 1:28 |
| CHAPTER 6. THE SEVEN LAYERS OF A MIGRATION (27:00 - 33:00) | 342 | 2:17 |
| CHAPTER 7. DEMO B: MIGRATING A PIPELINE (33:00 - 41:00) | 722 | 4:49 |
| CHAPTER 8. OPERATING CONNECT, AND WHERE TO START (41:00 - 43:00) | 248 | 1:39 |
| total spoken | 3649 | 24:20 |


### B-Roll / Visuals Needed

| Timestamp | Visual | Type |
|---|---|---|
| 0:00-1:30 | Two IDE windows with `examples/01_classic_or_connect.py`: launch configurations 1 and 2, `java: command not found` on one | Screen recording |
| 1:30-3:30 | Driver coupling diagram and history timeline | Motion graphics |
| 3:30-5:00 | Plan-in/Arrow-out animation | Motion graphics |
| 5:00 | Launch configuration 3 (example 02) with the `explain()` output | Screen recording |
| 5:30-6:30 | Four-distribution table, full screen | Diagram |
| 6:30-8:00 | `make setup` (full install sped up), then launch configurations 4 and 5 (example 03) | Screen recording |
| 8:00-8:30 | `pyspark-connect` metadata from the PyPI JSON API, `requires_dist` highlighted | Screen recording |
| 8:30-9:30 | Savings table, 359 MB row revealed last | Animated chart |
| 9:30-10:00 | IDE tree expanding: `examples/`, `pipeline/`, `setup/`, `tests/` | Screen recording |
| 10:00-10:45 | `setup/TOPOLOGIES.md` and the six-topology table | Diagram |
| 10:45-12:30 | Launch configuration 6 (example 04): the port check, then the Spark version line | Screen recording |
| 12:30-13:00 | Launch configuration 7 (example 04 with `pyspark-client`) → `INVALID_CONNECT_URL` | Screen recording |
| 13:00-14:00 | Launch configuration 8 (example 05): session reuse, then isolated sessions | Screen recording |
| 14:00-15:00 | `start-connect-server.sh` options; then `connect-server.yaml` `replicas: 1` | Screen recording |
| 15:00-15:40 | `make up` and the `spark-connect` service in `compose.yaml` | Screen recording |
| 15:40-17:00 | Launch configuration 9 (doctor), all checks passing | Screen recording |
| 17:00-18:00 | Launch configuration 10 (example 06); example 06 from Python 3.12; the doctor against the unmodified image | Screen recording |
| 18:00-18:30 | Launch configuration 11 (example 07) | Screen recording |
| 18:30-20:00 | Official packaging page; launch configuration 12 (example 08) | Screen recording |
| 20:00-22:30 | `spark.api.mode` switch diagram | Motion graphics |
| 22:30-27:00 | JIRA grid (`10_connect_42_whats_new.svg`); launch configuration 13 (example 09); accumulator raising in a REPL | Diagram + screen |
| 27:00-33:00 | Seven-layer diagram; configuration boundary diagram | Diagram |
| 33:00-35:00 | Launch configuration 14 (audit), zoom on the findings; launch configurations 15 and 16 (example 10) | Screen recording |
| 35:00-37:00 | IDE side-by-side diff of `pipeline_before.py` and `pipeline_after.py` | Screen recording |
| 37:00-40:00 | `make pipeline`, ending in `7/7 tables identical` | Screen recording |
| 40:00-41:00 | `case_study/lakehouse_stack/README.md` at the parity results | Screen recording |
| 41:00-43:00 | JIRA grid, operations cells | Diagram |

### Code Demos to Pre-Record

| Demo | Chapter | Duration | Requirements |
|---|---|---|---|
| `make setup`, then example 03 in both environments | 2 | 90 s | uv; the full install is slow, so cut it |
| `pyspark-connect` metadata from PyPI | 2 | 20 s | network access, `jq` |
| Example 04, in-process server | 3 | 60 s | `.venv-full` and a JDK; port 15002 free (run it before `make up`, or set `CONNECT_PORT` in `.env`) |
| Example 04 with `pyspark-client` (`INVALID_CONNECT_URL`) | 3 | 20 s | `.venv` |
| Example 05, sessions | 3 | 60 s | `make up` |
| `make up` and `compose.yaml` | 3b | 45 s | Docker |
| Doctor, all checks passing | 3b | 60 s | `make up` |
| Example 06, then example 06 from Python 3.12 | 3b | 40 s | a Python 3.12 environment with `pyspark-client==4.2.0` and `pandas<3` |
| Doctor against the unmodified 4.2.0 image | 3b | 30 s | a Connect server from `apache/spark:4.2.0-scala2.13-java21-python3-ubuntu` on a spare port |
| Example 07, connection strings | 3b | 40 s | `make up` |
| Example 08, `addArtifact` | 3b | 45 s | `make up` |
| Example 09; accumulator raising in a REPL | 5 | 40 s | `make up` |
| Audit, then example 10 in both modes | 7 | 90 s | `.venv-full` and a JDK for the Spark Classic run |
| Before/after diff | 7 | 60 s | IDE diff view |
| `make pipeline` → 7/7 | 7 | 30 s | `make up` |

### Graphics Needed

1. **`01_four_packages.svg`** (Ch 2): four cards to scale, dashed arrow from the 10 KB metapackage to the 429 MB distribution. Also the thumbnail source.
2. **`02_plan_in_arrow_out.svg`** (Ch 1): client/server split, protobuf plan out, Arrow results back, plan compression annotated.
3. **`03_connection_string_anatomy.svg`** (Ch 3b): `sc://host:port/;k=v` broken into parts, with its three constraints.
4. **`04_api_mode_switch.svg`** (Ch 4): one file, a switch, two execution paths.
5. **`05_demo_a_topology.svg`** (Ch 3b): the host with `.venv` (`pyspark-client`) and `.venv-full`; the `compose.yaml` network with `spark-master` (UI 8080), `spark-worker` (2 cores, 2 GB), `spark-connect` (15002, UI 4040) and the shared `/opt/spark/work-dir` volume.
6. **`06_demo_b_before_after.svg`** (Ch 7): left, `spark-submit` inside the `spark-connect` container runs `pipeline_before.py` on the standalone master; right, `pipeline_after.py` on the host sends plans to the Connect server; both use the same worker and write to the shared volume, and `parity.py` compares the outputs.
7. **`07_migration_layers.svg`** (Ch 6): the seven layers with the scope column; designed to stand on its own.
8. **`08_config_boundary.svg`** (Ch 6): values vs class names, with the "no client JVM, no class loader" mechanism drawn.
9. **`09_addartifact_flow.svg`** (Ch 3b/6): Spark Classic mechanisms vs `addArtifact` modes.
10. **`10_connect_42_whats_new.svg`** (Ch 5/8): JIRA grid, each cell carrying its ID.
11. **`11_migration_friction_map.svg`** (Ch 7): `pipeline_before.py` (50 lines) as a line gutter, with lines 14, 20 to 27 and 30 colored by layer to match graphic 7, beside the corresponding lines of `pipeline_after.py`.
12. **`12_topology_map.svg`** (Ch 3): the six topologies in one diagram: process, host and cluster boundaries, with the identical client code line through all of them.
13. **`13_title_card.svg`**
14. **`14_driver_coupling_and_timeline.svg`** (Ch 1): PySpark application and driver JVM joined by Py4J, beside a client connected to a Connect server over gRPC; below, a timeline from the 2022 SPIP to 4.2 with JIRA IDs.

Conventions: 1920×1080, dark primary with a same-named `light/` mirror, `NN_snake_case.svg`, Spark
orange `#E25A1C`, Connect purple `#8957e5`/`#a371f7`, background `#1a1a2e`, cards `#161b22`, borders
`#30363d`, text `#e6edf3`/`#7d8590`, `'Segoe UI', Arial` with `Consolas` for code.

### Thumbnail Concepts

- **A:** A 429 MB block beside a 1.6 MB block, drawn to scale, with the caption "Spark Connect in 4.2".
- **B:** Split terminal. Left: `PySparkAttributeError: sparkContext`. Right: the pipeline output. Caption: "the one line that changed".
- **C:** Four `pip install` boxes, one highlighted. Caption: "which one has no JVM?"

### ON-SCREEN SAFETY CHECKLIST (review every frame before export)

The demos in this project mount no configuration files and hold no credentials. The case study, and any footage of the lakehouse stack, touches configuration files that contain real credentials.

- **Never** show `~/lakehouse-stack/config/spark/spark-defaults.conf`: plaintext Postgres password and object-store keys.
- **Never** show `~/open-lakehouse/config/spark/spark-defaults.conf`: contains a live-looking JWT and AWS keys. Do not open that repository on camera at all.
- **Never** show the rendered `/opt/spark/conf/spark-defaults.conf` inside a container.
- **Never** show unfiltered `docker inspect` (it prints environment variables) or `env` output.
- To show a mount, use the mounts-only form: `docker inspect <c> --format '{{range .Mounts}}{{.Source}}:{{.Destination}}{{"\n"}}{{end}}'`.
- **Never** use the lakehouse-stack `spark-connect-41` container for footage; record against the services in this project's `compose.yaml`.
- `tools/doctor.py` prints four fixed configuration keys and masks the value of any key whose name contains password, secret, token, credential or access.key.
- Do not show a connection string that carries a `token`.

### Accuracy Notes for Q&A

Likely questions, with the correct answers:

- **"Isn't the JDBC driver new in 4.2?"** No. SPARK-53484 has fix version 4.1.0, and all 26 subtasks landed in 4.1.0. The URL scheme is `jdbc:sc://`. There is no documentation page.
- **"Isn't Connect the default in Spark 4?"** No. Without configuration, applications use Spark Classic; SPARK-50411 is still open.
- **"Didn't 4.2 add geospatial types?"** No. A scan of every JAR in the official 4.2.0 image (git revision `32f72996011`) finds no `GeographyVal`, `GeometryVal` or `ST_*` functions.
- **"Is the Iceberg runtime official?"** No, and the video says so. There is no Iceberg release for Spark 4.2 (Maven Central stops at `4.1_2.13`), so the case study uses a local source build, documented in `case_study/lakehouse_stack/BUILDING_ICEBERG_FOR_SPARK_4_2.md`. Nothing else in the material depends on it.
- **"Does YARN cluster mode work?"** SPARK-55239 is resolved with fix version 4.2.0, but it isn't itemized in the release notes and wasn't tested here; cite the JIRA.
- **"Can the client and server run different Spark versions?"** The documentation describes an existing client with a newer server, but publishes no compatibility matrix. A sample of fourteen operations between 4.1.2 and 4.2.0 (blog post Table 2-1) found that a 4.1.2 client behaved the same against a 4.2.0 server as against its own. A 4.2.0 client against a 4.1.2 server failed on `createDataFrame()` from local Python data, because the older server reports `spark.sql.session.localRelationSizeLimit` as `3221225472b`, and on the new `GetStatus` RPC. Present it as a sample, not a support statement. Python UDFs still need matching Python minor versions.
- **"When did Structured Streaming and ML come to Connect?"** The 3.5.0 release notes list Structured Streaming (SPARK-42938) and PyTorch-based distributed ML (SPARK-42471); the 4.0.0 release notes list ML on Spark Connect. The JIRA umbrellas for the 3.5.0 items have no fix version set, so cite the release notes.

### Cross-Promotion

- Demo 1 of this series covers metric views in Spark 4.2 ([blog post](../01_metrics_views/blog_metric_views.md)).
- §12 of *Companion Guide: Spark on Kubernetes* (`companion_guide_spark_kubernetes.md`, a separate guide in the Spark content library) for Connect on Kubernetes.
- Spark 4.2's Data Source V2 work (transactions, schema evolution, operation metrics, partition-statistics filtering) is material for a separate video.
