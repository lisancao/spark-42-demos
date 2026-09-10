# Spark Connect Deployment Topologies

This document explains why deploying Spark Connect involves a choice of topology, describes six
topologies, and shows how to choose between them. Behavior marked **verified** was observed against
Apache Spark 4.2.0 on 2026-09-09. Material marked **untested here** is taken from the documentation and
was not exercised.

## Background

### From Deploy Mode to Topology

In Spark Classic, where the driver runs is decided for each application when it is submitted.
Per the submission documentation, `spark-submit --deploy-mode` chooses "whether to deploy your driver
on the worker nodes (`cluster`) or locally as an external client (`client`)", and `client` is the
default. Because the application's code runs inside the driver, that choice also decides where the
application runs, and the driver must remain reachable by its executors for as long as the
application runs.

Spark Connect separates the two decisions. The driver becomes a long-running server that clients
reach over gRPC, so where the driver runs is decided once, when the server is deployed, and client
applications take no part in it. A *topology* is that decision: where the Connect server runs
relative to the cluster, the data and the clients.

Two properties of Connect determine what each topology can offer (companion guide §2). A single
server hosts many sessions, isolated from one another but sharing the server's JVM, memory and
executors; isolation stronger than that requires more servers. And session state lives only on the
server that created it, so anything that places several servers behind one address must route each
client back to the same server.

### How Server Deployment Has Developed

*Table 1. Spark Connect server deployment by release*

| Release | Change | Source |
|---|---|---|
| Spark 3.4.0 (April 2023) | The server is started with `start-connect-server.sh --packages org.apache.spark:spark-connect_2.12:3.4.0`; the documentation notes that the package "is required to use Spark Connect" | 3.4.0 Connect overview |
| Spark 3.5.0 (September 2023) | Unchanged: the server still requires the `spark-connect` package | 3.5.0 Connect overview |
| Spark 4.0.0 (May 2025) | The server is included in the distribution, and `start-connect-server.sh` takes no package. An additional download is "pre-built … with Spark Connect enabled". `spark.api.mode` is introduced | 4.0.0 Connect overview; 4.0.0 release notes; downloads page |
| May 2025 – July 2026 | The Apache Spark Kubernetes Operator publishes its first release (0.1.0, May 2025) and 1.0.0 (July 2026). Its examples deploy a Connect server as a `SparkApplication` whose main class is `SparkConnectServer` | apache/spark-kubernetes-operator |
| Spark 4.2.0 (July 2026) | The Connect server can run inside a YARN application master | [SPARK-55239](https://issues.apache.org/jira/browse/SPARK-55239) |

**Citations:** [Submitting Applications](https://spark.apache.org/docs/latest/submitting-applications.html), [Cluster Mode Overview](https://spark.apache.org/docs/latest/cluster-overview.html), [Spark Connect Overview (3.4.0)](https://spark.apache.org/docs/3.4.0/spark-connect-overview.html), [Spark Connect Overview (4.0.0)](https://spark.apache.org/docs/4.0.0/spark-connect-overview.html), [Spark Release 4.0.0](https://spark.apache.org/releases/spark-release-4-0-0.html), [Apache Spark Kubernetes Operator](https://github.com/apache/spark-kubernetes-operator), [SPARK-55239](https://issues.apache.org/jira/browse/SPARK-55239)

---

## Choosing a Topology

*Table 2. Spark Connect deployment topologies*

| Topology | Where the server runs | Suited to | Less suited to |
|---|---|---|---|
| 1. In-process | Inside the Python process (`spark.remote=local[*]`) | Unit tests, CI, a workstation with no cluster, teaching | Work that needs more than one machine, or a session that outlives the process |
| 2. Same host, separate process | A daemon on the same machine | Development against a persistent session | Anything shared between people |
| 3. Remote standalone cluster | One server in front of one cluster | A shared team cluster, notebooks, BI tools | Tenants that need isolation stronger than separate sessions |
| 4. Kubernetes | A Deployment behind a Service | Multi-tenant platforms, autoscaling, per-team endpoints | Teams that do not already operate Kubernetes |
| 5. YARN cluster mode | Inside a YARN application master (new in 4.2) | An existing Hadoop deployment | New deployments without an existing YARN cluster |
| 6. Multiple clusters | Several servers, selected by the client | Routing by workload size, cost or region; blue-green upgrades | Environments with a single cluster |

The client code is the same in every row:

```python
spark = SparkSession.builder.remote(URL).getOrCreate()
```

Because the code does not reveal the topology, validate the deployment explicitly with `doctor`,
described at the end of this document.

---

## 1. In-Process

`spark.remote=local[*]` starts a Connect server inside the Python process and connects to it over
loopback gRPC. No separate server is required.

```python
spark = SparkSession.builder.remote("local[*]").getOrCreate()
```

**Verified:** this is the Connect code path. The session object is a
`pyspark.sql.connect.session.SparkSession`, and `spark.sparkContext` raises `PySparkAttributeError`
as it does against a remote server. Connect compatibility tests in CI can therefore use this topology
and exercise the same restrictions as production.

In-process mode requires `pyspark[connect]`. The other distributions fail in different ways
(Table 3).

*Table 3. `remote("local[*]")` by distribution (verified)*

| Distribution | Result |
|---|---|
| `pyspark[connect]` | Works |
| `pyspark` | `[PACKAGE_NOT_INSTALLED] Pandas >= 2.2.0 must be installed` |
| `pyspark-client` | `[INVALID_CONNECT_URL] The URL must start with 'sc://'` |

`pyspark-client` accepts only `sc://` URLs, because it has no JVM in which to start a server. This
matches the installation documentation, which describes `pyspark-client` as supporting
`spark.remote` with a connection URI only.

### Port 15002

In-process mode requires port 15002. If another process holds it, startup fails:

```
java.net.BindException: Failed to bind to address 0.0.0.0/0.0.0.0:15002:
Service 'org.apache.spark.sql.connect.service.SparkConnectService' failed after 0 retries
```

`spark.connect.grpc.port.maxRetries` defaults to 0, so Spark does not try another port.

Setting `spark.connect.grpc.binding.port` to a different port does not resolve this. The setting
moves the server, but the client in `local[*]` mode connects to `localhost:15002` regardless, and the
two no longer meet. **Verified** with `binding.port=15014`:

```
client endpoint      localhost:15002     <- not 15014
server version       4.1.0               <- a Spark 4.1 container, inside a 4.2.0 virtualenv
is anything listening on 15014? YES      <- the 4.2.0 server just started, idle
```

No error was raised. The session was connected to an unrelated Spark 4.1 server that held port 15002,
while the 4.2.0 server the process had started received no requests. A test suite using that session
would have run against the wrong Spark version.

The `BindException` is therefore the preferable failure. To resolve it, find and stop the process
holding the port:

```bash
ss -ltnp | grep 15002
docker ps --format '{{.Names}}\t{{.Ports}}' | grep 15002
```

If that process is a Connect server that should keep running, use topology 2 or 3 and connect to it
by URL instead. `examples/04_in_process.py` checks the port before starting a local server, and exits
with an explanation if the port is in use.

To guard against connecting to an unexpected server, check the version that the server on port
15002 reports:

```bash
python tools/doctor.py --remote sc://localhost:15002 --expect-version 4.2
```

The doctor reports `FAIL` for the version check, and exits with status 1, when the server's version
does not start with the expected one.

> **Note:** When a Connect session reports an unexpected Spark version, check which server the client
> is connected to before investigating the local installation. A classic session in the same
> environment that reports the expected version, while the Connect session does not, indicates this
> port conflict.

---

## 2. Same Host, Separate Process

A long-lived server on the same machine. The session survives a client restart, which is the main
reason to prefer this over in-process mode during development.

```bash
./sbin/start-connect-server.sh \
  --conf spark.connect.grpc.binding.port=15002
# To stop it:
./sbin/stop-connect-server.sh
```

Add `--wait` to keep the server in the foreground (it sets `SPARK_NO_DAEMONIZE=1`). This is
appropriate under a process supervisor or in a container, but not from an interactive shell.

Clients connect to `sc://localhost:15002`. Without a binding setting the server listens on all
interfaces, so other hosts can reach it too; add `--conf spark.connect.grpc.binding.host=127.0.0.1`
to accept only local clients.

Instructions written for Spark 3.5 add `--packages org.apache.spark:spark-connect_2.13:<version>`.
Since Spark 4.0 the server is included in the distribution and the documentation no longer uses the
flag; omit it.

---

## 3. Remote Standalone Cluster

The most common production arrangement: one Connect server in front of one cluster, used by several
clients.

```bash
./sbin/start-connect-server.sh --wait \
  --master spark://master-host:7077 \
  --conf spark.connect.grpc.binding.host=0.0.0.0 \
  --conf spark.connect.grpc.binding.port=15002 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
  --jars /opt/jars/iceberg-runtime.jar
```

Four aspects of this command are significant:

- **`binding.host=0.0.0.0`.** States the listening interface explicitly. A 4.2.0 server with no
  binding setting already listens on all interfaces (`VERIFIED_FACTS.md` §2); a specific address
  restricts it.
- **Catalog and extension configuration is set on the server.** These values are class names, and
  the client has no JVM in which to load them.
- **`--jars` is sufficient for `spark.sql.extensions`.** See the note on the startup warning below.
- **Credentials are held by the server.** The catalog password is configured on the server and is not
  needed by clients.

**Startup warning.** A Connect server started with `--jars` and `spark.sql.extensions` logs the
following at WARN level:

```
WARN SparkSession: Cannot use org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
to configure session extensions.
java.lang.ClassNotFoundException: org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
```

The warning does not indicate a problem. **Verified** against 4.2.0: with `--jars` alone, a client
session on the server runs `MERGE INTO` and `CALL <catalog>.system.expire_snapshots(...)`, both of
which require the Iceberg extensions. The warning applies to the server's bootstrap session, which is
created before `--jars` are on the class path; client sessions created afterward load the extensions.
Adding `--driver-class-path` does not change the warning. Verify extensions by using them rather than
by reading the log.

`compose.yaml` at the project root runs this topology in containers: a standalone master, one
worker, and a Connect server started with `--wait`.

---

## 4. Kubernetes

Manifests are in [`kubernetes/`](kubernetes/). **Untested here**; no Kubernetes cluster was available.
The manifests use a plain Deployment and Service. The Apache Spark Kubernetes Operator is an
alternative: its example manifest runs the Connect server as a `SparkApplication`.

Table 4 lists ways to expose the server.

*Table 4. Exposing a Connect server on Kubernetes*

| Method | Client URL | Notes |
|---|---|---|
| `ClusterIP` | `sc://spark-connect.ns.svc:15002` | In-cluster access only; the appropriate default |
| `NodePort` | `sc://<node-ip>:30002` | Suitable for development |
| `LoadBalancer` | `sc://<lb-host>:15002` | Requires TLS and authentication in front of it |
| gRPC ingress | `sc://connect.example.com:443` | Requires HTTP/2; the place to enforce authentication |

**Each Connect server holds its own session state.** A Service that load-balances across several
Connect pods can route a client's later requests to a pod that does not hold its session. Use session
affinity, or run one replica per logical service. This follows from the session living on the server
(companion guide §2) rather than from Kubernetes itself.

**Spark Connect has no built-in authentication.** Per the overview documentation, it is intended to
run behind an authenticating gRPC proxy. Do not expose port 15002 publicly. Setting `token` in the
connection string enables TLS on the client side; authentication must still be enforced in front of
the server.

---

## 5. YARN Cluster Mode

Spark 4.2 can launch the Connect server inside a YARN application master
([SPARK-55239](https://issues.apache.org/jira/browse/SPARK-55239)). **Untested here.** The issue is
resolved with fix version 4.2.0, but it is not itemized in the 4.2.0 release notes.

```bash
./sbin/start-connect-server.sh --master yarn --deploy-mode cluster
```

In an existing Hadoop deployment, this gives the Connect server YARN's resource management and
restart behavior instead of requiring separate supervision.

---

## 6. Multiple Clusters

A client can route workloads across several servers by data size, cost or region, or during a
blue-green Spark upgrade. `examples/05_sessions.py` demonstrates the session behavior described
below.

```python
endpoints = {
    "small": "sc://connect-small:15002",   # interactive work
    "large": "sc://connect-large:15002",   # large workloads
}
```

**Session reuse (verified).** `getOrCreate()` returns the cached active session even when the builder
names a different URL:

```python
a = SparkSession.builder.remote("sc://good:15002").getOrCreate()
b = SparkSession.builder.remote("sc://127.0.0.1:1").getOrCreate()
assert b is a     # passes, with no error or warning
```

No server listens on port 1. In a process that uses several clusters, a request for a second cluster
therefore returns the session for the first, and calling `stop()` on that shared object ends the
session for all code in the process that holds it.

Use `builder.create()` to obtain a new server-side session. **Verified:** two sessions created with
`create()` against the same server can register the same temporary view name with different contents
without seeing each other's, and stopping one leaves the other running.

---

## Validating Any Topology

Since the topology is not visible from client code, validate it with `doctor`:

```bash
make doctor                                      # the endpoint in SPARK_REMOTE
python tools/doctor.py --remote sc://prod-connect:15002 --expect-version 4.2
```

It checks, in order: which PySpark distribution is installed and whether it includes JARs and Py4J;
whether anything is listening at the endpoint; whether the server reports the expected version;
whether the server runs SQL, returns Arrow results, reports `observe()` metrics and runs Python UDFs
with the client's Python minor version; and whether a session can ship a dependency with
`addArtifact`.

Exit status 0 means all checks passed, 1 means a check failed, and 2 means nothing was listening at
the endpoint. The command is suitable as a deployment gate in CI.
