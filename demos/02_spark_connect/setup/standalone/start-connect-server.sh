#!/usr/bin/env bash
# Start a Spark Connect server from a Spark distribution (setup/TOPOLOGIES.md, topologies 2 and 3).
#
#   ./start-connect-server.sh                                    # executes in the server process
#   SPARK_MASTER=spark://master:7077 ./start-connect-server.sh   # executes on a standalone cluster
#
# Settings come from the environment variables below. Further arguments are passed to Spark's
# sbin/start-connect-server.sh.
set -euo pipefail

SPARK_HOME="${SPARK_HOME:-/opt/spark}"
CONNECT_PORT="${CONNECT_PORT:-15002}"
# With no binding setting, a 4.2.0 server listens on all interfaces (companion guide §4). This
# script defaults to loopback; set BIND_HOST=0.0.0.0 to accept connections from other hosts.
BIND_HOST="${BIND_HOST:-127.0.0.1}"
SPARK_MASTER="${SPARK_MASTER:-}"      # empty: local[*] inside the server process
EXTRA_JARS="${EXTRA_JARS:-}"          # comma-separated, for example an Iceberg runtime

if [ ! -x "$SPARK_HOME/sbin/start-connect-server.sh" ]; then
  echo "No Spark distribution at SPARK_HOME=$SPARK_HOME" >&2
  echo "Download one: https://spark.apache.org/downloads.html" >&2
  exit 2
fi

args=(
  # Stay in the foreground, as a process supervisor or a container expects. Without --wait, the
  # script starts the server in the background and returns.
  --wait
  --conf "spark.connect.grpc.binding.host=${BIND_HOST}"
  --conf "spark.connect.grpc.binding.port=${CONNECT_PORT}"
)

# spark.connect.grpc.port.maxRetries defaults to 0, so if the port is in use the server fails to
# start rather than choosing another port.
#
# The Spark 4.2 distribution includes the server in jars/spark-connect_2.13-4.2.0.jar, so the
# --packages org.apache.spark:spark-connect_2.13:<version> option from Spark 3.5 instructions is
# not needed.

if [ -n "$SPARK_MASTER" ]; then
  args+=(--master "$SPARK_MASTER")
fi

if [ -n "$EXTRA_JARS" ]; then
  # --jars is enough for spark.sql.extensions: against 4.2.0, client sessions ran MERGE INTO and
  # CALL <catalog>.system.* with --jars alone (setup/TOPOLOGIES.md, topology 3). The server logs a
  # ClassNotFoundException for the extension class at WARN during startup. It concerns the
  # server's bootstrap session, created before the jars are added; client sessions are not
  # affected. Check extensions by using them, not by reading the log.
  args+=(--jars "$EXTRA_JARS")
fi

echo "starting Spark Connect server"
echo "  SPARK_HOME   $SPARK_HOME"
echo "  listening    ${BIND_HOST}:${CONNECT_PORT}"
echo "  master       ${SPARK_MASTER:-local[*] (in the server process)}"
echo "  client URL   sc://${BIND_HOST/0.0.0.0/<this-host>}:${CONNECT_PORT}"
echo

exec "$SPARK_HOME/sbin/start-connect-server.sh" "${args[@]}" "$@"
