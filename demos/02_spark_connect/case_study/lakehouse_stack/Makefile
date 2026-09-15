# Case study: migrating the lakehouse-stack pipeline to Spark Connect.
#
# Requires ~/lakehouse-stack data and a locally built Iceberg runtime for Spark 4.2
# (BUILDING_ICEBERG_FOR_SPARK_4_2.md). Run from this directory, or with
# `make -C case_study/lakehouse_stack <target>` from the project root.

SHELL       := /bin/bash
PY          := ../../.venv/bin/python
AUDIT       := ../../tools/compat_audit.py
WAREHOUSE   := $(CURDIR)/.warehouse
ICEBERG_JAR ?= $(HOME)/lakehouse-stack/jars/iceberg-spark-runtime-4.2_2.13-8f28a86-localbuild.jar
LH_DATA     := $(HOME)/lakehouse-stack/data
# The 90-day orders file is 7.3 GB. The 74 MB one-day file is mounted at the same path, so both
# pipeline files run unchanged.
ORDERS_SRC  := $(LH_DATA)/events/orders_1d.parquet
REMOTE      := sc://localhost:15003
CONTAINER   := case-study-connect42

.DEFAULT_GOAL := help
.PHONY: help audit up verify-iceberg build-iceberg down migrate-before migrate-after parity clean

help:  ## List targets
	@grep -hE '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*## "};{printf "  %-16s %s\n",$$1,$$2}'

audit:  ## Audit the lakehouse-stack scripts for Spark Connect compatibility
	$(PY) $(AUDIT) $(HOME)/lakehouse-stack/scripts --fail-on NONE

up:  ## Start a Connect server with two Iceberg warehouses on port 15003
	@test -f "$(ICEBERG_JAR)" || { echo "missing $(ICEBERG_JAR); see BUILDING_ICEBERG_FOR_SPARK_4_2.md"; exit 1; }
	mkdir -p $(WAREHOUSE)/warehouse $(WAREHOUSE)/warehouse_after $(WAREHOUSE)/jars
	# The container writes as uid 185, so permissions are set from a root container.
	docker run --rm -u 0 -v $(WAREHOUSE):/d alpine chmod -R 777 /d
	cp "$(ICEBERG_JAR)" $(WAREHOUSE)/jars/iceberg.jar
	-docker rm -f $(CONTAINER) 2>/dev/null
	docker run -d --name $(CONTAINER) \
	  -p 127.0.0.1:15003:15002 \
	  -v $(WAREHOUSE)/warehouse:/warehouse \
	  -v $(WAREHOUSE)/warehouse_after:/warehouse_after \
	  -v $(WAREHOUSE)/jars:/jars:ro \
	  -v $(CURDIR):/demo:ro \
	  -v $(LH_DATA)/dimensions:/data/dimensions:ro \
	  -v $(ORDERS_SRC):/data/events/orders_90d.parquet:ro \
	  --entrypoint bash apache/spark:4.2.0-scala2.13-java21-python3-ubuntu -c \
	  '/opt/spark/sbin/start-connect-server.sh --wait \
	     --jars /jars/iceberg.jar \
	     --conf spark.connect.grpc.binding.address=0.0.0.0 \
	     --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
	     --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
	     --conf spark.sql.catalog.iceberg.type=hadoop \
	     --conf spark.sql.catalog.iceberg.warehouse=/warehouse_after \
	     --conf spark.sql.catalog.iceberg_before=org.apache.iceberg.spark.SparkCatalog \
	     --conf spark.sql.catalog.iceberg_before.type=hadoop \
	     --conf spark.sql.catalog.iceberg_before.warehouse=/warehouse \
	     --conf spark.driver.memory=3g'
	@for i in $$(seq 1 40); do \
	  docker logs $(CONTAINER) 2>&1 | grep -q "Spark Connect server started" && { echo "server started at $(REMOTE)"; exit 0; }; \
	  sleep 5; done; echo "server did not start"; exit 1

verify-iceberg:  ## Check the Iceberg runtime (views are skipped on a Hadoop catalog)
	$(PY) verify_iceberg_runtime.py $(REMOTE)

build-iceberg:  ## Build the Iceberg runtime for Spark 4.2 from source in ~/iceberg-src
	@test -d $(HOME)/iceberg-src || { echo "no ~/iceberg-src checkout"; exit 1; }
	cd $(HOME)/iceberg-src && ./gradlew --no-daemon --no-build-cache \
		-DsparkVersions=4.2 -DscalaVersion=2.13 \
		-DhiveVersions= -DflinkVersions= -DkafkaVersions= \
		:iceberg-spark:iceberg-spark-runtime-4.2_2.13:shadowJar
	@ls -la $(HOME)/iceberg-src/spark/v4.2/spark-runtime/build/libs/*.jar

down:  ## Stop the case study server (the warehouses are kept)
	-docker rm -f $(CONTAINER)

migrate-before:  ## Run the original pipeline on Spark Classic, writing to /warehouse
	docker exec $(CONTAINER) /opt/spark/bin/spark-submit \
	  --jars /jars/iceberg.jar \
	  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
	  --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
	  --conf spark.sql.catalog.iceberg.type=hadoop \
	  --conf spark.sql.catalog.iceberg.warehouse=/warehouse \
	  --conf spark.driver.memory=3g \
	  /demo/pipeline_before.py

migrate-after:  ## Run the migrated pipeline over Spark Connect, writing to /warehouse_after
	SPARK_REMOTE=$(REMOTE) PIPELINE_CATALOG=iceberg $(PY) pipeline_after.py

parity:  ## Compare the ten tables written by the two runs
	SPARK_REMOTE=$(REMOTE) $(PY) parity.py --left iceberg_before --right iceberg

clean:  ## Remove the warehouses (written by uid 185, so removed from a root container)
	docker run --rm -u 0 -v $(WAREHOUSE):/d alpine rm -rf /d/warehouse /d/warehouse_after
