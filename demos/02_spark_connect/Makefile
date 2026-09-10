# Spark Connect demo. Run `make help` to list targets.

SHELL := /bin/bash
-include .env

CONNECT_PORT ?= 15002
CONNECT_41_PORT ?= 15096
SPARK_REMOTE ?= sc://localhost:$(CONNECT_PORT)
export SPARK_REMOTE

PY      := .venv/bin/python
PYFULL  := .venv-full/bin/python
PY41    := .venv-41/bin/python
COMPOSE := docker compose
OUT     := /opt/spark/work-dir/out

.DEFAULT_GOAL := help
.PHONY: help setup up examples doctor audit pipeline test down teleprompter check matrix clean

help:  ## List targets
	@grep -hE '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*## "};{printf "  %-14s %s\n",$$1,$$2}'

# ---- on camera ---------------------------------------------------------------------------------

setup:  ## Create .venv (pyspark-client) and .venv-full (pyspark[connect]) with uv
	uv sync
	UV_PROJECT_ENVIRONMENT=.venv-full uv sync --no-default-groups --group full --group dev

up:  ## Start the Spark master, worker and Connect server
	$(COMPOSE) up -d --build --wait spark-master spark-worker spark-connect
	@echo "Spark Connect: $(SPARK_REMOTE)"

examples:  ## Run every example in order
	@echo "== 01 classic";    env -u SPARK_REMOTE $(PYFULL) examples/01_classic_or_connect.py classic
	@echo "== 01 connect";    $(PY) examples/01_classic_or_connect.py connect
	@echo "== 02";            $(PY) examples/02_first_query.py
	@echo "== 03 client";     $(PY) examples/03_which_client.py
	@echo "== 03 full";       $(PYFULL) examples/03_which_client.py
	@echo "== 04";            env -u SPARK_REMOTE $(PYFULL) examples/04_in_process.py
	@echo "== 05";            $(PY) examples/05_sessions.py
	@echo "== 06";            $(PY) examples/06_python_versions.py
	@echo "== 07";            $(PY) examples/07_connection_strings.py
	@echo "== 08";            $(PY) examples/08_add_artifact.py
	@echo "== 09";            $(PY) examples/09_api_differences.py
	@echo "== 10 classic";    env -u SPARK_REMOTE $(PYFULL) examples/10_eager_and_lazy.py classic
	@echo "== 10 connect";    $(PY) examples/10_eager_and_lazy.py connect

doctor:  ## Check the client, the server and its capabilities
	$(PY) tools/doctor.py --remote $(SPARK_REMOTE) --expect-version 4.2

audit:  ## Audit the pipeline and example 10 for Spark Connect compatibility
	$(PY) tools/compat_audit.py pipeline/pipeline_before.py pipeline/pipeline_after.py \
		examples/10_eager_and_lazy.py --fail-on NONE

pipeline:  ## Run the pipeline on Spark Classic, then over Spark Connect, then compare outputs
	$(COMPOSE) exec -T spark-connect /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 --conf spark.cores.max=1 \
		--conf spark.driver.host=spark-connect \
		/demo/pipeline/pipeline_before.py --output $(OUT)/classic
	SPARK_REMOTE="$(SPARK_REMOTE)/;user_agent=orders-pipeline" \
		$(PY) pipeline/pipeline_after.py --output $(OUT)/connect
	$(PY) pipeline/parity.py --left $(OUT)/classic --right $(OUT)/connect

test:  ## Run unit tests, then Spark tests on Classic and on Connect
	$(PY) -m pytest tests/unit
	env -u SPARK_REMOTE $(PYFULL) -m pytest tests/spark
	$(PY) -m pytest tests/spark tests/connect

down:  ## Stop the cluster
	$(COMPOSE) down

# ---- off camera --------------------------------------------------------------------------------

teleprompter:  ## Export the video script's spoken text for a teleprompter app
	python3 tools/teleprompter_export.py video_spark_connect.md video_spark_connect_teleprompter.txt

check:  ## Check documentation references, launch configurations and style rules
	python3 tools/check.py

matrix:  ## Run client and server version tests (4.1.2 and 4.2.0)
	test -x $(PY41) || { uv venv .venv-41 -p 3.10 && uv pip install -p $(PY41) "pyspark-client==4.1.2" "pandas<3"; }
	$(COMPOSE) --profile versions up -d --wait spark-connect-41
	for run in 1 2; do \
	  $(PY)   extras/version_matrix/matrix.py --cell A --run $$run --remote sc://localhost:$(CONNECT_PORT) && \
	  $(PY)   extras/version_matrix/matrix.py --cell B --run $$run --remote sc://localhost:$(CONNECT_41_PORT) && \
	  $(PY41) extras/version_matrix/matrix.py --cell C --run $$run --remote sc://localhost:$(CONNECT_PORT) && \
	  $(PY41) extras/version_matrix/matrix.py --cell D --run $$run --remote sc://localhost:$(CONNECT_41_PORT) \
	  || exit 1; \
	done
	$(PY) extras/version_matrix/matrix.py --report
	$(COMPOSE) --profile versions stop spark-connect-41

clean:  ## Stop the cluster, remove its volume and local caches
	$(COMPOSE) --profile versions down -v
	find . -path ./.venv -prune -o -path ./.venv-full -prune -o -path ./.venv-41 -prune -o \
		\( -name __pycache__ -o -name .pytest_cache -o -name .ruff_cache \) -type d -print \
		| xargs rm -rf
