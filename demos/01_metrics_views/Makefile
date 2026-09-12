# Metric views demo. Run `make help` to list targets.

SHELL := /bin/bash
-include .env

CONNECT_PORT ?= 15002
SPARK_REMOTE ?= sc://localhost:$(CONNECT_PORT)
export SPARK_REMOTE

PY      := .venv/bin/python
PYFULL  := .venv-full/bin/python
COMPOSE := docker compose

.DEFAULT_GOAL := help
.PHONY: help setup data up doctor examples reel test down probe teleprompter check clean

help:  ## List targets
	@grep -hE '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*## "};{printf "  %-14s %s\n",$$1,$$2}'

# ---- on camera ---------------------------------------------------------------------------------

setup:  ## Create .venv (pyspark-client) and .venv-full (pyspark[connect]) with uv
	uv sync
	UV_PROJECT_ENVIRONMENT=.venv-full uv sync --no-default-groups --group full --group dev

data: data/sessions.parquet  ## Generate the dataset into data/

data/sessions.parquet: tools/generate_data.py
	$(PY) tools/generate_data.py

# The dataset is generated first, so that Docker does not create data/ as root for the mount.
up: data/sessions.parquet  ## Start the Spark Connect server
	$(COMPOSE) up -d --wait spark-connect
	@echo "Spark Connect: $(SPARK_REMOTE)"

doctor:  ## Check the client, the server, the mounted dataset and a metric view
	$(PY) tools/doctor.py --remote $(SPARK_REMOTE) --expect-version 4.2

examples:  ## Run every example in order
	@echo "== 01"; $(PY) examples/01_average_of_ratios.py
	@echo "== 02"; $(PY) examples/02_summing_distinct_counts.py
	@echo "== 03"; $(PY) examples/03_create_metric_view.py
	@echo "== 04"; $(PY) examples/04_measure_at_every_grain.py
	@echo "== 05"; $(PY) examples/05_what_it_does_not_prevent.py
	@echo "== 06"; $(PY) examples/06_definition_rules.py
	@echo "== 07"; $(PY) examples/07_view_lifecycle.py
	@echo "== 08"; $(PY) examples/08_modeled_source.py

reel:  ## Run the examples shown in the short video
	@echo "== 01"; $(PY) examples/01_average_of_ratios.py
	@echo "== 02"; $(PY) examples/02_summing_distinct_counts.py
	@echo "== 03"; $(PY) examples/03_create_metric_view.py
	@echo "== 04"; $(PY) examples/04_measure_at_every_grain.py
	@echo "== 05"; $(PY) examples/05_what_it_does_not_prevent.py

test:  ## Run unit tests, then Spark tests on Classic and on Connect
	$(PY) -m pytest tests/unit
	env -u SPARK_REMOTE $(PYFULL) -m pytest tests/spark
	$(PY) -m pytest tests/spark tests/connect

down:  ## Stop the server
	$(COMPOSE) down

# ---- off camera --------------------------------------------------------------------------------

probe:  ## Record metric view behavior: two runs on the server, one on Spark Classic
	$(PY) grammar_probe/probe.py --run connect-1
	$(PY) grammar_probe/probe.py --run connect-2
	env -u SPARK_REMOTE $(PYFULL) grammar_probe/probe.py --run classic
	$(PY) grammar_probe/probe.py --report

teleprompter:  ## Export the spoken text of both video scripts for a teleprompter app
	python3 tools/teleprompter_export.py video_metric_views.md video_metric_views_teleprompter.txt
	python3 tools/teleprompter_export.py video_metric_views_reel.md \
		video_metric_views_reel_teleprompter.txt

check:  ## Check documentation references, figures, launch configurations and style rules
	python3 tools/check.py

clean:  ## Stop the server and remove local caches (keeps data/ and the environments)
	$(COMPOSE) down -v
	find . -path ./.venv -prune -o -path ./.venv-full -prune -o \
		\( -name __pycache__ -o -name .pytest_cache -o -name .ruff_cache \) -type d -print \
		| xargs rm -rf
