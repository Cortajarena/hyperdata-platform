.PHONY: up down build logs lint test dbt spark flink kind-up kind-down

COMPOSE ?= docker compose

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

build:
	$(COMPOSE) build

logs:
	$(COMPOSE) logs -f

lint:
	ruff check .

test:
	pytest

dbt:
	$(COMPOSE) exec airflow-scheduler dbt run --project-dir /opt/airflow/dbt

spark:
	$(COMPOSE) exec spark-master spark-submit /opt/spark/workloads/jobs/

flink:
	$(COMPOSE) exec flink-jobmanager flink run /opt/flink/usrlib/

kind-up:
	bash infrastructure/kind/up.sh

kind-down:
	kind delete cluster --name hyperdata
