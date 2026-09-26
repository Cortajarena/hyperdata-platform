.PHONY: up down build logs lint test dbt spark flink kind-up kind-down tf-init tf-plan tf-apply

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

# ---- GCP terraform (vars from infrastructure/gcp/gcp.tfvars if present) ----
TF_DIR := infrastructure/gcp
TFVARS := $(shell [ -f $(TF_DIR)/gcp.tfvars ] && echo "-var-file=$(TF_DIR)/gcp.tfvars")
TF     := terraform -chdir=$(TF_DIR)

tf-init:
	$(TF) init -reconfigure \
	    -backend-config="bucket=$(shell grep '^state_bucket' $(TF_DIR)/gcp.tfvars | cut -d= -f2 | tr -d '" ')"

tf-plan:
	$(TF) plan $(TFVARS)

tf-apply:
	$(TF) apply $(TFVARS) -auto-approve=false
