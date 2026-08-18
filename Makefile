.PHONY: help install test lint run clean

VENV ?= .venv
PY   := $(VENV)/bin/python

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

install:  ## cria o venv e instala as dependencias
	python3 -m venv $(VENV) && $(PY) -m pip install -q -U pip && $(PY) -m pip install -r requirements-dev.txt

test:  ## roda a suite de testes
	PYSPARK_PYTHON=$(PY) $(PY) -m pytest -q

lint:  ## checa estilo
	$(VENV)/bin/ruff check src tests

run:  ## executa o pipeline inteiro no perfil local
	PYSPARK_PYTHON=$(PY) $(PY) -m nyc_taxi.run --layer all --profile local

run-bronze:
	PYSPARK_PYTHON=$(PY) $(PY) -m nyc_taxi.run --layer bronze --profile local

deploy:  ## publica o bundle no Databricks (target dev)
	databricks bundle validate -t dev && databricks bundle deploy -t dev

clean:  ## apaga o lakehouse local e caches
	rm -rf lakehouse spark-warehouse metastore_db derby.log .pytest_cache .ruff_cache
