# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Camada Silver
# MAGIC Conformacao, deduplicacao e validacao. Linhas rejeitadas vao para quarentena.

# COMMAND ----------
# MAGIC %pip install -q PyYAML
# MAGIC %restart_python

# COMMAND ----------
import sys, os, uuid
sys.path.append(os.path.abspath("../src"))

from nyc_taxi.config import load_config
from nyc_taxi import silver, quality

cfg = load_config(path="../conf/pipeline.yml", profile="databricks")
silver.run(spark, cfg)
quality.run_expectations(spark, cfg, "silver.fct_source_trips", str(uuid.uuid4())[:8])

# COMMAND ----------
# MAGIC %md ### O que foi para a quarentena, e por que?

# COMMAND ----------
display(spark.sql(f"""
  SELECT motivo, count(*) AS linhas
  FROM {cfg.table("silver", "fct_source_trips_quarantine")}
  LATERAL VIEW explode(_violations) v AS motivo
  GROUP BY motivo ORDER BY linhas DESC
"""))
