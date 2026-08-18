# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Camada Bronze
# MAGIC Ingestao crua dos parquets da TLC. Nenhuma regra de negocio aqui.

# COMMAND ----------
# MAGIC %pip install -q PyYAML
# MAGIC %restart_python

# COMMAND ----------
import sys, os
sys.path.append(os.path.abspath("../src"))
os.environ["NYC_TAXI_PROFILE"] = "databricks"

from nyc_taxi.config import load_config
from nyc_taxi.session import ensure_schemas
from nyc_taxi import bronze

cfg = load_config(path="../conf/pipeline.yml", profile="databricks")
ensure_schemas(spark, cfg)
bronze.run(spark, cfg)

# COMMAND ----------
# MAGIC %md ### Linhagem: de qual arquivo veio cada lote?

# COMMAND ----------
display(spark.sql(f"""
  SELECT load_month, _batch_id, min(_ingested_at) AS ingerido_em, count(*) AS linhas
  FROM {cfg.table("bronze", "raw_trips")}
  GROUP BY load_month, _batch_id ORDER BY load_month
"""))
