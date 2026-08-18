# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Camada Gold
# MAGIC Modelo dimensional: `fct_trips` + 6 dimensoes.

# COMMAND ----------
# MAGIC %pip install -q PyYAML
# MAGIC %restart_python

# COMMAND ----------
import sys, os, uuid
sys.path.append(os.path.abspath("../src"))

from nyc_taxi.config import load_config
from nyc_taxi import gold, quality

cfg = load_config(path="../conf/pipeline.yml", profile="databricks")
gold.run(spark, cfg)
quality.run_expectations(spark, cfg, "gold.fct_trips", str(uuid.uuid4())[:8])

# COMMAND ----------
# MAGIC %md ### Consulta de negocio sobre o star schema

# COMMAND ----------
display(spark.sql(f"""
  SELECT t.day_period,
         l.borough                         AS bairro_origem,
         count(*)                          AS corridas,
         round(avg(f.trip_distance_km), 2) AS km_medio,
         round(avg(f.tip_pct) * 100, 1)    AS gorjeta_media_pct,
         round(sum(f.total_amount), 2)     AS receita
  FROM {cfg.table("gold", "fct_trips")} f
  JOIN {cfg.table("gold", "dim_time")} t     ON f.pickup_time_key = t.time_key
  JOIN {cfg.table("gold", "dim_location")} l ON f.pickup_location_key = l.location_key
  GROUP BY 1, 2
  ORDER BY receita DESC
  LIMIT 20
"""))
