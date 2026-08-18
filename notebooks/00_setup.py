# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup do ambiente
# MAGIC
# MAGIC Cria catalogo, schemas e o Volume de landing. Rode **uma vez** por workspace.
# MAGIC
# MAGIC No Databricks Free Edition a saida para a internet e restrita, entao os
# MAGIC parquets da TLC precisam ser enviados manualmente para o Volume
# MAGIC (`Catalog > nyc_taxi > bronze > landing > Upload`), baixando antes de
# MAGIC https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

# COMMAND ----------

CATALOG = dbutils.widgets.get("catalog") if "catalog" in [w.name for w in dbutils.widgets.getAll()] else "nyc_taxi"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
for schema in ("bronze", "silver", "gold"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.bronze.landing")
print(f"Pronto. Envie os parquets para /Volumes/{CATALOG}/bronze/landing")

# COMMAND ----------

display(spark.sql(f"SHOW SCHEMAS IN {CATALOG}"))
