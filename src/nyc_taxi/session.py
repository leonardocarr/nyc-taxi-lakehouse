"""Fabrica de SparkSession.

Dentro do Databricks a sessao ja existe e e retornada como esta. Fora dele,
sobe um Spark local com Delta Lake configurado -- o mesmo codigo de
transformacao roda nos dois lugares, o que e o que torna os testes possiveis.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

from pyspark.sql import SparkSession

from .config import Config

log = logging.getLogger(__name__)


def running_on_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def get_spark(cfg: Config, app_name: str = "nyc-taxi-lakehouse") -> SparkSession:
    active = SparkSession.getActiveSession()
    if running_on_databricks() and active is not None:
        log.info("Usando a SparkSession do Databricks Runtime")
        return active

    warehouse = Path(cfg.warehouse_dir or "./lakehouse").resolve()
    warehouse.mkdir(parents=True, exist_ok=True)

    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("javax.jdo.option.ConnectionURL",
                f"jdbc:derby:;databaseName={warehouse / 'metastore_db'};create=true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.getenv("SHUFFLE_PARTITIONS", "8"))
        # Delta: evita o problema de "small files" sem precisar de OPTIMIZE manual
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoCompact.enabled", "true")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "false")
    )

    try:  # configure_spark_with_delta_pip resolve o jar do Delta via Maven
        from delta import configure_spark_with_delta_pip

        builder = configure_spark_with_delta_pip(builder)
    except ImportError:  # pragma: no cover
        log.warning("delta-spark nao instalado; assumindo jars ja no classpath")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return spark


def ensure_schemas(spark: SparkSession, cfg: Config) -> None:
    """Cria catalogo (quando UC) e schemas das tres camadas se nao existirem."""
    if cfg.catalog:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {cfg.catalog}")
    for layer in ("bronze", "silver", "gold"):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.schema(layer)}")
