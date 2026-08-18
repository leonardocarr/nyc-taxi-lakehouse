"""Camada Bronze -- ingestao crua, sem regra de negocio.

Principio: bronze e o "backup imutavel" da origem. Nada e filtrado, nada e
corrigido, nenhum tipo e forcado alem do que o arquivo ja traz. O que se
adiciona sao apenas colunas de linhagem (``_ingested_at``, ``_source_file``,
``_batch_id``) e a particao logica de carga.

Se uma regra de negocio mudar amanha, da para reprocessar silver e gold
inteiros a partir daqui sem tocar na origem de novo.
"""

from __future__ import annotations

import logging
import urllib.request
import uuid
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from . import io
from .config import Config

log = logging.getLogger(__name__)

TRIPS_TABLE = "raw_trips"
ZONES_TABLE = "raw_taxi_zones"

ZONES_SCHEMA = StructType(
    [
        StructField("LocationID", IntegerType(), True),
        StructField("Borough", StringType(), True),
        StructField("Zone", StringType(), True),
        StructField("service_zone", StringType(), True),
    ]
)


# --------------------------------------------------------------------------- #
# Resolucao da origem
# --------------------------------------------------------------------------- #
def _download(url: str, dest: Path) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists() and dest.stat().st_size > 0:
        log.info("Cache local encontrado: %s", dest.name)
        return dest
    log.info("Baixando %s", url)
    urllib.request.urlretrieve(url, dest)  # noqa: S310 - URL fixa e publica
    return dest


def _trips_path(cfg: Config, month: str) -> str:
    """Caminho do arquivo de um mes, conforme o modo de origem configurado."""
    filename = f"{cfg.service}_tripdata_{month}.parquet"

    if cfg.source.mode == "http":
        local = Path(cfg.source.volume_path) / filename
        return str(_download(f"{cfg.base_url}/{filename}", local))

    if cfg.source.mode == "volume":
        # No Databricks Free Edition a saida para internet e restrita: suba os
        # parquets uma vez para um UC Volume e aponte volume_path para ele.
        return f"{cfg.source.volume_path.rstrip('/')}/{filename}"

    raise ValueError(f"source.mode invalido para trips: {cfg.source.mode}")


# --------------------------------------------------------------------------- #
# Ingestao
# --------------------------------------------------------------------------- #
def ingest_trips(spark: SparkSession, cfg: Config, month: str) -> int:
    """Ingere um mes de corridas. Idempotente: reexecutar substitui so esse mes."""
    table = cfg.table("bronze", TRIPS_TABLE)
    batch_id = str(uuid.uuid4())

    if cfg.source.mode == "samples":
        raw = spark.table("samples.nyctaxi.trips")
    else:
        raw = spark.read.parquet(_trips_path(cfg, month))

    df: DataFrame = (
        raw.withColumn("_source_file", F.input_file_name())
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("load_month", F.lit(month))
    )

    io.overwrite_partitions(
        df,
        table,
        partition_by=["load_month"],
        replace_where=f"load_month = '{month}'",
        comment="Corridas de taxi da NYC TLC exatamente como vieram da origem",
    )

    count = spark.table(table).where(F.col("load_month") == month).count()
    log.info("bronze.%s <- %s: %s linhas (batch %s)", TRIPS_TABLE, month, count, batch_id)
    return count


def ingest_zones(spark: SparkSession, cfg: Config) -> int:
    """Ingere o lookup de zonas -- a origem da dimensao de localizacao."""
    table = cfg.table("bronze", ZONES_TABLE)

    if cfg.source.mode == "http":
        path = str(_download(cfg.zones_url, Path(cfg.source.zones_path)))
    else:
        path = cfg.source.zones_path

    df = (
        spark.read.option("header", "true")
        .schema(ZONES_SCHEMA)
        .csv(path)
        .withColumn("_ingested_at", F.current_timestamp())
    )

    io.overwrite_partitions(
        df, table, comment="Lookup oficial de zonas de taxi da NYC TLC"
    )
    count = spark.table(table).count()
    log.info("bronze.%s: %s zonas", ZONES_TABLE, count)
    return count


def run(spark: SparkSession, cfg: Config) -> dict[str, int]:
    """Executa a camada bronze inteira para os meses configurados."""
    results = {"zones": ingest_zones(spark, cfg)}
    for month in cfg.months:
        results[month] = ingest_trips(spark, cfg, month)
    return results
