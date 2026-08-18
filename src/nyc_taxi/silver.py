"""Camada Silver -- conformada, tipada, deduplicada e validada.

Aqui e onde a regra de negocio entra. As funcoes de transformacao sao puras
(DataFrame -> DataFrame) justamente para poderem ser testadas com dados
sinteticos sem subir nada de Databricks.

Decisao de projeto importante: linhas invalidas **nao sao descartadas**. Elas
vao para ``silver.fct_source_trips_quarantine`` com a lista de regras violadas. Um
pipeline que joga dado fora silenciosamente e um pipeline que ninguem consegue
auditar.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from . import io
from .bronze import TRIPS_TABLE, ZONES_TABLE
from .config import Config, Rule

log = logging.getLogger(__name__)

TRIPS_TABLE_SILVER = "fct_source_trips"
QUARANTINE_TABLE = "fct_source_trips_quarantine"
ZONES_TABLE_SILVER = "dim_source_zones"

MILES_TO_KM = 1.609344

# A TLC troca a capitalizacao das colunas entre anos (Airport_fee vs airport_fee,
# VendorID vs vendorid). O mapa e resolvido case-insensitive.
COLUMN_MAP = {
    "vendorid": "vendor_id",
    "tpep_pickup_datetime": "pickup_datetime",
    "lpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "lpep_dropoff_datetime": "dropoff_datetime",
    "passenger_count": "passenger_count",
    "trip_distance": "trip_distance_mi",
    "ratecodeid": "rate_code_id",
    "store_and_fwd_flag": "store_and_fwd_flag",
    "pulocationid": "pu_location_id",
    "dolocationid": "do_location_id",
    "payment_type": "payment_type_id",
    "fare_amount": "fare_amount",
    "extra": "extra_amount",
    "mta_tax": "mta_tax_amount",
    "tip_amount": "tip_amount",
    "tolls_amount": "tolls_amount",
    "improvement_surcharge": "improvement_surcharge_amount",
    "total_amount": "total_amount",
    "congestion_surcharge": "congestion_surcharge_amount",
    "airport_fee": "airport_fee_amount",
    "cbd_congestion_fee": "cbd_congestion_fee_amount",
    "load_month": "load_month",
    "_ingested_at": "_ingested_at",
    "_source_file": "_source_file",
    "_batch_id": "_batch_id",
}

NUMERIC_COLUMNS = [
    "fare_amount",
    "extra_amount",
    "mta_tax_amount",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge_amount",
    "total_amount",
    "congestion_surcharge_amount",
    "airport_fee_amount",
    "cbd_congestion_fee_amount",
]

# Chave de negocio da corrida: a TLC nao fornece um id, entao ele e derivado.
# Duas linhas com estes mesmos valores sao, para todos os efeitos, a mesma corrida.
NATURAL_KEY = [
    "vendor_id",
    "pickup_datetime",
    "dropoff_datetime",
    "pu_location_id",
    "do_location_id",
    "trip_distance_mi",
    "total_amount",
]


# --------------------------------------------------------------------------- #
# Transformacoes puras
# --------------------------------------------------------------------------- #
def normalize_columns(df: DataFrame) -> DataFrame:
    """Renomeia para snake_case canonico e descarta colunas desconhecidas."""
    selected, seen = [], set()
    for col in df.columns:
        target = COLUMN_MAP.get(col.lower())
        if target and target not in seen:
            selected.append(F.col(f"`{col}`").alias(target))
            seen.add(target)
    out = df.select(*selected)

    # Colunas que so existem em anos recentes: cria como nulo para manter o
    # schema estavel ao longo de todo o historico.
    for canonical in set(COLUMN_MAP.values()):
        if canonical not in out.columns:
            out = out.withColumn(canonical, F.lit(None).cast("double"))
    return out


def cast_types(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("vendor_id", F.col("vendor_id").cast("int"))
        .withColumn("pickup_datetime", F.col("pickup_datetime").cast("timestamp"))
        .withColumn("dropoff_datetime", F.col("dropoff_datetime").cast("timestamp"))
        .withColumn("passenger_count", F.col("passenger_count").cast("int"))
        .withColumn("trip_distance_mi", F.col("trip_distance_mi").cast("double"))
        .withColumn("rate_code_id", F.col("rate_code_id").cast("int"))
        .withColumn("pu_location_id", F.col("pu_location_id").cast("int"))
        .withColumn("do_location_id", F.col("do_location_id").cast("int"))
        .withColumn("payment_type_id", F.col("payment_type_id").cast("int"))
        .withColumn("store_and_fwd_flag", F.col("store_and_fwd_flag").cast("string"))
        .withColumn("load_month", F.col("load_month").cast("string"))
        .transform(
            lambda d: d.select(
                *[
                    F.coalesce(F.col(c).cast("decimal(10,2)"), F.lit(0).cast("decimal(10,2)")).alias(c)
                    if c in NUMERIC_COLUMNS
                    else F.col(c)
                    for c in d.columns
                ]
            )
        )
    )


def add_derived_columns(df: DataFrame) -> DataFrame:
    """Metricas calculadas uma unica vez, no lugar certo do pipeline."""
    duration_min = (
        F.col("dropoff_datetime").cast("long") - F.col("pickup_datetime").cast("long")
    ) / 60.0

    return (
        df.withColumn("trip_duration_min", F.round(duration_min, 2))
        .withColumn("trip_distance_km", F.round(F.col("trip_distance_mi") * MILES_TO_KM, 3))
        .withColumn(
            "avg_speed_kmh",
            F.when(
                F.col("trip_duration_min") > 0,
                F.round(F.col("trip_distance_km") / (F.col("trip_duration_min") / 60.0), 2),
            ),
        )
        .withColumn(
            "tip_pct",
            F.when(
                F.col("fare_amount") > 0,
                F.round(F.col("tip_amount") / F.col("fare_amount"), 4),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn("pickup_date", F.to_date("pickup_datetime"))
        .withColumn("pickup_hour", F.hour("pickup_datetime"))
        .withColumn("is_airport_trip", F.col("airport_fee_amount") > 0)
    )


def add_trip_id(df: DataFrame, key_columns: Sequence[str] = NATURAL_KEY) -> DataFrame:
    """Surrogate key deterministica: mesma corrida -> mesmo id em qualquer execucao."""
    payload = F.concat_ws(
        "||", *[F.coalesce(F.col(c).cast("string"), F.lit("~")) for c in key_columns]
    )
    return df.withColumn("trip_id", F.sha2(payload, 256))


def deduplicate(df: DataFrame) -> DataFrame:
    """Mantem a ocorrencia ingerida mais recente de cada trip_id."""
    from pyspark.sql.window import Window

    w = Window.partitionBy("trip_id").orderBy(F.col("_ingested_at").desc())
    return (
        df.withColumn("_rn", F.row_number().over(w)).where(F.col("_rn") == 1).drop("_rn")
    )


def _violations_column(rules: Sequence[Rule]) -> Column:
    """Array com o nome de cada regra violada pela linha (vazio = linha valida)."""
    return F.array_compact(
        F.array(
            *[
                F.when(~F.expr(r.expression) | F.expr(r.expression).isNull(), F.lit(r.name))
                for r in rules
            ]
        )
    )


def split_valid_invalid(
    df: DataFrame, rules: Sequence[Rule]
) -> tuple[DataFrame, DataFrame]:
    """Separa o lote em (validos, quarentena) aplicando as regras do YAML."""
    tagged = df.withColumn("_violations", _violations_column(rules))
    valid = tagged.where(F.size("_violations") == 0).drop("_violations")
    invalid = tagged.where(F.size("_violations") > 0).withColumn(
        "_quarantined_at", F.current_timestamp()
    )
    return valid, invalid


def build_trips(df_bronze: DataFrame, rules: Sequence[Rule]) -> tuple[DataFrame, DataFrame]:
    """Pipeline silver completo, encadeado. Testavel sem nenhuma tabela real."""
    prepared = (
        df_bronze.transform(normalize_columns)
        .transform(cast_types)
        .transform(add_derived_columns)
        .transform(add_trip_id)
        .transform(deduplicate)
    )
    return split_valid_invalid(prepared, rules)


def build_zones(df_bronze: DataFrame) -> DataFrame:
    """Conforma o lookup de zonas; trata os codigos especiais 264/265."""
    def blank_to_null(col: str) -> Column:
        """Trata '', '   ' e NULL como a mesma coisa: ausencia de valor."""
        return F.nullif(F.trim(F.col(col)), F.lit(""))

    return (
        df_bronze.select(
            F.col("LocationID").cast("int").alias("location_id"),
            F.initcap(blank_to_null("Borough")).alias("borough"),
            blank_to_null("Zone").alias("zone"),
            F.lower(blank_to_null("service_zone")).alias("service_zone"),
        )
        .withColumn(
            "borough",
            F.when(
                F.col("borough").isNull() | F.col("borough").isin("Unknown", "N/A"),
                F.lit("Desconhecido"),
            ).otherwise(F.col("borough")),
        )
        .withColumn("zone", F.coalesce(F.col("zone"), F.lit("Zona nao informada")))
        .withColumn("service_zone", F.coalesce(F.col("service_zone"), F.lit("n/a")))
        .dropDuplicates(["location_id"])
    )


# --------------------------------------------------------------------------- #
# Orquestracao da camada
# --------------------------------------------------------------------------- #
def run(spark: SparkSession, cfg: Config) -> dict[str, int]:
    bronze_trips = spark.table(cfg.table("bronze", TRIPS_TABLE)).where(
        F.col("load_month").isin(cfg.months)
    )
    valid, invalid = build_trips(bronze_trips, cfg.rules)

    months_filter = ", ".join(f"'{m}'" for m in cfg.months)
    io.overwrite_partitions(
        valid,
        cfg.table("silver", TRIPS_TABLE_SILVER),
        partition_by=["load_month"],
        replace_where=f"load_month IN ({months_filter})",
        comment="Corridas conformadas, tipadas, deduplicadas e validadas",
    )
    io.overwrite_partitions(
        invalid,
        cfg.table("silver", QUARANTINE_TABLE),
        partition_by=["load_month"],
        replace_where=f"load_month IN ({months_filter})",
        comment="Corridas rejeitadas pelas regras de negocio, com o motivo",
    )
    io.overwrite_partitions(
        build_zones(spark.table(cfg.table("bronze", ZONES_TABLE))),
        cfg.table("silver", ZONES_TABLE_SILVER),
        comment="Zonas de taxi conformadas",
    )

    n_valid = spark.table(cfg.table("silver", TRIPS_TABLE_SILVER)).count()
    n_invalid = spark.table(cfg.table("silver", QUARANTINE_TABLE)).count()
    rate = n_invalid / (n_valid + n_invalid) if (n_valid + n_invalid) else 0
    log.info("silver: %s validas | %s em quarentena (%.2f%%)", n_valid, n_invalid, rate * 100)
    return {"valid": n_valid, "quarantined": n_invalid}
