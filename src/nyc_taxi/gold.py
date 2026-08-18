"""Camada Gold -- modelo dimensional (star schema) para consumo analitico.

Grao da tabela fato: **uma corrida de taxi**.

Convencoes seguidas:

* Toda dimensao tem um membro "Nao informado" com chave ``-1``. Com isso a fato
  nunca tem FK nula nem orfa, e o analista nao perde linhas em INNER JOIN.
* ``dim_date`` e ``dim_time`` sao geradas, nao ingeridas -- dimensoes de calendario
  nunca devem depender do que apareceu nos dados.
* ``dim_location`` e SCD Tipo 2: se a TLC renomear uma zona, o historico das
  corridas antigas continua apontando para o nome que valia na epoca.
* Dimensoes de codigo estatico (vendor, rate code, payment type) usam o proprio
  codigo como chave -- criar surrogate para um dominio de 7 linhas que nunca muda
  so adiciona um join sem beneficio.
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from . import io
from .config import Config
from .silver import TRIPS_TABLE_SILVER, ZONES_TABLE_SILVER

log = logging.getLogger(__name__)

UNKNOWN_KEY = -1

VENDORS = [
    (1, "Creative Mobile Technologies"),
    (2, "Curb Mobility"),
    (6, "Myle Technologies"),
    (7, "Helix"),
]

RATE_CODES = [
    (1, "Tarifa padrao", True),
    (2, "JFK", False),
    (3, "Newark", False),
    (4, "Nassau ou Westchester", False),
    (5, "Tarifa negociada", False),
    (6, "Corrida em grupo", False),
    (99, "Nao identificada", False),
]

PAYMENT_TYPES = [
    (0, "Flex Fare", False),
    (1, "Cartao de credito", True),
    (2, "Dinheiro", False),
    (3, "Sem cobranca", False),
    (4, "Contestada", False),
    (5, "Desconhecida", False),
    (6, "Anulada", False),
]


# --------------------------------------------------------------------------- #
# Dimensoes geradas
# --------------------------------------------------------------------------- #
def build_dim_date(spark: SparkSession, start: str, end: str) -> DataFrame:
    """Calendario continuo entre duas datas -- sem buracos, independente dos fatos."""
    df = spark.sql(
        f"SELECT explode(sequence(to_date('{start}'), to_date('{end}'), interval 1 day)) AS full_date"
    )
    return (
        df.withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("full_date"))
        .withColumn("quarter", F.quarter("full_date"))
        .withColumn("month", F.month("full_date"))
        .withColumn("month_name", F.date_format("full_date", "MMMM"))
        .withColumn("year_month", F.date_format("full_date", "yyyy-MM"))
        .withColumn("day", F.dayofmonth("full_date"))
        .withColumn("day_of_year", F.dayofyear("full_date"))
        .withColumn("week_of_year", F.weekofyear("full_date"))
        .withColumn("day_of_week", F.dayofweek("full_date"))
        .withColumn("day_name", F.date_format("full_date", "EEEE"))
        .withColumn("is_weekend", F.dayofweek("full_date").isin(1, 7))
        .withColumn(
            "day_type", F.when(F.col("is_weekend"), "Fim de semana").otherwise("Dia util")
        )
        .withColumn("is_month_start", F.dayofmonth("full_date") == 1)
        .withColumn("is_month_end", F.col("full_date") == F.last_day("full_date"))
        .select(
            "date_key", "full_date", "year", "quarter", "month", "month_name",
            "year_month", "day", "day_of_year", "week_of_year", "day_of_week",
            "day_name", "is_weekend", "day_type", "is_month_start", "is_month_end",
        )
    )


def build_dim_time(spark: SparkSession) -> DataFrame:
    """1440 linhas: um registro por minuto do dia."""
    df = spark.sql("SELECT explode(sequence(0, 1439)) AS minute_of_day")
    hour = F.floor(F.col("minute_of_day") / 60).cast("int")
    minute = (F.col("minute_of_day") % 60).cast("int")
    return (
        df.withColumn("hour", hour)
        .withColumn("minute", minute)
        .withColumn("time_key", (F.col("hour") * 100 + F.col("minute")).cast("int"))
        .withColumn(
            "time_label",
            F.concat(F.lpad(F.col("hour"), 2, "0"), F.lit(":"), F.lpad(F.col("minute"), 2, "0")),
        )
        .withColumn(
            "day_period",
            F.when(F.col("hour") < 6, "Madrugada")
            .when(F.col("hour") < 12, "Manha")
            .when(F.col("hour") < 18, "Tarde")
            .otherwise("Noite"),
        )
        .withColumn(
            "is_rush_hour",
            F.col("hour").between(7, 9) | F.col("hour").between(16, 19),
        )
        .select("time_key", "hour", "minute", "time_label", "day_period", "is_rush_hour")
    )


# --------------------------------------------------------------------------- #
# Dimensoes de codigo estatico
# --------------------------------------------------------------------------- #
def build_dim_vendor(spark: SparkSession) -> DataFrame:
    rows = [(v, n) for v, n in VENDORS] + [(UNKNOWN_KEY, "Nao informado")]
    return spark.createDataFrame(rows, "vendor_key int, vendor_name string")


def build_dim_rate_code(spark: SparkSession) -> DataFrame:
    rows = list(RATE_CODES) + [(UNKNOWN_KEY, "Nao informado", False)]
    return spark.createDataFrame(
        rows, "rate_code_key int, rate_code_name string, is_standard boolean"
    )


def build_dim_payment_type(spark: SparkSession) -> DataFrame:
    rows = list(PAYMENT_TYPES) + [(UNKNOWN_KEY, "Nao informado", False)]
    return spark.createDataFrame(
        rows, "payment_type_key int, payment_type_name string, is_electronic boolean"
    )


# --------------------------------------------------------------------------- #
# Dimensao SCD Tipo 2
# --------------------------------------------------------------------------- #
def build_dim_location_source(df_zones: DataFrame) -> DataFrame:
    """Prepara a dimensao de localizacao antes do merge SCD2."""
    unknown = df_zones.sparkSession.createDataFrame(
        [(UNKNOWN_KEY, "Nao informado", "Nao informado", "n/a")],
        "location_id int, borough string, zone string, service_zone string",
    )
    return (
        df_zones.select("location_id", "borough", "zone", "service_zone")
        .unionByName(unknown)
        .withColumn(
            "location_key",
            F.xxhash64(F.concat_ws("|", F.col("location_id").cast("string"))),
        )
        .withColumn(
            "is_airport",
            F.col("zone").rlike("(?i)(JFK|LaGuardia|Newark|Airport)"),
        )
    )


# --------------------------------------------------------------------------- #
# Tabela fato
# --------------------------------------------------------------------------- #
def build_fct_trips(df_trips: DataFrame, dim_location: DataFrame) -> DataFrame:
    """Substitui chaves naturais por chaves de dimensao e seleciona as metricas.

    O join com ``dim_location`` usa a versao vigente na data da corrida
    (``effective_from``/``effective_to``), nao a versao atual -- e isso que faz o
    SCD Tipo 2 valer alguma coisa.
    """
    loc = dim_location.select(
        "location_id", "location_key", "effective_from", "effective_to"
    )

    pu = loc.alias("pu")
    do = loc.alias("do")

    fact = (
        df_trips.alias("t")
        .join(
            pu,
            (F.col("t.pu_location_id") == F.col("pu.location_id"))
            & F.col("t.pickup_date").between(F.col("pu.effective_from"), F.col("pu.effective_to")),
            "left",
        )
        .join(
            do,
            (F.col("t.do_location_id") == F.col("do.location_id"))
            & F.col("t.pickup_date").between(F.col("do.effective_from"), F.col("do.effective_to")),
            "left",
        )
    )

    unknown_location_key = (
        dim_location.where(F.col("location_id") == UNKNOWN_KEY)
        .select("location_key")
        .first()
    )
    unknown_key_value = unknown_location_key[0] if unknown_location_key else None

    valid_vendors = [v for v, _ in VENDORS]
    valid_rate_codes = [r for r, _, _ in RATE_CODES]
    valid_payments = [p for p, _, _ in PAYMENT_TYPES]

    return fact.select(
        # --- chave degenerada ------------------------------------------------
        F.col("t.trip_id").alias("trip_id"),
        # --- chaves estrangeiras --------------------------------------------
        F.date_format("t.pickup_datetime", "yyyyMMdd").cast("int").alias("pickup_date_key"),
        (F.hour("t.pickup_datetime") * 100 + F.minute("t.pickup_datetime")).cast("int").alias("pickup_time_key"),
        F.date_format("t.dropoff_datetime", "yyyyMMdd").cast("int").alias("dropoff_date_key"),
        (F.hour("t.dropoff_datetime") * 100 + F.minute("t.dropoff_datetime")).cast("int").alias("dropoff_time_key"),
        F.coalesce(F.col("pu.location_key"), F.lit(unknown_key_value)).alias("pickup_location_key"),
        F.coalesce(F.col("do.location_key"), F.lit(unknown_key_value)).alias("dropoff_location_key"),
        F.when(F.col("t.vendor_id").isin(valid_vendors), F.col("t.vendor_id"))
        .otherwise(F.lit(UNKNOWN_KEY)).alias("vendor_key"),
        F.when(F.col("t.rate_code_id").isin(valid_rate_codes), F.col("t.rate_code_id"))
        .otherwise(F.lit(UNKNOWN_KEY)).alias("rate_code_key"),
        F.when(F.col("t.payment_type_id").isin(valid_payments), F.col("t.payment_type_id"))
        .otherwise(F.lit(UNKNOWN_KEY)).alias("payment_type_key"),
        # --- atributos degenerados ------------------------------------------
        F.col("t.store_and_fwd_flag").alias("store_and_fwd_flag"),
        F.col("t.is_airport_trip").alias("is_airport_trip"),
        # --- metricas aditivas ----------------------------------------------
        F.col("t.passenger_count").alias("passenger_count"),
        F.col("t.trip_distance_km").alias("trip_distance_km"),
        F.col("t.trip_duration_min").alias("trip_duration_min"),
        F.col("t.fare_amount").alias("fare_amount"),
        F.col("t.extra_amount").alias("extra_amount"),
        F.col("t.mta_tax_amount").alias("mta_tax_amount"),
        F.col("t.tip_amount").alias("tip_amount"),
        F.col("t.tolls_amount").alias("tolls_amount"),
        F.col("t.improvement_surcharge_amount").alias("improvement_surcharge_amount"),
        F.col("t.congestion_surcharge_amount").alias("congestion_surcharge_amount"),
        F.col("t.airport_fee_amount").alias("airport_fee_amount"),
        F.col("t.total_amount").alias("total_amount"),
        # --- metricas nao aditivas (nunca some estas colunas) ----------------
        F.col("t.avg_speed_kmh").alias("avg_speed_kmh"),
        F.col("t.tip_pct").alias("tip_pct"),
        # --- particao --------------------------------------------------------
        F.col("t.load_month").alias("load_month"),
    )


# --------------------------------------------------------------------------- #
# Orquestracao da camada
# --------------------------------------------------------------------------- #
def run(spark: SparkSession, cfg: Config) -> dict[str, int]:
    trips = spark.table(cfg.table("silver", TRIPS_TABLE_SILVER)).where(
        F.col("load_month").isin(cfg.months)
    )
    zones = spark.table(cfg.table("silver", ZONES_TABLE_SILVER))

    bounds = trips.select(
        F.min("pickup_date").alias("min_d"), F.max("pickup_date").alias("max_d")
    ).first()
    start = f"{min(cfg.months)}-01"
    end = str(bounds["max_d"]) if bounds and bounds["max_d"] else f"{max(cfg.months)}-28"

    # ---- dimensoes -------------------------------------------------------- #
    io.overwrite_partitions(
        build_dim_date(spark, start, end), cfg.table("gold", "dim_date"),
        comment="Calendario gerado. Grao: dia.",
    )
    io.overwrite_partitions(
        build_dim_time(spark), cfg.table("gold", "dim_time"),
        comment="Dimensao de horario. Grao: minuto do dia.",
    )
    io.overwrite_partitions(
        build_dim_vendor(spark), cfg.table("gold", "dim_vendor"),
        comment="Operadoras de taxi credenciadas pela TLC",
    )
    io.overwrite_partitions(
        build_dim_rate_code(spark), cfg.table("gold", "dim_rate_code"),
        comment="Codigos tarifarios da corrida",
    )
    io.overwrite_partitions(
        build_dim_payment_type(spark), cfg.table("gold", "dim_payment_type"),
        comment="Formas de pagamento",
    )

    io.upsert_scd2(
        build_dim_location_source(zones),
        cfg.table("gold", "dim_location"),
        business_keys=["location_id"],
        tracked_columns=["borough", "zone", "service_zone", "is_airport"],
        effective_date=f"{min(cfg.months)}-01",
    )

    # ---- fato ------------------------------------------------------------- #
    dim_location = spark.table(cfg.table("gold", "dim_location"))
    fact = build_fct_trips(trips, dim_location)

    months_filter = ", ".join(f"'{m}'" for m in cfg.months)
    io.overwrite_partitions(
        fact,
        cfg.table("gold", "fct_trips"),
        partition_by=["load_month"],
        replace_where=f"load_month IN ({months_filter})",
        comment="Fato de corridas. Grao: uma corrida de taxi.",
    )

    n = spark.table(cfg.table("gold", "fct_trips")).count()
    log.info("gold.fct_trips: %s linhas", n)
    return {"fct_trips": n}
