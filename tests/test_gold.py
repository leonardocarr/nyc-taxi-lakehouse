"""Testes do modelo dimensional."""

from __future__ import annotations

from pyspark.sql import functions as F

from nyc_taxi import gold, silver
from nyc_taxi.gold import UNKNOWN_KEY


def test_dim_date_nao_tem_buracos(spark):
    dim = gold.build_dim_date(spark, "2024-01-01", "2024-03-31")
    assert dim.count() == 91
    assert dim.select("date_key").distinct().count() == 91
    assert dim.where(F.col("full_date") == "2024-02-29").count() == 1  # ano bissexto


def test_dim_date_marca_fim_de_semana(spark):
    dim = gold.build_dim_date(spark, "2024-01-01", "2024-01-07")
    weekend = {r["full_date"].isoformat() for r in dim.where("is_weekend").collect()}
    assert weekend == {"2024-01-06", "2024-01-07"}


def test_dim_time_tem_um_registro_por_minuto(spark):
    dim = gold.build_dim_time(spark)
    assert dim.count() == 1440
    assert dim.select("time_key").distinct().count() == 1440
    assert dim.where(F.col("time_key") == 830).first()["day_period"] == "Manha"
    assert dim.where(F.col("time_key") == 830).first()["is_rush_hour"] is True


def test_dimensoes_estaticas_tem_membro_desconhecido(spark):
    for builder, key in (
        (gold.build_dim_vendor, "vendor_key"),
        (gold.build_dim_rate_code, "rate_code_key"),
        (gold.build_dim_payment_type, "payment_type_key"),
    ):
        dim = builder(spark)
        assert dim.where(F.col(key) == UNKNOWN_KEY).count() == 1


def test_dim_location_marca_aeroportos(spark, raw_zones):
    dim = gold.build_dim_location_source(silver.build_zones(raw_zones))
    jfk = dim.where(F.col("location_id") == 132).first()
    assert jfk["is_airport"] is True
    assert dim.where(F.col("location_id") == UNKNOWN_KEY).count() == 1


def test_fato_nunca_tem_chave_orfa(spark, raw_trips, raw_zones, cfg):
    """Codigos invalidos (vendor 99, rate 88, payment 77, zona inexistente)
    devem cair no membro 'Nao informado', nunca virar NULL nem sumir."""
    valid, _ = silver.build_trips(raw_trips, cfg.rules)
    dim_location = gold.build_dim_location_source(silver.build_zones(raw_zones)).withColumn(
        "effective_from", F.lit("2024-01-01").cast("date")
    ).withColumn("effective_to", F.lit("9999-12-31").cast("date"))

    fact = gold.build_fct_trips(valid, dim_location)
    row = fact.where(F.col("trip_duration_min") == 25.0).orderBy("pickup_date_key").collect()[-1]

    assert row["vendor_key"] == UNKNOWN_KEY
    assert row["rate_code_key"] == UNKNOWN_KEY
    assert row["payment_type_key"] == UNKNOWN_KEY
    assert fact.where(F.col("pickup_location_key").isNull()).count() == 0
    assert fact.count() == valid.count()


def test_fato_preserva_o_grao_de_uma_linha_por_corrida(spark, raw_trips, raw_zones, cfg):
    valid, _ = silver.build_trips(raw_trips, cfg.rules)
    dim_location = gold.build_dim_location_source(silver.build_zones(raw_zones)).withColumn(
        "effective_from", F.lit("2024-01-01").cast("date")
    ).withColumn("effective_to", F.lit("9999-12-31").cast("date"))

    fact = gold.build_fct_trips(valid, dim_location)
    assert fact.count() == fact.select("trip_id").distinct().count()


def test_chaves_de_data_e_hora_batem_com_o_timestamp(spark, raw_trips, raw_zones, cfg):
    valid, _ = silver.build_trips(raw_trips, cfg.rules)
    dim_location = gold.build_dim_location_source(silver.build_zones(raw_zones)).withColumn(
        "effective_from", F.lit("2024-01-01").cast("date")
    ).withColumn("effective_to", F.lit("9999-12-31").cast("date"))

    fact = gold.build_fct_trips(valid, dim_location)
    row = fact.where(F.col("pickup_time_key") == 830).first()
    assert row["pickup_date_key"] == 20240105
