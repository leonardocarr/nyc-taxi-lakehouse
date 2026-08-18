"""Testes do motor de qualidade e das primitivas Delta (idempotencia, SCD2)."""

from __future__ import annotations

import pytest
from pyspark.sql import functions as F

from nyc_taxi import io, quality


# --------------------------------------------------------------------------- #
# Motor de expectativas
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    ("observed", "expect", "esperado"),
    [(0, "== 0", True), (5, "== 0", False), (3, "<= 5", True), (9, "> 10", False)],
)
def test_avaliacao_de_expectativa(observed, expect, esperado):
    assert quality._evaluate(observed, expect) is esperado


def test_expectativa_malformada_falha_alto():
    with pytest.raises(ValueError):
        quality._evaluate(0, "zero por favor")


# --------------------------------------------------------------------------- #
# Idempotencia da escrita
# --------------------------------------------------------------------------- #
def test_replace_where_reprocessa_so_o_mes_alvo(spark, cfg):
    table = cfg.table("bronze", "test_idempotencia")
    spark.sql(f"DROP TABLE IF EXISTS {table}")

    jan = spark.createDataFrame([(1, "2024-01"), (2, "2024-01")], "id int, load_month string")
    fev = spark.createDataFrame([(3, "2024-02")], "id int, load_month string")

    io.overwrite_partitions(jan, table, ["load_month"], "load_month = '2024-01'")
    io.overwrite_partitions(fev, table, ["load_month"], "load_month = '2024-02'")
    assert spark.table(table).count() == 3

    # reprocessa janeiro com menos linhas: fevereiro nao pode ser afetado
    jan_menor = spark.createDataFrame([(1, "2024-01")], "id int, load_month string")
    io.overwrite_partitions(jan_menor, table, ["load_month"], "load_month = '2024-01'")

    assert spark.table(table).count() == 2
    assert spark.table(table).where(F.col("load_month") == "2024-02").count() == 1


def test_rodar_duas_vezes_produz_o_mesmo_resultado(spark, cfg):
    table = cfg.table("bronze", "test_rerun")
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    df = spark.createDataFrame([(1, "2024-01"), (2, "2024-01")], "id int, load_month string")

    for _ in range(3):
        io.overwrite_partitions(df, table, ["load_month"], "load_month = '2024-01'")

    assert spark.table(table).count() == 2


# --------------------------------------------------------------------------- #
# SCD Tipo 2
# --------------------------------------------------------------------------- #
def _dim(spark, zone_name: str):
    return spark.createDataFrame(
        [(142, "Manhattan", zone_name, "yellow zone", False)],
        "location_id int, borough string, zone string, service_zone string, is_airport boolean",
    ).withColumn("location_key", F.xxhash64(F.col("location_id").cast("string")))


def test_scd2_versiona_quando_atributo_muda(spark, cfg):
    table = cfg.table("gold", "test_dim_scd2")
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    tracked = ["borough", "zone", "service_zone", "is_airport"]

    io.upsert_scd2(_dim(spark, "Lincoln Square East"), table, ["location_id"], tracked, "2024-01-01")
    assert spark.table(table).count() == 1

    # carga sem mudanca: nao pode criar versao nova
    io.upsert_scd2(_dim(spark, "Lincoln Square East"), table, ["location_id"], tracked, "2024-02-01")
    assert spark.table(table).count() == 1

    # zona renomeada: fecha a versao antiga e abre uma nova
    io.upsert_scd2(_dim(spark, "Lincoln Square"), table, ["location_id"], tracked, "2024-03-01")
    rows = spark.table(table).orderBy("effective_from").collect()

    assert len(rows) == 2
    assert rows[0]["is_current"] is False
    assert rows[0]["effective_to"].isoformat() == "2024-02-29"
    assert rows[1]["is_current"] is True
    assert rows[1]["zone"] == "Lincoln Square"
    assert spark.table(table).where("is_current").count() == 1
