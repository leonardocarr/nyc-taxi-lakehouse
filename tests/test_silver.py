"""Testes das transformacoes da camada silver.

Cada teste ataca uma regra especifica -- se um deles quebra, da para saber
exatamente qual comportamento regrediu.
"""

from __future__ import annotations

import pytest
from pyspark.sql import functions as F

from nyc_taxi import silver


def test_normalize_columns_renomeia_para_snake_case(raw_trips):
    out = silver.normalize_columns(raw_trips)
    assert "pickup_datetime" in out.columns
    assert "pu_location_id" in out.columns
    assert "VendorID" not in out.columns


def test_normalize_columns_cria_colunas_ausentes_de_anos_recentes(raw_trips):
    """cbd_congestion_fee so existe a partir de 2025; o schema precisa ser estavel."""
    out = silver.normalize_columns(raw_trips)
    assert "cbd_congestion_fee_amount" in out.columns
    assert out.where(F.col("cbd_congestion_fee_amount").isNotNull()).count() == 0


def test_derived_columns_calculam_duracao_e_distancia(raw_trips):
    out = (
        raw_trips.transform(silver.normalize_columns)
        .transform(silver.cast_types)
        .transform(silver.add_derived_columns)
    )
    row = out.where(F.col("pickup_datetime") == "2024-01-05 08:30:00").first()
    assert row["trip_duration_min"] == pytest.approx(25.0)
    assert row["trip_distance_km"] == pytest.approx(5.150, abs=1e-3)
    assert row["avg_speed_kmh"] == pytest.approx(12.36, abs=0.05)
    assert row["tip_pct"] == pytest.approx(0.2222, abs=1e-3)


def test_trip_id_e_deterministico(raw_trips):
    prepared = (
        raw_trips.transform(silver.normalize_columns)
        .transform(silver.cast_types)
        .transform(silver.add_trip_id)
    )
    ids = [r["trip_id"] for r in prepared.collect()]
    # linhas 1 e 3 sao a mesma corrida -> mesmo id
    assert ids[0] == ids[2]
    assert len(set(ids)) == len(ids) - 1


def test_deduplicate_remove_a_duplicata(raw_trips):
    prepared = (
        raw_trips.transform(silver.normalize_columns)
        .transform(silver.cast_types)
        .transform(silver.add_derived_columns)
        .transform(silver.add_trip_id)
    )
    assert prepared.count() == 7
    assert silver.deduplicate(prepared).count() == 6


def test_quarentena_captura_cada_violacao(raw_trips, cfg):
    valid, invalid = silver.build_trips(raw_trips, cfg.rules)

    violations = {
        r["trip_id"]: set(r["_violations"]) for r in invalid.collect()
    }
    all_violations = set().union(*violations.values())

    assert "chronological_order" in all_violations
    assert "distance_positive" in all_violations
    assert "pickup_within_declared_month" in all_violations
    assert invalid.count() == 3
    assert valid.count() == 3


def test_linhas_validas_nao_carregam_coluna_de_violacao(raw_trips, cfg):
    valid, _ = silver.build_trips(raw_trips, cfg.rules)
    assert "_violations" not in valid.columns


def test_build_zones_trata_valores_sujos(raw_zones):
    zones = silver.build_zones(raw_zones)
    by_id = {r["location_id"]: r for r in zones.collect()}

    assert by_id[264]["borough"] == "Desconhecido"      # era NULL
    assert by_id[264]["zone"] == "Zona nao informada"   # era '   '
    assert by_id[264]["service_zone"] == "n/a"
    assert by_id[265]["borough"] == "Desconhecido"      # era 'Unknown'
    assert by_id[132]["service_zone"] == "airports"
    assert zones.count() == 7
