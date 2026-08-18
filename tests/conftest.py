"""Fixtures de teste: um Spark local com Delta, reaproveitado na sessao inteira."""

from __future__ import annotations

import shutil
import sys
import tempfile
from datetime import datetime
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from nyc_taxi.config import load_config  # noqa: E402
from nyc_taxi.session import ensure_schemas, get_spark  # noqa: E402


@pytest.fixture(scope="session")
def warehouse() -> Path:
    path = Path(tempfile.mkdtemp(prefix="nyc-taxi-test-"))
    yield path
    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture(scope="session")
def cfg(warehouse: Path):
    config = load_config(profile="local")
    config.warehouse_dir = str(warehouse)
    return config


@pytest.fixture(scope="session")
def spark(cfg):
    session = get_spark(cfg, app_name="nyc-taxi-tests")
    ensure_schemas(session, cfg)
    yield session
    session.stop()


@pytest.fixture()
def raw_trips(spark):
    """Lote sintetico cobrindo os casos que importam:

    1. corrida normal
    2. corrida normal (segunda)
    3. duplicata exata da linha 1  -> deve ser deduplicada
    4. dropoff antes do pickup     -> quarentena
    5. distancia zero              -> quarentena
    6. pickup fora do mes de carga -> quarentena
    7. vendor inexistente (99)     -> deve virar chave -1 na gold
    """
    rows = [
        # VendorID, pickup, dropoff, passenger, dist, rate, sf, PU, DO, pay, fare, extra, mta, tip, tolls, imp, total, cong, airport
        (1, datetime(2024, 1, 5, 8, 30), datetime(2024, 1, 5, 8, 55), 1, 3.2, 1, "N", 142, 236, 1, 18.0, 1.0, 0.5, 4.0, 0.0, 1.0, 27.0, 2.5, 0.0),
        (2, datetime(2024, 1, 5, 18, 5), datetime(2024, 1, 5, 18, 40), 2, 7.8, 2, "N", 132, 100, 2, 70.0, 0.0, 0.5, 0.0, 6.9, 1.0, 78.4, 0.0, 1.75),
        (1, datetime(2024, 1, 5, 8, 30), datetime(2024, 1, 5, 8, 55), 1, 3.2, 1, "N", 142, 236, 1, 18.0, 1.0, 0.5, 4.0, 0.0, 1.0, 27.0, 2.5, 0.0),
        (1, datetime(2024, 1, 6, 10, 0), datetime(2024, 1, 6, 9, 0), 1, 2.0, 1, "N", 100, 101, 1, 10.0, 0.0, 0.5, 0.0, 0.0, 1.0, 11.5, 0.0, 0.0),
        (2, datetime(2024, 1, 7, 12, 0), datetime(2024, 1, 7, 12, 20), 1, 0.0, 1, "N", 100, 101, 1, 10.0, 0.0, 0.5, 0.0, 0.0, 1.0, 11.5, 0.0, 0.0),
        (2, datetime(2023, 12, 31, 23, 0), datetime(2023, 12, 31, 23, 30), 1, 5.0, 1, "N", 100, 101, 1, 20.0, 0.0, 0.5, 0.0, 0.0, 1.0, 21.5, 0.0, 0.0),
        (99, datetime(2024, 1, 8, 14, 0), datetime(2024, 1, 8, 14, 25), 3, 4.4, 88, "N", 264, 265, 77, 22.0, 0.0, 0.5, 2.0, 0.0, 1.0, 25.5, 0.0, 0.0),
    ]
    schema = (
        "VendorID int, tpep_pickup_datetime timestamp, tpep_dropoff_datetime timestamp, "
        "passenger_count int, trip_distance double, RatecodeID int, store_and_fwd_flag string, "
        "PULocationID int, DOLocationID int, payment_type int, fare_amount double, extra double, "
        "mta_tax double, tip_amount double, tolls_amount double, improvement_surcharge double, "
        "total_amount double, congestion_surcharge double, airport_fee double"
    )
    from pyspark.sql import functions as F

    return (
        spark.createDataFrame(rows, schema)
        .withColumn("load_month", F.lit("2024-01"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.lit("teste.parquet"))
        .withColumn("_batch_id", F.lit("batch-teste"))
    )


@pytest.fixture()
def raw_zones(spark):
    rows = [
        (142, "Manhattan", "Lincoln Square East", "Yellow Zone"),
        (236, "Manhattan", "Upper East Side North", "Yellow Zone"),
        (132, "Queens", "JFK Airport", "Airports"),
        (100, "Manhattan", "Garment District", "Yellow Zone"),
        (101, "Queens", "Jamaica", "Boro Zone"),
        (264, None, "  ", None),
        (265, "Unknown", "Outside of NYC", "N/A"),
    ]
    return spark.createDataFrame(rows, "LocationID int, Borough string, Zone string, service_zone string")
