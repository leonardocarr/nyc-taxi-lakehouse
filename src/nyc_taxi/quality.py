"""Motor de expectativas de dados.

As expectativas sao declaradas em ``conf/pipeline.yml``, nao em codigo, para que
um analista consiga adicionar uma verificacao sem abrir um PR de Python.

Cada execucao grava o resultado em ``gold.dq_results``: e essa tabela que
permite responder "quando essa metrica quebrou?" tres meses depois.
"""

from __future__ import annotations

import logging
import operator
import re
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from . import io
from .config import Config, Expectation

log = logging.getLogger(__name__)

RESULTS_TABLE = "dq_results"

OPERATORS = {
    "==": operator.eq,
    "!=": operator.ne,
    "<=": operator.le,
    ">=": operator.ge,
    "<": operator.lt,
    ">": operator.gt,
}

RESULTS_SCHEMA = StructType(
    [
        StructField("run_at", TimestampType()),
        StructField("run_id", StringType()),
        StructField("target", StringType()),
        StructField("expectation", StringType()),
        StructField("severity", StringType()),
        StructField("observed", LongType()),
        StructField("expected", StringType()),
        StructField("passed", BooleanType()),
    ]
)


class ExpectationFailed(RuntimeError):
    """Levantada quando uma expectativa de severidade 'fail' nao e atendida."""


def _evaluate(observed: int, expect: str) -> bool:
    match = re.match(r"^\s*(==|!=|<=|>=|<|>)\s*(-?\d+)\s*$", expect)
    if not match:
        raise ValueError(f"Expectativa invalida: {expect!r} (use por exemplo '== 0')")
    op, value = match.groups()
    return OPERATORS[op](observed, int(value))


def run_expectations(
    spark: SparkSession, cfg: Config, target_key: str, run_id: str
) -> list[dict]:
    """Roda todas as expectativas de um alvo (ex.: 'gold.fct_trips')."""
    layer, table_name = target_key.split(".", 1)
    table = cfg.table(layer, table_name)
    expectations: list[Expectation] = cfg.expectations_for(target_key)
    results: list[dict] = []

    for exp in expectations:
        query = exp.query.format(table=table, catalog_prefix=cfg.catalog_prefix)
        observed = int(spark.sql(query).first()[0])
        passed = _evaluate(observed, exp.expect)

        results.append(
            {
                "run_at": datetime.now(timezone.utc),
                "run_id": run_id,
                "target": target_key,
                "expectation": exp.name,
                "severity": exp.severity,
                "observed": observed,
                "expected": exp.expect,
                "passed": passed,
            }
        )

        level = log.info if passed else (log.error if exp.severity == "fail" else log.warning)
        level(
            "[DQ] %-40s %s  observado=%s esperado=%s",
            f"{target_key}.{exp.name}",
            "OK  " if passed else "FALHA",
            observed,
            exp.expect,
        )

    if results:
        io.append(
            spark.createDataFrame(results, schema=RESULTS_SCHEMA),
            cfg.table("gold", RESULTS_TABLE),
        )

    failures = [r for r in results if not r["passed"] and r["severity"] == "fail"]
    if failures:
        names = ", ".join(f["expectation"] for f in failures)
        raise ExpectationFailed(f"Expectativas bloqueantes falharam em {target_key}: {names}")

    return results
