"""Primitivas de escrita em Delta Lake.

Tres padroes de escrita cobrem o pipeline inteiro:

* ``overwrite_partitions`` -- carga idempotente por particao (``replaceWhere``).
  Reprocessar 2024-01 nao toca em 2024-02.
* ``upsert_scd1``          -- dimensao sobrescrita no lugar (ultima versao vence).
* ``upsert_scd2``          -- dimensao com historico: fecha a linha antiga e
  insere a nova quando um atributo rastreado muda.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

log = logging.getLogger(__name__)

HIGH_DATE = "9999-12-31"


# --------------------------------------------------------------------------- #
# Escrita basica
# --------------------------------------------------------------------------- #
def table_exists(spark: SparkSession, table: str) -> bool:
    try:
        return spark.catalog.tableExists(table)
    except Exception:  # noqa: BLE001 - catalogos antigos levantam AnalysisException
        return False


def overwrite_partitions(
    df: DataFrame,
    table: str,
    partition_by: Sequence[str] | None = None,
    replace_where: str | None = None,
    comment: str | None = None,
) -> None:
    """Grava em Delta substituindo apenas as particoes atingidas.

    Sem ``replace_where`` faz overwrite total -- usado nas dimensoes pequenas.
    """
    spark = df.sparkSession
    writer = df.write.format("delta").mode("overwrite")

    if partition_by:
        writer = writer.partitionBy(*partition_by)
    if replace_where and table_exists(spark, table):
        writer = writer.option("replaceWhere", replace_where)
    else:
        # primeira carga: nao ha o que substituir, e o schema pode evoluir
        writer = writer.option("overwriteSchema", "true")

    log.info("Gravando %s (replaceWhere=%s)", table, replace_where)
    writer.saveAsTable(table)

    if comment:
        try:
            spark.sql(f"COMMENT ON TABLE {table} IS '{comment}'")
        except Exception as exc:  # noqa: BLE001 - documentacao nunca deve quebrar carga
            log.debug("COMMENT ignorado em %s: %s", table, exc)


def append(df: DataFrame, table: str, partition_by: Sequence[str] | None = None) -> None:
    writer = df.write.format("delta").mode("append")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.saveAsTable(table)


# --------------------------------------------------------------------------- #
# Dimensoes
# --------------------------------------------------------------------------- #
def upsert_scd1(df: DataFrame, table: str, keys: Sequence[str]) -> None:
    """Merge simples: atualiza no lugar, insere o que e novo, nao guarda historico."""
    spark = df.sparkSession
    if not table_exists(spark, table):
        df.write.format("delta").option("overwriteSchema", "true").mode(
            "overwrite"
        ).saveAsTable(table)
        return

    from delta.tables import DeltaTable

    target = DeltaTable.forName(spark, table)
    cond = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    (
        target.alias("t")
        .merge(df.alias("s"), cond)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def upsert_scd2(
    df: DataFrame,
    table: str,
    business_keys: Sequence[str],
    tracked_columns: Iterable[str],
    effective_date: str,
) -> None:
    """SCD Tipo 2 pelo padrao de duas passadas do Delta.

    A operacao MERGE do Delta so aceita uma acao por linha de origem, e o SCD2
    precisa de duas (fechar a versao antiga + inserir a nova). O truque padrao e
    unir a origem consigo mesma: linhas com ``merge_key`` nulo nunca casam com o
    alvo e portanto caem sempre no ramo de INSERT.
    """
    from delta.tables import DeltaTable

    spark = df.sparkSession
    tracked = list(tracked_columns)

    staged = (
        df.withColumn("effective_from", F.lit(effective_date).cast("date"))
        .withColumn("effective_to", F.lit(HIGH_DATE).cast("date"))
        .withColumn("is_current", F.lit(True))
        .withColumn(
            "row_hash",
            F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in tracked]), 256),
        )
    )

    if not table_exists(spark, table):
        staged.write.format("delta").option("overwriteSchema", "true").mode(
            "overwrite"
        ).saveAsTable(table)
        return

    target = DeltaTable.forName(spark, table)
    target_columns = target.toDF().columns
    bk0 = business_keys[0]
    bk0_type = dict(staged.dtypes)[bk0]

    # Linhas que mudaram: precisam fechar a versao vigente E inserir a nova.
    current = target.toDF().filter("is_current")
    changed = (
        staged.alias("s")
        .join(
            current.alias("t"),
            on=[F.col(f"s.{k}") == F.col(f"t.{k}") for k in business_keys],
            how="inner",
        )
        .where(F.col("s.row_hash") != F.col("t.row_hash"))
        .select("s.*")
    )

    # merge_key nulo => nunca casa com o alvo => cai sempre no ramo de INSERT
    source = staged.withColumn("__merge_key", F.col(bk0)).unionByName(
        changed.withColumn("__merge_key", F.lit(None).cast(bk0_type))
    )

    merge_condition = " AND ".join(
        [f"t.{bk0} = s.__merge_key", "t.is_current = true"]
        + [f"t.{k} = s.{k}" for k in business_keys[1:]]
    )

    (
        target.alias("t")
        .merge(source.alias("s"), merge_condition)
        .whenMatchedUpdate(
            condition="t.row_hash <> s.row_hash",
            set={
                "is_current": F.lit(False),
                "effective_to": F.date_sub(F.col("s.effective_from"), 1),
            },
        )
        # insert explicito: a origem carrega __merge_key, que nao existe no alvo
        .whenNotMatchedInsert(values={c: F.col(f"s.{c}") for c in target_columns})
        .execute()
    )


# --------------------------------------------------------------------------- #
# Manutencao
# --------------------------------------------------------------------------- #
def optimize(spark: SparkSession, table: str, zorder_by: Sequence[str] | None = None) -> None:
    """OPTIMIZE + Z-ORDER. No-op silencioso fora do Databricks (OSS nao suporta)."""
    try:
        stmt = f"OPTIMIZE {table}"
        if zorder_by:
            stmt += f" ZORDER BY ({', '.join(zorder_by)})"
        spark.sql(stmt)
        log.info("OPTIMIZE aplicado em %s", table)
    except Exception as exc:  # noqa: BLE001
        log.warning("OPTIMIZE ignorado em %s: %s", table, exc)
