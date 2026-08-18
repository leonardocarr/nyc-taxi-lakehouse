"""Ponto de entrada unico do pipeline.

    python -m nyc_taxi.run --layer all
    python -m nyc_taxi.run --layer silver --months 2024-01 2024-02
    python -m nyc_taxi.run --layer gold --profile databricks

O mesmo modulo e chamado pelos notebooks do Databricks e pelo job do Asset
Bundle -- nao existe uma versao "de producao" diferente da versao local.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
import uuid

from . import bronze, gold, quality, silver
from .config import load_config
from .session import ensure_schemas, get_spark

LAYERS = ("bronze", "silver", "gold")


def _setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s | %(levelname)-7s | %(name)-22s | %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stdout,
        force=True,
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pipeline medalhao NYC Taxi")
    parser.add_argument("--layer", choices=(*LAYERS, "all"), default="all")
    parser.add_argument("--profile", default=None, help="local | databricks")
    parser.add_argument("--months", nargs="*", default=None, help="ex.: 2024-01 2024-02")
    parser.add_argument("--config", default=None)
    parser.add_argument("--skip-quality", action="store_true")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    # O job do Databricks passa os meses como uma string unica
    # ("2024-01 2024-02"); a CLI passa como argumentos separados. Normaliza os dois.
    if args.months:
        args.months = [m for chunk in args.months for m in chunk.split()]
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    _setup_logging(args.log_level)
    log = logging.getLogger("nyc_taxi.run")

    cfg = load_config(args.config, profile=args.profile, months=args.months)
    run_id = str(uuid.uuid4())[:8]
    log.info("run_id=%s perfil=%s meses=%s", run_id, cfg.profile, cfg.months)

    spark = get_spark(cfg)
    ensure_schemas(spark, cfg)

    layers = LAYERS if args.layer == "all" else (args.layer,)
    started = time.time()

    for layer in layers:
        t0 = time.time()
        log.info("--- %s ---", layer.upper())

        if layer == "bronze":
            bronze.run(spark, cfg)
        elif layer == "silver":
            silver.run(spark, cfg)
            if not args.skip_quality:
                quality.run_expectations(spark, cfg, "silver.fct_source_trips", run_id)
        elif layer == "gold":
            gold.run(spark, cfg)
            if not args.skip_quality:
                quality.run_expectations(spark, cfg, "gold.fct_trips", run_id)

        log.info("%s concluida em %.1fs", layer, time.time() - t0)

    if cfg.optimize.get("enabled"):
        from . import io

        for key, cols in (cfg.optimize.get("zorder") or {}).items():
            layer, name = key.split(".", 1)
            if layer in layers:
                io.optimize(spark, cfg.table(layer, name), cols)

    log.info("Pipeline finalizado em %.1fs (run_id=%s)", time.time() - started, run_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
