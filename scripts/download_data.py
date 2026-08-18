#!/usr/bin/env python3
"""Baixa os arquivos da TLC para ``data/landing/``.

Util em dois cenarios:

1. Rodar o perfil ``local`` sem depender de rede durante o pipeline.
2. Preparar os arquivos para upload manual em um UC Volume quando o workspace
   Databricks bloqueia saida para a internet (caso do Free Edition).

    python scripts/download_data.py --months 2024-01 2024-02 2024-03
"""

from __future__ import annotations

import argparse
import sys
import urllib.request
from pathlib import Path

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONES_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"


def _progress(block: int, block_size: int, total: int) -> None:
    if total <= 0:
        return
    pct = min(100, block * block_size * 100 // total)
    print(f"\r  {pct:3d}%", end="", flush=True)


def download(url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists() and dest.stat().st_size > 0:
        print(f"[cache] {dest.name}")
        return
    print(f"[baixando] {dest.name}")
    urllib.request.urlretrieve(url, dest, reporthook=_progress)  # noqa: S310
    print(f"\r  ok ({dest.stat().st_size / 1e6:.1f} MB)")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--months", nargs="+", required=True, help="ex.: 2024-01 2024-02")
    parser.add_argument("--service", default="yellow", choices=("yellow", "green"))
    parser.add_argument("--out", default="data/landing")
    args = parser.parse_args()

    out = Path(args.out)
    download(ZONES_URL, out / "taxi_zone_lookup.csv")
    for month in args.months:
        name = f"{args.service}_tripdata_{month}.parquet"
        download(f"{BASE_URL}/{name}", out / name)

    print(f"\nArquivos em {out.resolve()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
