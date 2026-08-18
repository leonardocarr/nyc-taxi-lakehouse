"""Carregamento e resolucao da configuracao do pipeline.

O objetivo aqui e que nenhum nome de tabela, caminho ou regra de negocio
apareca hardcoded no codigo de transformacao. Tudo passa por este modulo.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[2] / "conf" / "pipeline.yml"


@dataclass(frozen=True)
class SourceConfig:
    mode: str
    volume_path: str
    zones_path: str


@dataclass(frozen=True)
class Rule:
    name: str
    expression: str


@dataclass(frozen=True)
class Expectation:
    name: str
    severity: str
    query: str
    expect: str


@dataclass
class Config:
    profile: str
    catalog: str | None
    warehouse_dir: str | None
    schemas: dict[str, str]
    source: SourceConfig
    service: str
    months: list[str]
    base_url: str
    zones_url: str
    rules: list[Rule]
    expectations: dict[str, list[Expectation]]
    optimize: dict[str, Any] = field(default_factory=dict)

    # ------------------------------------------------------------------ #
    # Resolucao de nomes de tabela
    # ------------------------------------------------------------------ #
    @property
    def catalog_prefix(self) -> str:
        """'nyc_taxi.' no Unity Catalog, '' no metastore local."""
        return f"{self.catalog}." if self.catalog else ""

    def schema(self, layer: str) -> str:
        """Nome totalmente qualificado do schema de uma camada."""
        return f"{self.catalog_prefix}{self.schemas[layer]}"

    def table(self, layer: str, name: str) -> str:
        """Nome totalmente qualificado de uma tabela.

        >>> cfg.table("gold", "fct_trips")
        'nyc_taxi.gold.fct_trips'   # perfil databricks
        'gold.fct_trips'            # perfil local
        """
        return f"{self.schema(layer)}.{name}"

    def expectations_for(self, key: str) -> list[Expectation]:
        return self.expectations.get(key, [])


def _parse_expectations(raw: dict[str, Any]) -> dict[str, list[Expectation]]:
    out: dict[str, list[Expectation]] = {}
    for table_key, items in (raw or {}).items():
        out[table_key] = [
            Expectation(
                name=i["name"],
                severity=i.get("severity", "fail"),
                query=" ".join(i["query"].split()),
                expect=i["expect"],
            )
            for i in items
        ]
    return out


def load_config(
    path: str | Path | None = None,
    profile: str | None = None,
    months: list[str] | None = None,
) -> Config:
    """Le o YAML, aplica o perfil escolhido e devolve um Config imutavel-ish.

    Precedencia do perfil: argumento > env NYC_TAXI_PROFILE > campo `profile`
    do proprio YAML.
    """
    path = Path(path or os.getenv("NYC_TAXI_CONFIG", DEFAULT_CONFIG_PATH))
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))

    profile = profile or os.getenv("NYC_TAXI_PROFILE") or raw["profile"]
    if profile not in raw["profiles"]:
        raise ValueError(
            f"Perfil '{profile}' nao existe. Disponiveis: {list(raw['profiles'])}"
        )
    p = raw["profiles"][profile]

    return Config(
        profile=profile,
        catalog=p.get("catalog"),
        warehouse_dir=p.get("warehouse_dir"),
        schemas=p["schemas"],
        source=SourceConfig(**p["source"]),
        service=raw["ingestion"]["service"],
        months=months or raw["ingestion"]["months"],
        base_url=raw["ingestion"]["base_url"].rstrip("/"),
        zones_url=raw["ingestion"]["zones_url"],
        rules=[Rule(**r) for r in raw["silver"]["rules"]],
        expectations=_parse_expectations(raw.get("expectations", {})),
        optimize=raw.get("optimize", {}),
    )
