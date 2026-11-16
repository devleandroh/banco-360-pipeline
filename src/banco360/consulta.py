"""
Consultas SQL diretamente sobre os arquivos Parquet do lake, sem carregar em banco.

Equivalente AWS: Amazon Athena (SQL serverless sobre o S3 usando o Glue Data Catalog).
Cada pasta da Gold vira uma view com o mesmo nome — o "catálogo" é a própria estrutura do lake.
"""
from __future__ import annotations

from pathlib import Path

import duckdb

from banco360.config import Config


def conectar(cfg: Config) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    for camada in ("gold", "silver"):
        base = Path(cfg.lake_dir, camada)
        if not base.exists():
            continue
        for pasta in sorted(p for p in base.iterdir() if p.is_dir() and not p.name.startswith("_")):
            nome = pasta.name if camada == "gold" else f"silver_{pasta.name}"
            padrao = (pasta / "**" / "*.parquet").as_posix()
            con.execute(f"create or replace view {nome} as select * from read_parquet('{padrao}', hive_partitioning = true)")
    return con


def consultar(cfg: Config, sql: str) -> str:
    con = conectar(cfg)
    try:
        rel = con.sql(sql)
        return rel.limit(200).__str__() if rel is not None else "ok"
    finally:
        con.close()
