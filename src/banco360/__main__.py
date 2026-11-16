"""
Linha de comando do pipeline.

  python -m banco360 executar --data 2026-11-15 [--simular] [--etapas a,b,c]
  python -m banco360 backfill --inicio 2025-10-12 --fim 2026-11-15 --simular
  python -m banco360 consultar "select canal, sum(valor_total) from mart_transacoes_diarias group by 1"
  python -m banco360 ddl            # cria/atualiza o schema do warehouse
  python -m banco360 snapshot --saida banco-snapshot.json
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, timedelta

from banco360.config import Config
from banco360.pipeline import ETAPAS, executar


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s | %(message)s", datefmt="%H:%M:%S")
    logging.getLogger("py4j").setLevel(logging.WARNING)
    ontem = date.today() - timedelta(days=1)
    p = argparse.ArgumentParser(prog="banco360", description="Pipeline Banco 360 — lakehouse bancário")
    sub = p.add_subparsers(dest="cmd", required=True)

    e = sub.add_parser("executar", help="execução diária (D-1 por padrão)")
    e.add_argument("--data", type=date.fromisoformat, default=ontem)
    e.add_argument("--simular", action="store_true", help="gera os dados de origem fictícios antes de ingerir")
    e.add_argument("--etapas", help=f"subconjunto de etapas separadas por vírgula: {','.join(ETAPAS)}")
    e.add_argument("--execucao-id", help="id da execução (o Airflow passa o mesmo id em todas as tarefas)")
    e.add_argument("--parcial", action="store_true", help="tarefa intermediária: não marca a execução como concluída")

    b = sub.add_parser("backfill", help="reprocessa/reconstrói um intervalo de datas")
    b.add_argument("--inicio", type=date.fromisoformat, required=True)
    b.add_argument("--fim", type=date.fromisoformat, default=ontem)
    b.add_argument("--simular", action="store_true")

    q = sub.add_parser("consultar", help="SQL sobre os Parquet da Gold com DuckDB (papel do Amazon Athena)")
    q.add_argument("sql")

    sub.add_parser("ddl", help="aplica sql/warehouse/001_banco.sql no warehouse")

    s = sub.add_parser("snapshot", help="exporta as views públicas para JSON (cópia offline do portfólio)")
    s.add_argument("--saida", default="/data/banco-snapshot.json")

    a = p.parse_args(argv)
    cfg = Config()
    if a.cmd == "executar":
        etapas = a.etapas.split(",") if a.etapas else None
        executar(cfg, a.data, a.data, a.simular, etapas, a.execucao_id, a.parcial)
    elif a.cmd == "backfill":
        executar(cfg, a.inicio, a.fim, a.simular)
    elif a.cmd == "consultar":
        from banco360.consulta import consultar
        print(consultar(cfg, a.sql))
    elif a.cmd == "ddl":
        from banco360.warehouse.carga import aplicar_ddl
        aplicar_ddl(cfg)
        print("DDL aplicada.")
    elif a.cmd == "snapshot":
        from pathlib import Path

        from banco360.warehouse.snapshot import exportar
        print(exportar(cfg, Path(a.saida)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
