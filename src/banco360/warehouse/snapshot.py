"""
Exporta as views públicas (banco_v_*) para um JSON.

O portfólio usa esse arquivo como cópia local quando o Supabase está indisponível,
garantindo que a demonstração nunca fique fora do ar.
"""
from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from banco360.config import Config
from banco360.warehouse.carga import conectar

VIEWS = ["carteira_mensal", "perfil_risco", "transacoes_diarias", "clientes_mensal", "macro_mensal", "execucoes", "qualidade"]


def _json(v):
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, datetime):
        return v.isoformat(timespec="seconds")
    if isinstance(v, date):
        return v.isoformat()
    return str(v)


def exportar(cfg: Config, saida: Path) -> dict[str, int]:
    dados: dict[str, list] = {}
    with conectar(cfg) as conn, conn.cursor() as cur:
        for v in VIEWS:
            cur.execute(f"select * from public.banco_v_{v}")
            cols = [c.name for c in cur.description]
            dados[v] = [dict(zip(cols, r)) for r in cur.fetchall()]
    dados["_gerado_em"] = datetime.now().isoformat(timespec="seconds")  # type: ignore[assignment]
    saida.parent.mkdir(parents=True, exist_ok=True)
    saida.write_text(json.dumps(dados, default=_json, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    return {v: len(dados[v]) for v in VIEWS}
