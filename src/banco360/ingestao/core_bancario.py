"""
Extração do banco relacional do core bancário (PostgreSQL) para a Bronze.

Equivalente AWS: um Glue Job com conexão JDBC (ou AWS DMS) gravando em
s3://<lake>/bronze/core_bancario/<tabela>/dt=<data>/.

As tabelas de cadastro e posição são pequenas, então a estratégia é snapshot
diário completo (full extract), igual ao que se faz com ERPs. O SQL de extração
é explícito (sem SELECT *) para que mudanças de schema na origem sejam
detectadas em vez de propagadas silenciosamente.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import psycopg

from banco360.config import Config
from banco360.ingestao import registrar_manifesto

EXTRACOES = {
    "clientes": """select cliente_id, nome, cpf, data_nascimento, email, uf, segmento,
                          renda_mensal::float8 as renda_mensal, score_interno, data_cadastro, updated_at
                   from core.clientes""",
    "contas": """select conta_id, cliente_id, agencia, numero, tipo, data_abertura, data_encerramento,
                        status, saldo::float8 as saldo, updated_at
                 from core.contas""",
    "contratos": """select contrato_id, cliente_id, produto, data_contratacao, valor_contratado::float8 as valor_contratado,
                           taxa_mensal::float8 as taxa_mensal, prazo_meses, valor_parcela::float8 as valor_parcela,
                           parcelas_pagas, saldo_devedor::float8 as saldo_devedor,
                           valor_em_atraso::float8 as valor_em_atraso, dias_atraso, status, updated_at
                    from core.contratos""",
}


def extrair(cfg: Config, d: date) -> dict[str, int]:
    resultado = {}
    ingestao_ts = datetime.now(timezone.utc).replace(tzinfo=None)
    with psycopg.connect(cfg.source_db_url) as conn, conn.cursor() as cur:
        for tabela, sql in EXTRACOES.items():
            cur.execute(sql)
            cols = [c.name for c in cur.description]
            df = pd.DataFrame(cur.fetchall(), columns=cols)
            for c in df.columns:
                if c.startswith("data_"):
                    df[c] = pd.to_datetime(df[c]).dt.date
            df["updated_at"] = pd.to_datetime(df["updated_at"], utc=True).dt.tz_localize(None)
            df["_ingestao_ts"] = ingestao_ts
            df["_origem"] = f"postgres://core_banking/core.{tabela}"
            pasta = cfg.camada("bronze") / "core_bancario" / tabela / f"dt={d.isoformat()}"
            pasta.mkdir(parents=True, exist_ok=True)
            destino = Path(pasta, f"{tabela}.parquet")
            # Spark lê timestamps em microssegundos; o pandas usa nanossegundos por padrão.
            df.to_parquet(destino, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)
            registrar_manifesto(cfg, d, f"core_bancario.{tabela}", destino, len(df))
            resultado[tabela] = len(df)
    return resultado
