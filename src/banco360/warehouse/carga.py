"""
Carga Gold (Parquet) → Data Warehouse (PostgreSQL).

Equivalente AWS: comando COPY do Amazon Redshift lendo os Parquet do S3.
Aqui usamos o COPY do PostgreSQL (protocolo binário de alta vazão), sempre em
transação — ou a carga inteira entra, ou nada muda.

Estratégias por tipo de tabela:
* dimensões → upsert (insert ... on conflict do update), preservando as FKs dos fatos;
* fatos     → delete + insert das partições reprocessadas (idempotente: reprocessar
               um dia não duplica dados);
* marts     → substituição completa (são pequenos e recalculados a cada execução).
"""
from __future__ import annotations

import json
import logging
from datetime import date, datetime, timezone
from pathlib import Path

import pyarrow.dataset as ds
import pyarrow as pa
import psycopg

from banco360.config import Config
from banco360.jobs.comum import particoes

log = logging.getLogger(__name__)

DDL = Path(__file__).resolve().parents[3] / "sql" / "warehouse" / "001_banco.sql"

DIMENSOES = {  # tabela: chave primária
    "dim_tempo": "data_sk", "dim_cliente": "cliente_sk", "dim_produto": "produto_sk",
    "dim_canal": "canal_sk", "dim_conta": "conta_sk",
}
FATOS = {"fato_transacao": "data_sk", "fato_posicao_contrato": "data_sk"}
MARTS = ["mart_carteira_mensal", "mart_perfil_risco", "mart_transacoes_diarias", "mart_clientes_mensal", "mart_macro_mensal"]


def conectar(cfg: Config) -> psycopg.Connection:
    return psycopg.connect(cfg.warehouse_db_url, connect_timeout=20, prepare_threshold=None)


def aplicar_ddl(cfg: Config) -> None:
    with conectar(cfg) as conn:
        conn.execute(DDL.read_text(encoding="utf-8"))


def _tabela(caminho: Path, colunas: list[str]) -> pa.Table:
    t = ds.dataset(str(caminho), format="parquet", partitioning="hive").to_table()
    return t.select([c for c in colunas if c in t.column_names])


def _colunas(cur: psycopg.Cursor, tabela: str) -> list[str]:
    cur.execute(
        "select column_name from information_schema.columns where table_schema = 'banco' and table_name = %s "
        "and is_identity = 'NO' order by ordinal_position", (tabela,))
    return [r[0] for r in cur.fetchall()]


def _copy(cur: psycopg.Cursor, destino: str, cols: list[str], t: pa.Table) -> int:
    with cur.copy(f"copy {destino} ({', '.join(cols)}) from stdin") as cp:
        for lote in t.to_batches(max_chunksize=20_000):
            for row in zip(*[lote.column(c).to_pylist() for c in cols]):
                cp.write_row(row)
    return t.num_rows


def _upsert(cur: psycopg.Cursor, tabela: str, pk: str, t: pa.Table, cols: list[str]) -> int:
    cur.execute(f"create temp table _stg (like banco.{tabela} including defaults) on commit drop")
    n = _copy(cur, "_stg", cols, t)
    sets = ", ".join(f"{c} = excluded.{c}" for c in cols if c != pk)
    cur.execute(f"insert into banco.{tabela} ({', '.join(cols)}) select {', '.join(cols)} from _stg "
                f"on conflict ({pk}) do update set {sets}")
    cur.execute("drop table _stg")
    return n


def carregar(cfg: Config, datas: list[date]) -> dict[str, int]:
    gold = Path(cfg.lake_dir, "gold")
    out: dict[str, int] = {}
    with conectar(cfg) as conn, conn.cursor() as cur:
        for tabela, pk in DIMENSOES.items():
            cols = _colunas(cur, tabela)
            out[tabela] = _upsert(cur, tabela, pk, _tabela(gold / tabela, cols), cols)

        for tabela, col_data in FATOS.items():
            cols = _colunas(cur, tabela)
            disponiveis = [d for d in datas if d in set(particoes(cfg, "gold", tabela))]
            total = 0
            for d in disponiveis:
                t = _tabela(gold / tabela / f"dt={d.isoformat()}", cols)
                cur.execute(f"delete from banco.{tabela} where {col_data} = %s", (int(d.strftime("%Y%m%d")),))
                total += _copy(cur, f"banco.{tabela}", cols, t)
            out[tabela] = total

        for tabela in MARTS:
            cols = _colunas(cur, tabela)
            cur.execute(f"delete from banco.{tabela}")
            out[tabela] = _copy(cur, f"banco.{tabela}", cols, _tabela(gold / tabela, cols))

        # Retenção: remove partições diárias fora da janela (mantém fechamentos mensais das posições)
        limite_sk = int(date.fromordinal(max(datas).toordinal() - cfg.retencao_dias).strftime("%Y%m%d"))
        cur.execute("delete from banco.fato_transacao where data_sk < %s", (limite_sk,))
        conn.commit()
    return out


# ---------------------------------------------------------------------------
# Registro de execuções e resultados de qualidade
# ---------------------------------------------------------------------------
def registrar_inicio(cfg: Config, execucao_id: str, d: date, modo: str, inicio: datetime) -> None:
    with conectar(cfg) as conn:
        conn.execute(
            "insert into banco.pipeline_execucoes (execucao_id, data_referencia, modo, origem, status, inicio) "
            "values (%s, %s, %s, %s, 'em_execucao', %s) on conflict (execucao_id) do nothing",
            (execucao_id, d, modo, cfg.origem_execucao, inicio),
        )


def registrar_fim(cfg: Config, execucao_id: str, status: str, etapas: list[dict], volumes: dict,
                  erro: str | None, qualidade: list[dict], parcial: bool = False) -> None:
    """Acrescenta as etapas desta chamada ao registro da execução.

    Com `parcial=True` (tarefas intermediárias do Airflow, cada uma num processo), a
    execução continua "em_execucao" a menos que a etapa tenha falhado.
    """
    fim = datetime.now(timezone.utc)
    if parcial and status == "sucesso":
        status = "em_execucao"
    with conectar(cfg) as conn, conn.cursor() as cur:
        cur.execute(
            "update banco.pipeline_execucoes set status = %s, "
            "fim = case when %s = 'em_execucao' then null else %s end, "
            "duracao_s = round(extract(epoch from (%s - inicio))::numeric, 1), "
            "etapas = etapas || %s::jsonb, volumes = volumes || %s::jsonb, erro = coalesce(%s, erro) "
            "where execucao_id = %s",
            (status, status, fim, fim, json.dumps(etapas, ensure_ascii=False),
             json.dumps(volumes, ensure_ascii=False), erro, execucao_id),
        )
        if qualidade:
            with cur.copy("copy banco.dq_resultados (execucao_id, data_referencia, camada, dataset, regra, coluna, severidade, "
                          "status, observado, esperado, detalhe) from stdin") as cp:
                for q in qualidade:
                    cp.write_row([execucao_id, q["data_referencia"], q["camada"], q["dataset"], q["regra"], q["coluna"],
                                  q["severidade"], q["status"], q["observado"], q["esperado"], q["detalhe"]])
        # mantém o histórico de observabilidade enxuto
        cur.execute("delete from banco.pipeline_execucoes where inicio < now() - interval '120 days'")
        conn.commit()
