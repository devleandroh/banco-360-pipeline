"""
Materializa os sistemas de origem fictícios para uma data de referência:

* banco relacional do core bancário (PostgreSQL): clientes, contas e contratos;
* zona de aterrissagem (landing) com os arquivos diários enviados pelos sistemas:
  - cartões: CSV no padrão brasileiro (separador ';' e vírgula decimal);
  - pagamentos (Pix, TED e boletos): JSON Lines com estrutura aninhada.
"""
from __future__ import annotations

import csv
import io
import json
from datetime import date, datetime
from pathlib import Path

import psycopg

from banco360.config import Config
from banco360.simulador import universo as u

DDL = """
create schema if not exists core;
create table if not exists core.clientes (
  cliente_id int primary key, nome text not null, cpf char(11) not null unique,
  data_nascimento date not null, email text, uf char(2) not null, segmento text not null,
  renda_mensal numeric(12,2) not null, score_interno int not null, data_cadastro date not null,
  updated_at timestamptz not null default now()
);
create table if not exists core.contas (
  conta_id int primary key, cliente_id int not null references core.clientes,
  agencia char(4) not null, numero text not null, tipo text not null,
  data_abertura date not null, data_encerramento date, status text not null,
  saldo numeric(14,2) not null, updated_at timestamptz not null default now(),
  unique (agencia, numero)
);
create table if not exists core.contratos (
  contrato_id int primary key, cliente_id int not null references core.clientes,
  produto text not null, data_contratacao date not null, valor_contratado numeric(14,2) not null,
  taxa_mensal numeric(8,5) not null, prazo_meses int not null, valor_parcela numeric(14,2) not null,
  parcelas_pagas int not null, saldo_devedor numeric(14,2) not null, valor_em_atraso numeric(14,2) not null,
  dias_atraso int not null, status text not null, updated_at timestamptz not null default now()
);
"""

COLUNAS = {
    "clientes": ["cliente_id", "nome", "cpf", "data_nascimento", "email", "uf", "segmento", "renda_mensal", "score_interno", "data_cadastro"],
    "contas": ["conta_id", "cliente_id", "agencia", "numero", "tipo", "data_abertura", "data_encerramento", "status", "saldo"],
    "contratos": ["contrato_id", "cliente_id", "produto", "data_contratacao", "valor_contratado", "taxa_mensal", "prazo_meses",
                  "valor_parcela", "parcelas_pagas", "saldo_devedor", "valor_em_atraso", "dias_atraso", "status"],
}


def carregar_core_bancario(cfg: Config, d: date) -> dict[str, int]:
    """Deixa o banco de origem exatamente no estado da data `d`."""
    linhas = {
        "clientes": [c.__dict__ for c in u.clientes_em(d)],
        "contas": u.contas_em(d),
        "contratos": u.contratos_em(d),
    }
    ts = datetime(d.year, d.month, d.day, 23, 0)
    with psycopg.connect(cfg.source_db_url, autocommit=False) as conn, conn.cursor() as cur:
        cur.execute(DDL)
        cur.execute("truncate core.contratos, core.contas, core.clientes")
        for tabela in ("clientes", "contas", "contratos"):
            cols = COLUNAS[tabela]
            with cur.copy(f"copy core.{tabela} ({', '.join(cols)}, updated_at) from stdin") as cp:
                for row in linhas[tabela]:
                    cp.write_row([row[c] for c in cols] + [ts])
        conn.commit()
    return {t: len(v) for t, v in linhas.items()}


def _brl(v: float) -> str:
    return f"{v:.2f}".replace(".", ",")


def gerar_arquivos_landing(cfg: Config, d: date) -> dict[str, int]:
    eventos = u.transacoes_do_dia(d)
    ymd = d.strftime("%Y%m%d")
    pasta_cartoes = Path(cfg.landing_dir, "cartoes")
    pasta_pag = Path(cfg.landing_dir, "pagamentos")
    pasta_cartoes.mkdir(parents=True, exist_ok=True)
    pasta_pag.mkdir(parents=True, exist_ok=True)

    buf = io.StringIO()
    w = csv.writer(buf, delimiter=";", lineterminator="\n")
    w.writerow(["id_transacao", "data_hora", "agencia", "numero_conta", "modalidade", "valor", "categoria_mcc", "parcelas", "status"])
    n_cartoes = 0
    with open(pasta_pag / f"pagamentos_{ymd}.jsonl", "w", encoding="utf-8") as fj:
        for e in eventos:
            if e["tipo"].startswith("CARTAO"):
                n_cartoes += 1
                w.writerow([
                    f"CRT{ymd}{e['seq']:07d}", e["data_hora"].strftime("%d/%m/%Y %H:%M:%S"), e["agencia"], e["numero_conta"],
                    "CREDITO" if e["tipo"] == "CARTAO_CREDITO" else "DEBITO", _brl(e["valor"]), e["mcc"], e["parcelas"], e["status"],
                ])
            else:
                registro = {
                    "id": f"{e['tipo'][:3]}{ymd}{e['seq']:07d}",
                    "tipo": e["tipo"],
                    "dataHora": e["data_hora"].isoformat() + "-03:00",
                    "conta": {"agencia": e["agencia"], "numero": e["numero_conta"]},
                    "direcao": e["direcao"],
                    # Alguns sistemas legados enviam o valor como texto — o pipeline normaliza.
                    "valor": str(e["valor"]) if e["seq"] % 17 == 0 else e["valor"],
                    "contraparte": {"ispb": e["ispb"], "banco": e["banco"]},
                }
                if e["chave"]:
                    registro["chavePix"] = {"tipo": e["chave"]}
                fj.write(json.dumps(registro, ensure_ascii=False) + "\n")
    (pasta_cartoes / f"cartoes_{ymd}.csv").write_text(buf.getvalue(), encoding="utf-8")
    return {"cartoes": n_cartoes, "pagamentos": len(eventos) - n_cartoes}
