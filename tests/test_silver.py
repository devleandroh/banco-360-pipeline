"""Testa as regras da Silver com um mini-lake montado à mão."""
import json
from pathlib import Path

from pyspark.sql import functions as F

from banco360.jobs import silver
from banco360.jobs.comum import ler_metricas
from datetime import date

D = date(2026, 11, 10)


def _mini_lake(cfg, spark):
    lake = Path(cfg.lake_dir)
    contas = spark.createDataFrame(
        [(1, 10, "0001", "100007-1", "0001-100007-1"), (2, 11, "0001", "100014-2", "0001-100014-2")],
        "conta_id int, cliente_id int, agencia string, numero string, chave_conta string")
    contas.write.parquet(str(lake / "silver" / "contas" / f"dt={D}"))

    cartoes = lake / "bronze" / "cartoes" / f"dt={D}"
    cartoes.mkdir(parents=True)
    (cartoes / "cartoes_20261110.csv").write_text(
        "id_transacao;data_hora;agencia;numero_conta;modalidade;valor;categoria_mcc;parcelas;status\n"
        "CRT1;10/11/2026 10:00:00;0001;100007-1;CREDITO;1.234,56;Lazer;3;APROVADA\n"
        "CRT1;10/11/2026 10:00:00;0001;100007-1;CREDITO;1.234,56;Lazer;3;APROVADA\n"   # duplicada
        "CRT2;10/11/2026 11:00:00;0001;100014-2;DEBITO;10,00;Farmácia;1;NEGADA\n"      # negada
        "CRT3;10/11/2026 12:00:00;0001;999999-9;DEBITO;50,00;Farmácia;1;APROVADA\n",   # conta inexistente
        encoding="utf-8")
    pag = lake / "bronze" / "pagamentos" / f"dt={D}"
    pag.mkdir(parents=True)
    linhas = [
        {"id": "PIX1", "tipo": "PIX", "dataHora": "2026-11-10T09:00:00-03:00", "conta": {"agencia": "0001", "numero": "100014-2"},
         "direcao": "RECEBIDO", "valor": "250.5", "contraparte": {"ispb": "1", "banco": "X"}, "chavePix": {"tipo": "EMAIL"}},
        {"id": "PIX2", "tipo": "PIX", "dataHora": "2026-11-10T09:30:00-03:00", "conta": {"agencia": "0001", "numero": "100014-2"},
         "direcao": "ENVIADO", "valor": -5, "contraparte": {"ispb": "1", "banco": "X"}},                       # valor inválido
    ]
    (pag / "pagamentos_20261110.jsonl").write_text("\n".join(json.dumps(x) for x in linhas), encoding="utf-8")


def test_silver_transacoes_limpa_deduplica_e_reconcilia(cfg, spark):
    _mini_lake(cfg, spark)
    n = silver.silver_transacoes(spark, cfg, [D])
    df = spark.read.parquet(str(Path(cfg.lake_dir, "silver", "transacoes")))
    assert n == 2
    crt1 = df.where("transacao_id = 'CRT1'").first()
    assert float(crt1["valor"]) == 1234.56 and crt1["canal"] == "Cartão de crédito" and crt1["cliente_id"] == 10
    pix = df.where("transacao_id = 'PIX1'").first()
    assert pix["sentido"] == "Entrada" and float(pix["valor"]) == 250.5

    rej = spark.read.parquet(str(Path(cfg.lake_dir, "silver", "_quarentena", "transacoes")))
    motivos = {r["transacao_id"]: r["motivo_rejeicao"] for r in rej.collect()}
    assert motivos == {"CRT3": "conta inexistente", "PIX2": "valor inválido"}

    m = ler_metricas(cfg, D, "silver_transacoes")
    assert m == {"bronze_linhas": 6, "negadas": 1, "duplicadas": 1, "rejeitadas": 2, "silver_linhas": 2}
    assert m["silver_linhas"] + m["negadas"] + m["duplicadas"] + m["rejeitadas"] == m["bronze_linhas"]


def test_silver_clientes_remove_pii(cfg, spark):
    pasta = Path(cfg.lake_dir, "bronze", "core_bancario", "clientes", f"dt={D}")
    spark.createDataFrame(
        [(1, "Ana Prado", "12345678909", date(1990, 1, 1), "a@exemplo.com.br", "sp", "Prime", 12000.0, 700, date(2024, 1, 1))],
        "cliente_id int, nome string, cpf string, data_nascimento date, email string, uf string, segmento string, "
        "renda_mensal double, score_interno int, data_cadastro date").write.parquet(str(pasta))
    silver.silver_clientes(spark, cfg, D)
    df = spark.read.parquet(str(Path(cfg.lake_dir, "silver", "clientes")))
    assert not {"cpf", "nome", "email", "data_nascimento"} & set(df.columns)
    r = df.first()
    assert len(r["cpf_hash"]) == 64 and r["uf"] == "SP" and r["regiao"] == "Sudeste"
    assert r["faixa_renda"] == "R$ 8 a 15 mil" and r["faixa_etaria"] == "35 a 44"
    assert df.select(F.sha2(F.lit("teste12345678909"), 256)).first()[0] == r["cpf_hash"]
