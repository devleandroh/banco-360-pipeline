"""
Jobs Bronze → Silver (limpeza, padronização, LGPD e regras de negócio).

Equivalente AWS: Glue Jobs (Spark) lendo s3://<lake>/bronze e gravando
s3://<lake>/silver em Parquet, com as tabelas registradas no Glue Data Catalog.

Regras da Silver:
* tipos corretos, nomes padronizados, sem duplicidade;
* dados pessoais tratados (LGPD): CPF vira hash irreversível com "sal" secreto;
  nome, e-mail e data de nascimento não passam desta camada;
* registros inválidos vão para a quarentena com o motivo (nada some sem rastro).
"""
from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from banco360.config import Config
from banco360.jobs.comum import (
    argumentos_glue, caminho, gravar, ler_particoes, particoes, salvar_metricas, spark_session, ultima_particao,
)

REGIOES = {
    "Sudeste": ["SP", "RJ", "MG", "ES"], "Sul": ["PR", "SC", "RS"],
    "Nordeste": ["BA", "PE", "CE", "MA", "PB", "RN", "AL", "SE", "PI"],
    "Centro-Oeste": ["GO", "DF", "MT", "MS"], "Norte": ["PA", "AM", "RO", "AC", "AP", "RR", "TO"],
}


def _regiao(uf: F.Column) -> F.Column:
    expr = F.lit("Não informada")
    for regiao, ufs in REGIOES.items():
        expr = F.when(uf.isin(ufs), regiao).otherwise(expr)
    return expr


def faixa_renda(col: str) -> F.Column:
    c = F.col(col)
    return (F.when(c < 3000, "Até R$ 3 mil").when(c < 8000, "R$ 3 a 8 mil").when(c < 15000, "R$ 8 a 15 mil")
            .when(c < 30000, "R$ 15 a 30 mil").otherwise("Acima de R$ 30 mil"))


def faixa_score(col: str) -> F.Column:
    c = F.col(col)
    return F.when(c < 450, "Muito baixo").when(c < 550, "Baixo").when(c < 650, "Médio").when(c < 750, "Alto").otherwise("Muito alto")


# ---------------------------------------------------------------------------
# Cadastros e posições (snapshots do core bancário)
# ---------------------------------------------------------------------------
def silver_clientes(spark: SparkSession, cfg: Config, d: date) -> int:
    dt = ultima_particao(cfg, "bronze", "core_bancario", "clientes", ate=d)
    bronze = spark.read.parquet(caminho(cfg, "bronze", "core_bancario", "clientes", f"dt={dt.isoformat()}"))
    idade = F.floor(F.months_between(F.lit(dt.isoformat()).cast("date"), F.col("data_nascimento")) / 12)
    df = (
        bronze.dropDuplicates(["cliente_id"])
        .select(
            F.col("cliente_id").cast("int"),
            # LGPD: hash SHA-256 com sal secreto — permite cruzar bases sem expor o CPF
            F.sha2(F.concat(F.lit(cfg.pii_salt), F.regexp_replace("cpf", r"\D", "")), 256).alias("cpf_hash"),
            F.upper(F.trim("uf")).alias("uf"),
            _regiao(F.upper(F.trim("uf"))).alias("regiao"),
            F.initcap(F.trim("segmento")).alias("segmento"),
            F.col("renda_mensal").cast(T.DecimalType(12, 2)).alias("renda_mensal"),
            faixa_renda("renda_mensal").alias("faixa_renda"),
            F.col("score_interno").cast("int"),
            faixa_score("score_interno").alias("faixa_score"),
            F.when(idade < 25, "18 a 24").when(idade < 35, "25 a 34").when(idade < 45, "35 a 44")
            .when(idade < 60, "45 a 59").otherwise("60 ou mais").alias("faixa_etaria"),
            F.col("data_cadastro").cast("date"),
            F.lit(dt.isoformat()).alias("dt_referencia"),
        )
    )
    gravar(df, caminho(cfg, "silver", "clientes"))
    return df.count()


def silver_contas(spark: SparkSession, cfg: Config, datas: list[date]) -> int:
    disponiveis = [d for d in particoes(cfg, "bronze", "core_bancario", "contas") if d in datas]
    if not disponiveis:
        return 0
    bronze = ler_particoes(spark, cfg, "bronze", "core_bancario/contas", disponiveis)
    df = (
        bronze.dropDuplicates(["dt", "conta_id"])
        .select(
            F.col("conta_id").cast("int"), F.col("cliente_id").cast("int"),
            F.col("agencia"), F.col("numero"),
            F.concat_ws("-", "agencia", "numero").alias("chave_conta"),
            F.col("tipo"), F.col("data_abertura").cast("date"), F.col("data_encerramento").cast("date"),
            F.col("status"), F.col("saldo").cast(T.DecimalType(14, 2)).alias("saldo"), F.col("dt"),
        )
    )
    gravar(df, caminho(cfg, "silver", "contas"), "dt")
    return df.count()


def silver_contratos(spark: SparkSession, cfg: Config, datas: list[date]) -> int:
    disponiveis = [d for d in particoes(cfg, "bronze", "core_bancario", "contratos") if d in datas]
    if not disponiveis:
        return 0
    bronze = ler_particoes(spark, cfg, "bronze", "core_bancario/contratos", disponiveis)
    dias = F.col("dias_atraso")
    baixado = F.col("status") == "Baixado para prejuízo"
    # Estágios de risco inspirados na Res. CMN 4.966 / IFRS 9 (simplificado):
    # 1 = sem aumento significativo de risco, 2 = atraso de 31 a 90 dias, 3 = problemático (> 90 dias)
    estagio = F.when(baixado | (dias > 90), 3).when(dias > 30, 2).otherwise(1)
    df = (
        bronze.dropDuplicates(["dt", "contrato_id"])
        .select(
            F.col("contrato_id").cast("int"), F.col("cliente_id").cast("int"), F.trim("produto").alias("produto"),
            F.col("data_contratacao").cast("date"), F.date_format("data_contratacao", "yyyy-MM").alias("safra"),
            F.col("valor_contratado").cast(T.DecimalType(14, 2)), F.col("taxa_mensal").cast(T.DecimalType(8, 5)),
            F.col("prazo_meses").cast("int"), F.col("valor_parcela").cast(T.DecimalType(14, 2)),
            F.col("parcelas_pagas").cast("int"), F.col("saldo_devedor").cast(T.DecimalType(14, 2)),
            F.col("valor_em_atraso").cast(T.DecimalType(14, 2)), dias.cast("int").alias("dias_atraso"),
            F.when(dias == 0, "Em dia").when(dias <= 30, "1 a 30 dias").when(dias <= 60, "31 a 60 dias")
            .when(dias <= 90, "61 a 90 dias").when(dias <= 180, "91 a 180 dias").when(dias <= 360, "181 a 360 dias")
            .otherwise("Acima de 360 dias").alias("faixa_atraso"),
            estagio.alias("estagio"),
            (~F.col("status").isin("Liquidado", "Baixado para prejuízo")).alias("em_carteira"),
            F.col("status"), F.col("dt"),
        )
        .withColumn(
            "provisao_estimada",
            F.when(~F.col("em_carteira"), F.lit(0))
            .otherwise(F.col("saldo_devedor") * F.when(F.col("estagio") == 1, 0.01).when(F.col("estagio") == 2, 0.12).otherwise(0.60))
            .cast(T.DecimalType(14, 2)),
        )
    )
    gravar(df, caminho(cfg, "silver", "contratos"), "dt")
    return df.count()


# ---------------------------------------------------------------------------
# Transações (CSV de cartões + JSON de pagamentos)
# ---------------------------------------------------------------------------
SCHEMA_PAGAMENTOS = T.StructType([
    T.StructField("id", T.StringType()),
    T.StructField("tipo", T.StringType()),
    T.StructField("dataHora", T.StringType()),
    T.StructField("conta", T.StructType([T.StructField("agencia", T.StringType()), T.StructField("numero", T.StringType())])),
    T.StructField("direcao", T.StringType()),
    T.StructField("valor", T.StringType()),  # número ou texto, conforme o sistema que enviou
    T.StructField("contraparte", T.StructType([T.StructField("ispb", T.StringType()), T.StructField("banco", T.StringType())])),
    T.StructField("chavePix", T.StructType([T.StructField("tipo", T.StringType())])),
])

CANAIS = {"PIX": "Pix", "TED": "TED", "BOLETO": "Boleto", "CARTAO_DEBITO": "Cartão de débito", "CARTAO_CREDITO": "Cartão de crédito"}


def _dt_do_arquivo() -> F.Column:
    return F.regexp_extract(F.input_file_name(), r"dt=(\d{4}-\d{2}-\d{2})", 1)


def _cartoes(spark: SparkSession, cfg: Config, datas: list[date]) -> DataFrame:
    paths = [caminho(cfg, "bronze", "cartoes", f"dt={d.isoformat()}") for d in datas]
    raw = spark.read.option("header", True).option("sep", ";").option("encoding", "UTF-8").csv(paths)  # tudo como texto
    return raw.select(
        F.col("id_transacao").alias("transacao_id"),
        _dt_do_arquivo().alias("dt"),
        F.to_timestamp("data_hora", "dd/MM/yyyy HH:mm:ss").alias("data_hora"),
        F.when(F.col("modalidade") == "CREDITO", "CARTAO_CREDITO").otherwise("CARTAO_DEBITO").alias("tipo"),
        F.lit("Saída").alias("sentido"),
        F.col("agencia"), F.col("numero_conta"),
        # padrão brasileiro: "1.234,56" → 1234.56
        F.regexp_replace(F.regexp_replace("valor", r"\.", ""), ",", ".").cast(T.DecimalType(14, 2)).alias("valor"),
        F.col("categoria_mcc").alias("categoria"),
        F.col("parcelas").cast("int").alias("parcelas"),
        F.lit(None).cast("string").alias("banco_contraparte"),
        F.col("status").alias("status_origem"),
    )


def _pagamentos(spark: SparkSession, cfg: Config, datas: list[date]) -> DataFrame:
    paths = [caminho(cfg, "bronze", "pagamentos", f"dt={d.isoformat()}") for d in datas]
    raw = spark.read.schema(SCHEMA_PAGAMENTOS).json(paths)
    return raw.select(
        F.col("id").alias("transacao_id"),
        _dt_do_arquivo().alias("dt"),
        F.to_timestamp(F.substring("dataHora", 1, 19), "yyyy-MM-dd'T'HH:mm:ss").alias("data_hora"),
        F.upper("tipo").alias("tipo"),
        F.when(F.col("direcao") == "RECEBIDO", "Entrada").otherwise("Saída").alias("sentido"),
        F.col("conta.agencia").alias("agencia"), F.col("conta.numero").alias("numero_conta"),
        F.col("valor").cast(T.DecimalType(14, 2)).alias("valor"),
        F.coalesce(F.concat(F.lit("Chave "), F.lower("chavePix.tipo")), F.lit("Sem chave")).alias("categoria"),
        F.lit(1).alias("parcelas"),
        F.col("contraparte.banco").alias("banco_contraparte"),
        F.lit("APROVADA").alias("status_origem"),
    )


def silver_transacoes(spark: SparkSession, cfg: Config, datas: list[date]) -> int:
    datas = [d for d in datas if d in set(particoes(cfg, "bronze", "cartoes"))]
    if not datas:
        return 0
    bruto = _cartoes(spark, cfg, datas).unionByName(_pagamentos(spark, cfg, datas)).cache()

    aprovadas = bruto.where(F.col("status_origem") == "APROVADA")
    # Deduplicação: o mesmo id reenviado pela origem conta uma vez só
    w = Window.partitionBy("transacao_id").orderBy(F.col("data_hora").asc_nulls_last())
    unicas = aprovadas.withColumn("_n", F.row_number().over(w)).where("_n = 1").drop("_n")

    dt_contas = ultima_particao(cfg, "silver", "contas", ate=max(datas))
    contas = (spark.read.parquet(caminho(cfg, "silver", "contas", f"dt={dt_contas.isoformat()}"))
              .select("chave_conta", "conta_id", "cliente_id"))
    enriquecidas = unicas.join(
        F.broadcast(contas), F.concat_ws("-", "agencia", "numero_conta") == F.col("chave_conta"), "left"
    )
    motivo = (F.when(F.col("data_hora").isNull(), "data/hora inválida")
              .when(F.col("valor").isNull() | (F.col("valor") <= 0), "valor inválido")
              .when(F.col("conta_id").isNull(), "conta inexistente"))
    marcadas = enriquecidas.withColumn("motivo_rejeicao", motivo).cache()

    validas = marcadas.where(F.col("motivo_rejeicao").isNull()).select(
        "transacao_id", "data_hora", F.hour("data_hora").alias("hora"), "tipo",
        F.element_at(F.create_map(*[x for k, v in CANAIS.items() for x in (F.lit(k), F.lit(v))]), F.col("tipo")).alias("canal"),
        "sentido", "conta_id", "cliente_id", "valor", "categoria", "parcelas", "banco_contraparte", "dt",
    )
    rejeitadas = marcadas.where(F.col("motivo_rejeicao").isNotNull()).select(
        "transacao_id", "data_hora", "tipo", "agencia", "numero_conta", "valor", "motivo_rejeicao", "dt",
    )
    gravar(validas, caminho(cfg, "silver", "transacoes"), "dt")
    gravar(rejeitadas, caminho(cfg, "silver", "_quarentena", "transacoes"), "dt")

    # Métricas por dia para a reconciliação Bronze × Silver na etapa de qualidade
    def por_dia(df: DataFrame, nome: str) -> dict[str, int]:
        return {r["dt"]: r[nome] for r in df.groupBy("dt").agg(F.count("*").alias(nome)).collect()}

    m_bruto, m_aprov = por_dia(bruto, "n"), por_dia(aprovadas, "n")
    m_unicas, m_rej = por_dia(unicas, "n"), por_dia(rejeitadas, "n")
    m_validas = por_dia(spark.read.parquet(caminho(cfg, "silver", "transacoes")).where(
        F.col("dt").cast("string").isin([d.isoformat() for d in datas])).withColumn("dt", F.col("dt").cast("string")), "n")
    for d in datas:
        k = d.isoformat()
        salvar_metricas(cfg, d, "silver_transacoes", {
            "bronze_linhas": m_bruto.get(k, 0),
            "negadas": m_bruto.get(k, 0) - m_aprov.get(k, 0),
            "duplicadas": m_aprov.get(k, 0) - m_unicas.get(k, 0),
            "rejeitadas": m_rej.get(k, 0),
            "silver_linhas": m_validas.get(k, 0),
        })
    bruto.unpersist()
    marcadas.unpersist()
    return sum(m_validas.values())


# ---------------------------------------------------------------------------
# Indicadores macroeconômicos (API do Banco Central)
# ---------------------------------------------------------------------------
def silver_macro(spark: SparkSession, cfg: Config, d: date) -> int:
    dfs = []
    for serie in ("selic_meta", "ipca_mensal", "dolar_ptax"):
        try:
            dt = ultima_particao(cfg, "bronze", "bcb", f"serie={serie}", ate=d)
        except FileNotFoundError:
            continue
        raw = spark.read.option("multiLine", True).json(caminho(cfg, "bronze", "bcb", f"serie={serie}", f"dt={dt.isoformat()}"))
        dfs.append(
            raw.select("serie", F.explode("dados").alias("p"))
            .select("serie", F.to_date("p.data", "dd/MM/yyyy").alias("data"), F.col("p.valor").cast("double").alias("valor"))
            .withColumn("coletado_em", F.lit(dt.isoformat()))
        )
    if not dfs:
        return 0
    df = dfs[0]
    for outro in dfs[1:]:
        df = df.unionByName(outro)
    # A série da meta SELIC já traz datas futuras (vigência até a próxima reunião do Copom)
    df = df.where(F.col("data") <= F.lit(d.isoformat()).cast("date")).dropDuplicates(["serie", "data"])
    gravar(df, caminho(cfg, "silver", "indicadores_macro"))
    return df.count()


def executar(spark: SparkSession, cfg: Config, datas: list[date]) -> dict[str, int]:
    fim = max(datas)
    return {
        "clientes": silver_clientes(spark, cfg, fim),
        "contas": silver_contas(spark, cfg, datas),
        "contratos": silver_contratos(spark, cfg, datas),
        "transacoes": silver_transacoes(spark, cfg, datas),
        "indicadores_macro": silver_macro(spark, cfg, fim),
    }


if __name__ == "__main__":
    cfg, datas = argumentos_glue("Bronze → Silver")
    print(executar(spark_session("bronze_para_silver"), cfg, datas))
