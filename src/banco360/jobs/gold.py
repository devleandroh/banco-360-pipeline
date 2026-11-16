"""
Jobs Silver → Gold: modelo dimensional (estrela) e marts analíticos.

Equivalente AWS: Glue Jobs gravando s3://<lake>/gold, consultável pelo Athena e
carregado no Amazon Redshift (COPY a partir do S3).

Modelo estrela:
  fato_transacao          (grão: uma transação)       → dim_tempo, dim_cliente, dim_conta, dim_canal
  fato_posicao_contrato   (grão: contrato × data)     → dim_tempo, dim_cliente, dim_produto

Chaves substitutas (surrogate keys) são hashes estáveis da chave natural
(xxhash64), então não dependem de sequências e são reprodutíveis entre execuções.
"""
from __future__ import annotations

from datetime import date, timedelta

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from banco360.config import Config
from banco360.jobs.comum import (
    argumentos_glue, caminho, gravar, ler_particoes, particoes, spark_session, ultima_particao,
)
from banco360.simulador.universo import EPOCH


def sk(nome: str, *cols: str) -> F.Column:
    return F.xxhash64(F.lit(nome), *[F.col(c) for c in cols])


def data_sk(col: str) -> F.Column:
    return F.date_format(F.col(col), "yyyyMMdd").cast("int")


GARANTIA = {"Consignado": "Consignação em folha", "Financiamento de veículos": "Alienação fiduciária",
            "Crédito imobiliário": "Alienação fiduciária"}


# ---------------------------------------------------------------------------
# Dimensões
# ---------------------------------------------------------------------------
def dimensoes(spark: SparkSession, cfg: Config, fim: date) -> dict[str, int]:
    out = {}
    clientes = spark.read.parquet(caminho(cfg, "silver", "clientes"))
    dim_cliente = clientes.select(
        sk("cliente", "cliente_id").alias("cliente_sk"), "cliente_id", "cpf_hash", "segmento", "faixa_renda",
        "faixa_etaria", "faixa_score", "uf", "regiao", "data_cadastro",
        F.date_format("data_cadastro", "yyyy-MM").alias("safra_cadastro"),
    )
    gravar(dim_cliente, caminho(cfg, "gold", "dim_cliente"))
    out["dim_cliente"] = dim_cliente.count()

    dt_contas = ultima_particao(cfg, "silver", "contas", ate=fim)
    contas = spark.read.parquet(caminho(cfg, "silver", "contas", f"dt={dt_contas.isoformat()}"))
    dim_conta = contas.select(
        sk("conta", "conta_id").alias("conta_sk"), "conta_id", sk("cliente", "cliente_id").alias("cliente_sk"),
        "agencia", "tipo", "status", "data_abertura",
    )
    gravar(dim_conta, caminho(cfg, "gold", "dim_conta"))
    out["dim_conta"] = dim_conta.count()

    dt_ctr = ultima_particao(cfg, "silver", "contratos", ate=fim)
    produtos = spark.read.parquet(caminho(cfg, "silver", "contratos", f"dt={dt_ctr.isoformat()}")).select("produto").distinct()
    garantia = F.lit("Sem garantia")
    for p, g in GARANTIA.items():
        garantia = F.when(F.col("produto") == p, g).otherwise(garantia)
    dim_produto = produtos.select(sk("produto", "produto").alias("produto_sk"), "produto", garantia.alias("garantia"))
    gravar(dim_produto, caminho(cfg, "gold", "dim_produto"))
    out["dim_produto"] = dim_produto.count()

    canais = [("PIX", "Pix", "Pagamento instantâneo"), ("TED", "TED", "Transferência"), ("BOLETO", "Boleto", "Pagamento de contas"),
              ("CARTAO_DEBITO", "Cartão de débito", "Cartão"), ("CARTAO_CREDITO", "Cartão de crédito", "Cartão")]
    dim_canal = spark.createDataFrame(canais, "tipo string, canal string, meio string").select(
        sk("canal", "tipo").alias("canal_sk"), "tipo", "canal", "meio")
    gravar(dim_canal, caminho(cfg, "gold", "dim_canal"))
    out["dim_canal"] = dim_canal.count()

    dias = (fim - EPOCH).days + 1
    nomes = F.array(*[F.lit(n) for n in ["domingo", "segunda", "terça", "quarta", "quinta", "sexta", "sábado"]])
    dim_tempo = (
        spark.range(dias).select(F.date_add(F.lit(EPOCH.isoformat()).cast("date"), F.col("id").cast("int")).alias("data"))
        .select(
            data_sk("data").alias("data_sk"), "data", F.year("data").alias("ano"), F.month("data").alias("mes"),
            F.date_format("data", "yyyy-MM").alias("ano_mes"), F.quarter("data").alias("trimestre"),
            F.dayofweek("data").alias("dia_semana"), F.element_at(nomes, F.dayofweek("data")).alias("nome_dia"),
            F.dayofweek("data").isin(1, 7).alias("fim_de_semana"),
        )
    )
    gravar(dim_tempo, caminho(cfg, "gold", "dim_tempo"))
    out["dim_tempo"] = dim_tempo.count()
    return out


# ---------------------------------------------------------------------------
# Fatos (incrementais por partição dt)
# ---------------------------------------------------------------------------
def fatos(spark: SparkSession, cfg: Config, datas: list[date]) -> dict[str, int]:
    out = {"fato_transacao": 0, "fato_posicao_contrato": 0}
    d_tx = [d for d in datas if d in set(particoes(cfg, "silver", "transacoes"))]
    if d_tx:
        tx = ler_particoes(spark, cfg, "silver", "transacoes", d_tx)
        fato = tx.select(
            "transacao_id", data_sk("data_hora").alias("data_sk"), sk("cliente", "cliente_id").alias("cliente_sk"),
            sk("conta", "conta_id").alias("conta_sk"), sk("canal", "tipo").alias("canal_sk"), "sentido",
            "valor", "parcelas", "categoria", "hora", "dt",
        )
        gravar(fato, caminho(cfg, "gold", "fato_transacao"), "dt")
        out["fato_transacao"] = fato.count()

    d_ctr = [d for d in datas if d in set(particoes(cfg, "silver", "contratos"))]
    if d_ctr:
        ctr = ler_particoes(spark, cfg, "silver", "contratos", d_ctr)
        fato = ctr.select(
            F.date_format(F.col("dt").cast("date"), "yyyyMMdd").cast("int").alias("data_sk"), "contrato_id",
            sk("cliente", "cliente_id").alias("cliente_sk"), sk("produto", "produto").alias("produto_sk"),
            "data_contratacao", "safra", "valor_contratado", "taxa_mensal", "prazo_meses", "saldo_devedor",
            "valor_em_atraso", "dias_atraso", "faixa_atraso", "estagio", "provisao_estimada", "status", "em_carteira", "dt",
        )
        gravar(fato, caminho(cfg, "gold", "fato_posicao_contrato"), "dt")
        out["fato_posicao_contrato"] = fato.count()
    return out


# ---------------------------------------------------------------------------
# Marts (agregações prontas para consumo; recalculados por completo — são pequenos)
# ---------------------------------------------------------------------------
def _fechamentos_mensais(datas_disponiveis: list[date]) -> list[date]:
    """Última data disponível de cada mês (fechamento; o mês corrente usa a data mais recente)."""
    por_mes: dict[str, date] = {}
    for d in datas_disponiveis:
        por_mes[d.strftime("%Y-%m")] = max(d, por_mes.get(d.strftime("%Y-%m"), d))
    return sorted(por_mes.values())


def marts(spark: SparkSession, cfg: Config, fim: date) -> dict[str, int]:
    out = {}
    dim_cliente = spark.read.parquet(caminho(cfg, "gold", "dim_cliente"))
    dim_produto = spark.read.parquet(caminho(cfg, "gold", "dim_produto"))
    inicio_janela = fim - timedelta(days=cfg.retencao_dias)

    # --- Carteira de crédito por mês, segmento e produto ---------------------------
    fechamentos = [d for d in _fechamentos_mensais(particoes(cfg, "gold", "fato_posicao_contrato")) if d <= fim]
    pos = (ler_particoes(spark, cfg, "gold", "fato_posicao_contrato", fechamentos)
           .join(F.broadcast(dim_cliente.select("cliente_sk", "segmento")), "cliente_sk")
           .join(F.broadcast(dim_produto.select("produto_sk", "produto")), "produto_sk")
           .withColumn("ano_mes", F.substring("dt", 1, 7)))
    cart = F.col("em_carteira")
    soma = lambda cond, col="saldo_devedor": F.sum(F.when(cond, F.col(col)).otherwise(0)).cast(T.DecimalType(16, 2))  # noqa: E731
    conta = lambda cond: F.sum(F.when(cond, 1).otherwise(0)).cast("int")  # noqa: E731
    novos = F.substring(F.col("data_contratacao").cast("string"), 1, 7) == F.col("ano_mes")
    carteira = pos.groupBy("ano_mes", "segmento", "produto").agg(
        F.max("dt").alias("data_posicao"),
        conta(cart).alias("contratos_carteira"),
        soma(cart).alias("saldo_carteira"),
        soma(cart & (F.col("estagio") == 1)).alias("saldo_estagio_1"),
        soma(cart & (F.col("estagio") == 2)).alias("saldo_estagio_2"),
        soma(cart & (F.col("estagio") == 3)).alias("saldo_estagio_3"),
        soma(cart & (F.col("dias_atraso") > 90)).alias("saldo_atraso_90"),
        conta(cart & (F.col("dias_atraso") > 90)).alias("contratos_atraso_90"),
        soma(cart & (F.col("dias_atraso") > 15) & (F.col("dias_atraso") <= 90)).alias("saldo_atraso_15_90"),
        soma(cart, "provisao_estimada").alias("provisao_estimada"),
        conta(novos).alias("novos_contratos"),
        soma(novos, "valor_contratado").alias("valor_concedido"),
        F.sum(F.when(novos, F.col("valor_contratado") * F.col("taxa_mensal")).otherwise(0)).cast(T.DecimalType(18, 4)).alias("valor_x_taxa_concedido"),
    )
    gravar(carteira, caminho(cfg, "gold", "mart_carteira_mensal"))
    out["mart_carteira_mensal"] = carteira.count()

    # --- Perfil de risco na posição mais recente ------------------------------------
    atual = ler_particoes(spark, cfg, "gold", "fato_posicao_contrato", [fechamentos[-1]]).where("em_carteira")
    perfil = (atual.join(F.broadcast(dim_cliente), "cliente_sk")
              .join(F.broadcast(dim_produto.select("produto_sk", "produto")), "produto_sk")
              .groupBy("segmento", "faixa_renda", "faixa_score", "produto")
              .agg(F.countDistinct("cliente_sk").cast("int").alias("clientes"), F.count("*").cast("int").alias("contratos"),
                   F.sum("saldo_devedor").cast(T.DecimalType(16, 2)).alias("saldo_carteira"),
                   soma(F.col("dias_atraso") > 90).alias("saldo_atraso_90"),
                   F.sum("provisao_estimada").cast(T.DecimalType(16, 2)).alias("provisao_estimada"))
              .withColumn("data_posicao", F.lit(fechamentos[-1].isoformat())))
    gravar(perfil, caminho(cfg, "gold", "mart_perfil_risco"))
    out["mart_perfil_risco"] = perfil.count()

    # --- Transações por dia, canal e segmento ---------------------------------------
    tx = ler_particoes(spark, cfg, "gold", "fato_transacao").where(F.col("dt") >= inicio_janela.isoformat())
    dim_canal = spark.read.parquet(caminho(cfg, "gold", "dim_canal"))
    diario = (tx.join(F.broadcast(dim_cliente.select("cliente_sk", "segmento")), "cliente_sk")
              .join(F.broadcast(dim_canal.select("canal_sk", "canal")), "canal_sk")
              .groupBy(F.col("dt").cast("date").alias("data"), "canal", "segmento")
              .agg(F.count("*").cast("int").alias("quantidade"),
                   F.sum("valor").cast(T.DecimalType(16, 2)).alias("valor_total"),
                   F.sum(F.when(F.col("sentido") == "Entrada", F.col("valor")).otherwise(0)).cast(T.DecimalType(16, 2)).alias("valor_entrada"),
                   F.sum(F.when(F.col("sentido") == "Saída", F.col("valor")).otherwise(0)).cast(T.DecimalType(16, 2)).alias("valor_saida"),
                   F.countDistinct("cliente_sk").cast("int").alias("clientes_ativos")))
    gravar(diario, caminho(cfg, "gold", "mart_transacoes_diarias"))
    out["mart_transacoes_diarias"] = diario.count()

    # --- Base de clientes e depósitos (fechamento mensal das contas) ----------------
    fech_contas = [d for d in _fechamentos_mensais(particoes(cfg, "silver", "contas")) if d <= fim]
    contas = (ler_particoes(spark, cfg, "silver", "contas", fech_contas)
              .join(spark.read.parquet(caminho(cfg, "silver", "clientes")).select("cliente_id", "segmento"), "cliente_id")
              .withColumn("ano_mes", F.substring("dt", 1, 7)))
    ativa = F.col("status") == "Ativa"
    base = contas.groupBy("ano_mes", "segmento").agg(
        F.max("dt").alias("data_posicao"),
        F.countDistinct(F.when(ativa, F.col("cliente_id"))).cast("int").alias("clientes_ativos"),
        F.countDistinct(F.when(ativa & (F.substring(F.col("data_abertura").cast("string"), 1, 7) == F.col("ano_mes")), F.col("cliente_id"))).cast("int").alias("novos_clientes"),
        F.sum(F.when(ativa & (F.col("tipo") == "Corrente"), F.col("saldo")).otherwise(0)).cast(T.DecimalType(16, 2)).alias("saldo_conta_corrente"),
        F.sum(F.when(ativa & (F.col("tipo") == "Poupança"), F.col("saldo")).otherwise(0)).cast(T.DecimalType(16, 2)).alias("saldo_poupanca"),
    )
    gravar(base, caminho(cfg, "gold", "mart_clientes_mensal"))
    out["mart_clientes_mensal"] = base.count()

    # --- Indicadores macroeconômicos mensais (API BCB) ------------------------------
    macro = spark.read.parquet(caminho(cfg, "silver", "indicadores_macro")).withColumn("ano_mes", F.date_format("data", "yyyy-MM"))
    w_ultimo = Window.partitionBy("serie", "ano_mes").orderBy(F.col("data").desc())
    selic = (macro.where("serie = 'selic_meta'").withColumn("n", F.row_number().over(w_ultimo)).where("n = 1")
             .select("ano_mes", F.col("valor").alias("selic_meta")))
    dolar = macro.where("serie = 'dolar_ptax'").groupBy("ano_mes").agg(F.round(F.avg("valor"), 4).alias("dolar_medio"))
    ipca = macro.where("serie = 'ipca_mensal'").groupBy("ano_mes").agg(F.first("valor").alias("ipca_mensal"))
    w12 = Window.partitionBy(F.lit("ipca")).orderBy("ano_mes").rowsBetween(-11, 0)
    ipca = ipca.withColumn(
        "ipca_12m",
        F.when(F.count("ipca_mensal").over(w12) == 12,
               F.round((F.exp(F.sum(F.log1p(F.col("ipca_mensal") / 100)).over(w12)) - 1) * 100, 2)),
    )
    mensal = (selic.join(dolar, "ano_mes", "full").join(ipca, "ano_mes", "full")
              .where(F.col("ano_mes") >= inicio_janela.strftime("%Y-%m")))
    gravar(mensal, caminho(cfg, "gold", "mart_macro_mensal"))
    out["mart_macro_mensal"] = mensal.count()
    return out


def executar(spark: SparkSession, cfg: Config, datas: list[date]) -> dict[str, int]:
    fim = max(datas)
    res = dimensoes(spark, cfg, fim)
    res.update(fatos(spark, cfg, datas))
    res.update(marts(spark, cfg, fim))
    return res


if __name__ == "__main__":
    cfg, datas = argumentos_glue("Silver → Gold")
    print(executar(spark_session("silver_para_gold"), cfg, datas))
