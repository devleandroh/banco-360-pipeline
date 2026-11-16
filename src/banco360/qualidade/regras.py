"""
Motor de qualidade de dados.

Lê os contratos de config/qualidade.yml, avalia cada regra com Spark e devolve
uma lista de resultados (um por regra/coluna). Além das regras declarativas há
regras de reconciliação (os números batem entre camadas?) e de atualidade
(os dados externos estão recentes?).

Equivalente AWS: AWS Glue Data Quality (regras DQDL) ou Deequ, com resultados
publicados no CloudWatch.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from banco360.config import Config
from banco360.ingestao import ler_manifesto
from banco360.jobs.comum import caminho, ler_metricas, particoes

CONTRATOS = Path(__file__).resolve().parents[3] / "config" / "qualidade.yml"


@dataclass
class Resultado:
    camada: str
    dataset: str
    regra: str
    coluna: str | None
    severidade: str
    status: str  # ok | falha
    observado: float | None
    esperado: str
    detalhe: str = ""

    def dict(self) -> dict:
        return asdict(self)


class FalhaQualidade(RuntimeError):
    def __init__(self, falhas: list[Resultado]):
        super().__init__("; ".join(f"{f.camada}.{f.dataset}: {f.regra}({f.coluna or ''}) {f.detalhe}" for f in falhas))
        self.falhas = falhas


def _carregar(spark: SparkSession, cfg: Config, camada: str, dataset: str, particionado: bool, d: date) -> DataFrame | None:
    if particionado:
        if d not in set(particoes(cfg, camada, dataset)):
            return None
        return spark.read.parquet(caminho(cfg, camada, dataset, f"dt={d.isoformat()}"))
    p = Path(cfg.lake_dir, camada, dataset)
    return spark.read.parquet(str(p)) if p.exists() else None


def _regra(spark: SparkSession, cfg: Config, camada: str, dataset: str, df: DataFrame, r: dict, total: int) -> list[Resultado]:
    tipo, sev = r["tipo"], r.get("severidade", "erro")
    res: list[Resultado] = []

    def add(coluna, falhas, esperado, detalhe=""):
        res.append(Resultado(camada, dataset, tipo, coluna, sev, "ok" if falhas == 0 else "falha", float(falhas), esperado,
                             detalhe or (f"{falhas} de {total} linhas" if falhas else "")))

    if tipo == "nao_nulo":
        contagens = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in r["colunas"]]).first()
        for c in r["colunas"]:
            add(c, contagens[c] or 0, "0 nulos")
    elif tipo == "unico":
        cols = r["colunas"]
        dup = df.groupBy(*cols).count().where("count > 1").count()
        add(", ".join(cols), dup, "0 chaves duplicadas")
    elif tipo == "valores_aceitos":
        fora = df.where(~F.col(r["coluna"]).isin(r["valores"]) | F.col(r["coluna"]).isNull()).count()
        add(r["coluna"], fora, f"em {r['valores']}")
    elif tipo == "intervalo":
        c = F.col(r["coluna"])
        cond = F.lit(False)
        if "min" in r:
            cond = cond | (c < r["min"])
        if "max" in r:
            cond = cond | (c > r["max"])
        add(r["coluna"], df.where(cond).count(), f"entre {r.get('min', '-∞')} e {r.get('max', '+∞')}")
    elif tipo == "regex":
        add(r["coluna"], df.where(~F.col(r["coluna"]).rlike(r["padrao"])).count(), r["padrao"])
    elif tipo == "colunas_proibidas":
        presentes = [c for c in r["colunas"] if c in df.columns]
        add(", ".join(r["colunas"]), len(presentes), "ausentes", f"presentes: {presentes}" if presentes else "")
    elif tipo == "volume_minimo":
        res.append(Resultado(camada, dataset, tipo, None, sev, "ok" if total >= r["min"] else "falha", float(total), f">= {r['min']}"))
    elif tipo == "integridade_referencial":
        ref_camada, ref_ds, ref_col = r["referencia"].split(".")
        ref = spark.read.parquet(caminho(cfg, ref_camada, ref_ds))
        if "dt" in ref.columns:
            ref = ref.where(F.col("dt") == F.lit(max(particoes(cfg, ref_camada, ref_ds)).isoformat()))
        orfaos = df.join(ref.select(F.col(ref_col).alias("_ref")).distinct(), F.col(r["coluna"]) == F.col("_ref"), "left_anti").count()
        add(r["coluna"], orfaos, f"existe em {r['referencia']}")
    return res


def _reconciliacao(spark: SparkSession, cfg: Config, camada: str, d: date) -> list[Resultado]:
    res: list[Resultado] = []
    manifesto = ler_manifesto(cfg, d)

    def igual(dataset, regra, a, b, detalhe, sev="erro"):
        res.append(Resultado(camada, dataset, regra, None, sev, "ok" if a == b else "falha", float(a), str(b), detalhe if a != b else ""))

    if camada == "silver":
        m = ler_metricas(cfg, d, "silver_transacoes")
        if m:
            linhas_arquivos = sum(manifesto.get(f, {}).get("linhas", 0) for f in ("cartoes", "pagamentos"))
            igual("transacoes", "reconciliacao_arquivo_bronze", m["bronze_linhas"], linhas_arquivos,
                  "linhas lidas pelo Spark ≠ linhas dos arquivos brutos")
            soma = m["silver_linhas"] + m["negadas"] + m["duplicadas"] + m["rejeitadas"]
            igual("transacoes", "reconciliacao_bronze_silver", soma, m["bronze_linhas"],
                  f"silver {m['silver_linhas']} + negadas {m['negadas']} + duplicadas {m['duplicadas']} + rejeitadas {m['rejeitadas']}")
            taxa = m["rejeitadas"] / max(1, m["bronze_linhas"])
            res.append(Resultado(camada, "transacoes", "taxa_rejeicao", None, "alerta", "ok" if taxa <= 0.01 else "falha",
                                 round(taxa, 5), "<= 1%", f"{m['rejeitadas']} em quarentena"))
        if "core_bancario.contratos" in manifesto and d in set(particoes(cfg, "silver", "contratos")):
            n = spark.read.parquet(caminho(cfg, "silver", "contratos", f"dt={d.isoformat()}")).count()
            igual("contratos", "reconciliacao_bronze_silver", n, manifesto["core_bancario.contratos"]["linhas"], "contratos perdidos ou duplicados")
        # Atualidade dos dados externos (API do Banco Central)
        macro_path = Path(cfg.lake_dir, "silver", "indicadores_macro")
        if macro_path.exists():
            ultimas = {r["serie"]: r["ultima"] for r in spark.read.parquet(str(macro_path)).groupBy("serie")
                       .agg(F.max("data").alias("ultima")).collect()}
            for serie, limite in (("selic_meta", 10), ("dolar_ptax", 7), ("ipca_mensal", 75)):
                ult = ultimas.get(serie)
                atraso = (d - ult).days if ult else 9999
                res.append(Resultado(camada, "indicadores_macro", "atualidade", serie, "alerta", "ok" if atraso <= limite else "falha",
                                     float(atraso), f"<= {limite} dias", f"último dado: {ult}" if atraso > limite else ""))
    else:
        if d in set(particoes(cfg, "gold", "fato_transacao")):
            g = spark.read.parquet(caminho(cfg, "gold", "fato_transacao", f"dt={d.isoformat()}")).count()
            s = spark.read.parquet(caminho(cfg, "silver", "transacoes", f"dt={d.isoformat()}")).count()
            igual("fato_transacao", "reconciliacao_silver_gold", g, s, "linhas da fato ≠ linhas da silver")
        mart_path = Path(cfg.lake_dir, "gold", "mart_carteira_mensal")
        if mart_path.exists() and d in set(particoes(cfg, "gold", "fato_posicao_contrato")):
            mart = spark.read.parquet(str(mart_path)).where(F.col("data_posicao") == d.isoformat()) \
                .agg(F.sum("saldo_carteira")).first()[0] or 0
            fato = spark.read.parquet(caminho(cfg, "gold", "fato_posicao_contrato", f"dt={d.isoformat()}")) \
                .where("em_carteira").agg(F.sum("saldo_devedor")).first()[0] or 0
            res.append(Resultado("gold", "mart_carteira_mensal", "reconciliacao_fato_mart", None, "erro",
                                 "ok" if abs(float(mart) - float(fato)) < 1 else "falha", float(mart), f"{float(fato):.2f}",
                                 "" if abs(float(mart) - float(fato)) < 1 else "saldo do mart ≠ soma da fato"))
    return res


def avaliar(spark: SparkSession, cfg: Config, camada: str, d: date, interromper: bool = True) -> list[Resultado]:
    contratos = yaml.safe_load(CONTRATOS.read_text(encoding="utf-8"))[camada]
    resultados: list[Resultado] = []
    for dataset, spec in contratos.items():
        df = _carregar(spark, cfg, camada, dataset, spec.get("particionado", False), d)
        if df is None:
            resultados.append(Resultado(camada, dataset, "disponibilidade", None, "erro", "falha", 0, "dataset presente",
                                        f"partição dt={d} não encontrada"))
            continue
        df = df.cache()
        total = df.count()
        for r in spec["regras"]:
            resultados.extend(_regra(spark, cfg, camada, dataset, df, r, total))
        df.unpersist()
    resultados.extend(_reconciliacao(spark, cfg, camada, d))
    falhas = [r for r in resultados if r.status == "falha" and r.severidade == "erro"]
    if falhas and interromper:
        raise FalhaQualidade(falhas)
    return resultados
