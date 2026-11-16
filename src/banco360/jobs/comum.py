"""
Utilitários compartilhados pelos jobs PySpark.

Os jobs seguem o formato de um AWS Glue Job: cada módulo tem uma função
principal que recebe (spark, cfg, datas) e um bloco `__main__` que lê
argumentos `--data_inicio/--data_fim`, como o `getResolvedOptions` do Glue.
Assim o mesmo código roda via orquestrador (em processo) ou via `spark-submit`.
"""
from __future__ import annotations

import argparse
import json
import os
from datetime import date, timedelta
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from banco360.config import Config

_spark: SparkSession | None = None


def spark_session(app: str = "banco360") -> SparkSession:
    global _spark
    if _spark is None:
        _spark = (
            SparkSession.builder.master(os.environ.get("SPARK_MASTER", "local[*]"))
            .appName(app)
            .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8"))
            # sobrescreve só as partições (dt=...) que o job reprocessou
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .config("spark.sql.session.timeZone", "America/Sao_Paulo")
            .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "2g"))
            .config("spark.ui.enabled", "false")
            .config("spark.ui.showConsoleProgress", "false")
            .config("spark.sql.parquet.compression.codec", "snappy")
            .getOrCreate()
        )
        _spark.sparkContext.setLogLevel("WARN")
    return _spark


def caminho(cfg: Config, camada: str, *partes: str) -> str:
    return str(Path(cfg.lake_dir, camada, *partes))


def particoes(cfg: Config, camada: str, *partes: str) -> list[date]:
    """Datas disponíveis (dt=YYYY-MM-DD) num dataset particionado."""
    base = Path(cfg.lake_dir, camada, *partes)
    if not base.exists():
        return []
    return sorted(date.fromisoformat(p.name[3:]) for p in base.iterdir() if p.is_dir() and p.name.startswith("dt="))


def ultima_particao(cfg: Config, camada: str, *partes: str, ate: date | None = None) -> date:
    ds = [d for d in particoes(cfg, camada, *partes) if ate is None or d <= ate]
    if not ds:
        raise FileNotFoundError(f"Nenhuma partição em {camada}/{'/'.join(partes)} até {ate}")
    return ds[-1]


def ler_particoes(spark: SparkSession, cfg: Config, camada: str, dataset: str, datas: list[date] | None = None) -> DataFrame:
    """Lê um dataset particionado por dt; com `datas`, só as partições pedidas (partition pruning)."""
    base = caminho(cfg, camada, dataset)
    df = spark.read.option("basePath", base).parquet(base)
    if datas:
        df = df.where(F.col("dt").isin([d.isoformat() for d in datas]))
    return df.withColumn("dt", F.col("dt").cast("string"))


def gravar(df: DataFrame, destino: str, particionar_por: str | None = None) -> None:
    w = df.write.mode("overwrite")
    if particionar_por:
        w = w.partitionBy(particionar_por)
    w.parquet(destino)


def salvar_metricas(cfg: Config, d: date, nome: str, metricas: dict) -> None:
    pasta = Path(cfg.lake_dir, "_metricas", f"dt={d.isoformat()}")
    pasta.mkdir(parents=True, exist_ok=True)
    (pasta / f"{nome}.json").write_text(json.dumps(metricas, indent=2, ensure_ascii=False), encoding="utf-8")


def ler_metricas(cfg: Config, d: date, nome: str) -> dict:
    arq = Path(cfg.lake_dir, "_metricas", f"dt={d.isoformat()}", f"{nome}.json")
    return json.loads(arq.read_text(encoding="utf-8")) if arq.exists() else {}


def intervalo(inicio: date, fim: date) -> list[date]:
    return [inicio + timedelta(days=i) for i in range((fim - inicio).days + 1)]


def argumentos_glue(descricao: str) -> tuple[Config, list[date]]:
    p = argparse.ArgumentParser(description=descricao)
    p.add_argument("--data_inicio", required=True, type=date.fromisoformat)
    p.add_argument("--data_fim", type=date.fromisoformat)
    a = p.parse_args()
    return Config(), intervalo(a.data_inicio, a.data_fim or a.data_inicio)
