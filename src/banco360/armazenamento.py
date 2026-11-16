"""
Sincronização do Data Lake local com um armazenamento de objetos compatível com S3.

O mesmo código funciona com:
* Supabase Storage (protocolo S3) — usado na demonstração, sem custo;
* Amazon S3 — basta omitir LAKE_S3_ENDPOINT e informar as credenciais AWS.

Os jobs Spark processam no disco local (rápido e barato) e o lake persistente
fica no bucket: no começo da execução baixamos o histórico necessário (pull) e
no fim enviamos o que mudou (push). Em produção na AWS os jobs leriam/gravariam
direto em s3:// — ver docs/aws.md.

Regras de remoção no push (para nunca apagar o que só não foi baixado):
* numa pasta que o pipeline regravou, arquivos remotos que não existem mais
  localmente são removidos (o Spark gera nomes novos a cada escrita);
* partições removidas pela retenção são registradas em _manutencao/removidos.txt.
"""
from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import boto3
from botocore.config import Config as BotoConfig

from banco360.config import Config

log = logging.getLogger(__name__)
REMOVIDOS = Path("_manutencao", "removidos.txt")
PREFIXOS_PULL = ("silver/", "gold/", "_manifestos/", "_metricas/", "bronze/bcb/")


def _cliente(cfg: Config):
    return boto3.client(
        "s3",
        endpoint_url=cfg.s3_endpoint or None,
        region_name=cfg.s3_region,
        aws_access_key_id=cfg.s3_access_key or None,
        aws_secret_access_key=cfg.s3_secret_key or None,
        config=BotoConfig(s3={"addressing_style": "path"}, retries={"max_attempts": 5, "mode": "standard"},
                          max_pool_connections=32),
    )


def _remotos(s3, bucket: str, prefixo: str = "") -> dict[str, int]:
    objetos: dict[str, int] = {}
    for pagina in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefixo):
        for o in pagina.get("Contents", []):
            objetos[o["Key"]] = o["Size"]
    return objetos


def _locais(raiz: Path) -> dict[str, Path]:
    return {p.relative_to(raiz).as_posix(): p for p in raiz.rglob("*")
            if p.is_file() and not p.name.endswith(".crc") and p.name != "_SUCCESS" and not p.relative_to(raiz).as_posix().startswith("_manutencao")}


def registrar_remocao(cfg: Config, prefixo: str) -> None:
    arq = Path(cfg.lake_dir, REMOVIDOS)
    arq.parent.mkdir(parents=True, exist_ok=True)
    with open(arq, "a", encoding="utf-8") as f:
        f.write(prefixo.rstrip("/") + "/\n")


def enviar(cfg: Config) -> dict[str, int]:
    if not cfg.s3_habilitado:
        log.info("Armazenamento S3 não configurado; lake mantido apenas localmente.")
        return {"enviados": 0, "removidos": 0}
    s3, raiz = _cliente(cfg), Path(cfg.lake_dir)
    remotos, locais = _remotos(s3, cfg.s3_bucket), _locais(raiz)
    pendentes = [(k, p) for k, p in locais.items() if remotos.get(k) != p.stat().st_size]
    with ThreadPoolExecutor(16) as pool:
        list(pool.map(lambda kp: s3.upload_file(str(kp[1]), cfg.s3_bucket, kp[0]), pendentes))

    arq_rem = raiz / REMOVIDOS
    removidos = [x.strip() for x in arq_rem.read_text(encoding="utf-8").splitlines() if x.strip()] if arq_rem.exists() else []
    pastas_regravadas = {k.rsplit("/", 1)[0] for k in locais}
    obsoletos = [k for k in remotos if k not in locais and
                 (k.rsplit("/", 1)[0] in pastas_regravadas or any(k.startswith(p) for p in removidos))]
    for i in range(0, len(obsoletos), 1000):
        s3.delete_objects(Bucket=cfg.s3_bucket, Delete={"Objects": [{"Key": k} for k in obsoletos[i:i + 1000]]})
    arq_rem.unlink(missing_ok=True)
    return {"enviados": len(pendentes), "removidos": len(obsoletos)}


def baixar(cfg: Config, prefixos: tuple[str, ...] = PREFIXOS_PULL) -> dict[str, int]:
    """Pull: traz do bucket o histórico necessário para recalcular os marts."""
    if not cfg.s3_habilitado:
        return {"arquivos_baixados": 0}
    s3, raiz = _cliente(cfg), Path(cfg.lake_dir)
    tarefas = []
    for prefixo in prefixos:
        for chave, tamanho in _remotos(s3, cfg.s3_bucket, prefixo).items():
            destino = raiz / chave
            if not (destino.exists() and destino.stat().st_size == tamanho):
                destino.parent.mkdir(parents=True, exist_ok=True)
                tarefas.append((chave, destino))
    with ThreadPoolExecutor(16) as pool:
        list(pool.map(lambda t: s3.download_file(cfg.s3_bucket, t[0], str(t[1])), tarefas))
    return {"arquivos_baixados": len(tarefas)}
