"""
Ingestão de arquivos da zona de aterrissagem (landing) para a Bronze.

Equivalente AWS: arquivos chegam em s3://<landing>/ (SFTP Transfer Family,
parceiros, outros sistemas) e um evento do S3 ou o orquestrador copia para a
Bronze. O conteúdo é preservado byte a byte; o hash fica no manifesto.
"""
from __future__ import annotations

import shutil
from datetime import date
from pathlib import Path

from banco360.config import Config
from banco360.ingestao import registrar_manifesto

FONTES = {
    "cartoes": ("cartoes", "cartoes_{ymd}.csv", True),       # (pasta, padrão, tem cabeçalho)
    "pagamentos": ("pagamentos", "pagamentos_{ymd}.jsonl", False),
}


class ArquivoAusente(RuntimeError):
    pass


def ingerir(cfg: Config, d: date) -> dict[str, int]:
    ymd = d.strftime("%Y%m%d")
    resultado = {}
    for fonte, (pasta, padrao, cabecalho) in FONTES.items():
        origem = Path(cfg.landing_dir, pasta, padrao.format(ymd=ymd))
        if not origem.exists():
            raise ArquivoAusente(f"Arquivo esperado não chegou: {origem}")
        destino_dir = cfg.camada("bronze") / fonte / f"dt={d.isoformat()}"
        destino_dir.mkdir(parents=True, exist_ok=True)
        destino = destino_dir / origem.name
        shutil.copy2(origem, destino)
        with open(destino, encoding="utf-8") as f:
            linhas = sum(1 for _ in f) - (1 if cabecalho else 0)
        registrar_manifesto(cfg, d, fonte, destino, linhas)
        resultado[fonte] = linhas
    return resultado


def limpar_landing(cfg: Config, d: date) -> None:
    """Remove da landing os arquivos já ingeridos (eles estão preservados na Bronze)."""
    ymd = d.strftime("%Y%m%d")
    for pasta, padrao, _ in FONTES.values():
        Path(cfg.landing_dir, pasta, padrao.format(ymd=ymd)).unlink(missing_ok=True)
