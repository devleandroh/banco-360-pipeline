"""Configuração centralizada, lida de variáveis de ambiente (12-factor)."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


def _env(nome: str, padrao: str = "") -> str:
    return os.environ.get(nome, padrao).strip()


@dataclass(frozen=True)
class Config:
    # Data Lake local (espelhado no armazenamento S3 quando configurado)
    lake_dir: str = field(default_factory=lambda: _env("LAKE_DIR", str(Path.cwd() / "lake")))
    landing_dir: str = field(default_factory=lambda: _env("LANDING_DIR", str(Path.cwd() / "landing")))

    # Banco de origem (core bancário simulado) e Data Warehouse
    source_db_url: str = field(default_factory=lambda: _env("SOURCE_DB_URL", "postgresql://core:core@localhost:5433/core_banking"))
    warehouse_db_url: str = field(default_factory=lambda: _env("WAREHOUSE_DB_URL", "postgresql://postgres:postgres@localhost:5434/postgres"))

    # Armazenamento de objetos compatível com S3 (Supabase Storage ou AWS S3)
    s3_endpoint: str = field(default_factory=lambda: _env("LAKE_S3_ENDPOINT"))
    s3_region: str = field(default_factory=lambda: _env("LAKE_S3_REGION", "us-east-1"))
    s3_bucket: str = field(default_factory=lambda: _env("LAKE_S3_BUCKET", "banco-360-lake"))
    s3_access_key: str = field(default_factory=lambda: _env("LAKE_S3_ACCESS_KEY_ID"))
    s3_secret_key: str = field(default_factory=lambda: _env("LAKE_S3_SECRET_ACCESS_KEY"))

    # Segredo usado no hash de dados pessoais (LGPD). Nunca versionar o valor real.
    pii_salt: str = field(default_factory=lambda: _env("PII_SALT", "salt-de-desenvolvimento"))

    # API do Banco Central (SGS)
    bcb_base_url: str = field(default_factory=lambda: _env("BCB_BASE_URL", "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{codigo}/dados"))

    # Quem disparou a execução: local, airflow, github-actions
    origem_execucao: str = field(default_factory=lambda: _env("RUN_ORIGIN", "local"))

    # Retenção (dias) das partições diárias no lake e no warehouse
    retencao_dias: int = field(default_factory=lambda: int(_env("RETENCAO_DIAS", "400")))

    @property
    def s3_habilitado(self) -> bool:
        return bool(self.s3_endpoint or self.s3_access_key)

    def camada(self, nome: str) -> Path:
        return Path(self.lake_dir, nome)
