"""
Ingestão da API REST pública do Banco Central (SGS — Sistema Gerenciador de Séries Temporais).

Séries usadas:
  432 — Meta da taxa SELIC (% a.a., diária)
  433 — IPCA, variação mensal (%)
  1   — Dólar comercial PTAX, venda (R$, diária)

Equivalente AWS: uma função Lambda (ou Glue Python Shell) agendada que chama a
API e grava o JSON bruto em s3://<lake>/bronze/bcb/.

A API é externa e pode falhar: há retentativas com espera exponencial e, se
mesmo assim falhar, a execução segue usando a última coleta válida (o check de
"atualidade" da qualidade de dados acusa o atraso).
"""
from __future__ import annotations

import json
import logging
import time
from datetime import date, timedelta
from pathlib import Path

import requests

from banco360.config import Config
from banco360.ingestao import registrar_manifesto

log = logging.getLogger(__name__)

SERIES = {"selic_meta": 432, "ipca_mensal": 433, "dolar_ptax": 1}
JANELA_DIAS = 800


def _buscar(cfg: Config, codigo: int, inicio: date, fim: date, tentativas: int = 4) -> list[dict]:
    url = cfg.bcb_base_url.format(codigo=codigo)
    params = {"formato": "json", "dataInicial": inicio.strftime("%d/%m/%Y"), "dataFinal": fim.strftime("%d/%m/%Y")}
    for n in range(1, tentativas + 1):
        try:
            resp = requests.get(url, params=params, timeout=30, headers={"Accept": "application/json"})
            resp.raise_for_status()
            dados = resp.json()
            if not isinstance(dados, list):
                raise ValueError(f"resposta inesperada: {str(dados)[:200]}")
            return dados
        except (requests.RequestException, ValueError) as e:
            if n == tentativas:
                raise
            espera = 2 ** n
            log.warning("API BCB série %s falhou (%s); nova tentativa em %ss", codigo, e, espera)
            time.sleep(espera)
    return []


def ingerir(cfg: Config, d: date) -> dict[str, int]:
    resultado = {}
    for nome, codigo in SERIES.items():
        try:
            dados = _buscar(cfg, codigo, d - timedelta(days=JANELA_DIAS), d)
        except Exception as e:  # noqa: BLE001 — falha externa não derruba o pipeline
            log.error("Série %s indisponível; mantendo a última coleta. Erro: %s", nome, e)
            resultado[nome] = -1
            continue
        pasta = cfg.camada("bronze") / "bcb" / f"serie={nome}" / f"dt={d.isoformat()}"
        pasta.mkdir(parents=True, exist_ok=True)
        destino = Path(pasta, "dados.json")
        destino.write_text(json.dumps({"codigo_sgs": codigo, "serie": nome, "data_referencia": d.isoformat(), "dados": dados},
                                      ensure_ascii=False), encoding="utf-8")
        registrar_manifesto(cfg, d, f"bcb.{nome}", destino, len(dados))
        resultado[nome] = len(dados)
    return resultado
