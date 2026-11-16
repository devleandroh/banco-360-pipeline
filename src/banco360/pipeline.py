"""
Orquestração das etapas do pipeline, com registro de execução (observabilidade).

Cada etapa é cronometrada e registrada em banco.pipeline_execucoes à medida que
termina — se algo quebrar no meio, o painel mostra exatamente onde parou.
O Airflow (dags/banco360_dag.py) e o GitHub Actions chamam estas mesmas etapas.
"""
from __future__ import annotations

import logging
import shutil
import time
import traceback
import uuid
from collections.abc import Callable
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from banco360 import armazenamento
from banco360.config import Config
from banco360.ingestao import arquivos, bcb, core_bancario
from banco360.jobs.comum import intervalo, particoes
from banco360.simulador import origem
from banco360.warehouse import carga

log = logging.getLogger("banco360")

ETAPAS = [
    "sincronizar_entrada", "simular_origem", "ingestao_core", "ingestao_arquivos", "ingestao_bcb",
    "bronze_para_silver", "qualidade_silver", "silver_para_gold", "qualidade_gold",
    "carga_warehouse", "manutencao", "sincronizar_saida",
]


class Execucao:
    def __init__(self, cfg: Config, d: date, modo: str, execucao_id: str | None = None, registrar: bool = True,
                 parcial: bool = False):
        self.cfg, self.d, self.modo, self.parcial = cfg, d, modo, parcial
        self.id = _como_uuid(execucao_id) if execucao_id else str(uuid.uuid4())
        self.inicio = datetime.now(timezone.utc)
        self.etapas: list[dict] = []
        self.volumes: dict[str, dict] = {}
        self.qualidade: list[dict] = []
        self.registrar = registrar
        if registrar:
            try:
                carga.aplicar_ddl(cfg)
                carga.registrar_inicio(cfg, self.id, d, modo, self.inicio)
            except Exception as e:  # noqa: BLE001
                log.warning("Warehouse indisponível para registrar a execução (%s); seguindo sem registro.", e)
                self.registrar = False

    def etapa(self, nome: str, fn: Callable[[], dict | None]) -> dict | None:
        log.info("▶ %s", nome)
        t0 = time.perf_counter()
        inicio = datetime.now(timezone.utc).isoformat(timespec="seconds")
        try:
            resultado = fn() or {}
        except Exception:
            self.etapas.append({"etapa": nome, "status": "falha", "inicio": inicio, "duracao_s": round(time.perf_counter() - t0, 1)})
            raise
        dur = round(time.perf_counter() - t0, 1)
        linhas = {k: v for k, v in resultado.items() if isinstance(v, int)}
        self.etapas.append({"etapa": nome, "status": "sucesso", "inicio": inicio, "duracao_s": dur, "linhas": linhas})
        if linhas:
            self.volumes[nome] = linhas
        log.info("✔ %s (%.1fs) %s", nome, dur, linhas or "")
        return resultado

    def finalizar(self, erro: BaseException | None = None) -> None:
        status = "falha" if erro else "sucesso"
        msg = None
        if erro:
            msg = f"{type(erro).__name__}: {erro}"[:2000]
            log.error("Execução %s falhou: %s\n%s", self.id, msg, traceback.format_exc())
        if self.registrar:
            try:
                carga.registrar_fim(self.cfg, self.id, status, self.etapas, self.volumes, msg, self.qualidade, self.parcial)
            except Exception as e:  # noqa: BLE001
                log.error("Não foi possível registrar o fim da execução: %s", e)
        log.info("Execução %s: %s em %.1fs", self.id, status, (datetime.now(timezone.utc) - self.inicio).total_seconds())


def _como_uuid(valor: str) -> str:
    """Aceita um UUID ou qualquer texto estável (ex.: run_id do Airflow) e devolve um UUID."""
    try:
        return str(uuid.UUID(valor))
    except ValueError:
        return str(uuid.uuid5(uuid.NAMESPACE_URL, f"banco360/{valor}"))


def datas_de_snapshot(inicio: date, fim: date) -> list[date]:
    """Fechamentos mensais + últimos 7 dias: o histórico de posições que o backfill reconstrói."""
    fechamentos = [d for d in intervalo(inicio, fim) if (d + timedelta(days=1)).day == 1]
    recentes = [fim - timedelta(days=i) for i in range(7) if fim - timedelta(days=i) >= inicio]
    return sorted(set(fechamentos + recentes))


def _qualidade(exe: Execucao, spark, camada: str) -> dict:
    from banco360.qualidade.regras import FalhaQualidade, avaliar

    resultados = avaliar(spark, exe.cfg, camada, exe.d, interromper=False)
    exe.qualidade.extend({**r.dict(), "data_referencia": exe.d.isoformat()} for r in resultados)
    falhas = [r for r in resultados if r.status == "falha" and r.severidade == "erro"]
    alertas = sum(1 for r in resultados if r.status == "falha" and r.severidade == "alerta")
    log.info("Qualidade %s: %d regras, %d erros, %d alertas", camada, len(resultados), len(falhas), alertas)
    if falhas:
        raise FalhaQualidade(falhas)
    return {"regras": len(resultados), "alertas": alertas}


def _manutencao(cfg: Config, fim: date) -> dict:
    """Retenção no lake: partições diárias antigas e snapshots intermediários."""
    removidas = 0

    def remover(*partes: str) -> None:
        nonlocal removidas
        pasta = Path(cfg.lake_dir, *partes)
        shutil.rmtree(pasta)
        armazenamento.registrar_remocao(cfg, pasta.relative_to(cfg.lake_dir).as_posix())
        removidas += 1

    limite = fim - timedelta(days=cfg.retencao_dias)
    manter_snap = set(datas_de_snapshot(limite, fim))
    alvos = [("bronze", "cartoes"), ("bronze", "pagamentos"), ("silver", "transacoes"), ("silver", "_quarentena/transacoes"),
             ("gold", "fato_transacao"), ("_manifestos",), ("_metricas",)]
    for partes in alvos:
        for d in particoes(cfg, *partes):
            if d < limite:
                remover(*partes, f"dt={d.isoformat()}")
    for partes in [("bronze", "core_bancario", t) for t in ("clientes", "contas", "contratos")] + \
                  [("silver", "contas"), ("silver", "contratos"), ("gold", "fato_posicao_contrato")]:
        for d in particoes(cfg, *partes):
            if d not in manter_snap and d < fim - timedelta(days=7) and (d + timedelta(days=1)).day != 1:
                remover(*partes, f"dt={d.isoformat()}")
    for serie in ("selic_meta", "ipca_mensal", "dolar_ptax"):
        for d in particoes(cfg, "bronze", "bcb", f"serie={serie}")[:-7]:
            remover("bronze", "bcb", f"serie={serie}", f"dt={d.isoformat()}")
    return {"particoes_removidas": removidas}


def executar(cfg: Config, inicio: date, fim: date, simular: bool, etapas: list[str] | None = None,
             execucao_id: str | None = None, parcial: bool = False) -> str:
    modo = "diario" if inicio == fim else "backfill"
    datas = intervalo(inicio, fim)
    snapshots = datas if modo == "diario" else datas_de_snapshot(inicio, fim)
    pedidas = set(etapas or ETAPAS)
    if not simular:
        pedidas.discard("simular_origem")
    exe = Execucao(cfg, fim, modo, execucao_id, parcial=parcial)
    spark = None

    def get_spark():
        nonlocal spark
        if spark is None:
            from banco360.jobs.comum import spark_session
            spark = spark_session("banco360")
        return spark

    def simular() -> dict:
        tot = {"clientes": 0, "contas": 0, "contratos": 0, "cartoes": 0, "pagamentos": 0}
        for d in datas:
            for k, v in origem.gerar_arquivos_landing(cfg, d).items():
                tot[k] += v
        for k, v in origem.carregar_core_bancario(cfg, fim).items():
            tot[k] = v
        return tot

    def ingerir_core() -> dict:
        out: dict[str, int] = {}
        for d in snapshots:
            if modo == "backfill" and simular:
                origem.carregar_core_bancario(cfg, d)  # reconstrói o estado da origem em cada fechamento
            for k, v in core_bancario.extrair(cfg, d).items():
                out[k] = v
        return out

    def ingerir_arquivos() -> dict:
        tot = {"cartoes": 0, "pagamentos": 0}
        for d in datas:
            for k, v in arquivos.ingerir(cfg, d).items():
                tot[k] += v
            arquivos.limpar_landing(cfg, d)
        return tot

    passos: list[tuple[str, Callable[[], dict | None]]] = [
        ("sincronizar_entrada", lambda: armazenamento.baixar(cfg)),
        ("simular_origem", simular),
        ("ingestao_core", ingerir_core),
        ("ingestao_arquivos", ingerir_arquivos),
        ("ingestao_bcb", lambda: bcb.ingerir(cfg, fim)),
        ("bronze_para_silver", lambda: __import__("banco360.jobs.silver", fromlist=["executar"]).executar(get_spark(), cfg, datas)),
        ("qualidade_silver", lambda: _qualidade(exe, get_spark(), "silver")),
        ("silver_para_gold", lambda: __import__("banco360.jobs.gold", fromlist=["executar"]).executar(get_spark(), cfg, datas)),
        ("qualidade_gold", lambda: _qualidade(exe, get_spark(), "gold")),
        ("carga_warehouse", lambda: carga.carregar(cfg, datas)),
        ("manutencao", lambda: _manutencao(cfg, fim)),
        ("sincronizar_saida", lambda: armazenamento.enviar(cfg)),
    ]
    try:
        for nome, fn in passos:
            if nome in pedidas:
                exe.etapa(nome, fn)
        exe.finalizar()
    except BaseException as e:
        exe.finalizar(e)
        raise
    return exe.id
