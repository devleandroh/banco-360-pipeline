"""
DAG diária do Banco 360 (Apache Airflow 2.10 — mesma linha de versões do Amazon MWAA).

Fluxo:
    simular_origem ─┬─ ingestao_core ─────┐
                    ├─ ingestao_arquivos ─┼─ bronze_para_silver ─ qualidade_silver ─ silver_para_gold
                    └─ ingestao_bcb ──────┘        ─ qualidade_gold ─ carga_warehouse ─ manutencao ─ sincronizar_saida

* As três ingestões são independentes e rodam em paralelo.
* Os portões de qualidade (qualidade_*) falham a DAG antes de publicar dados ruins.
* Todas as tarefas compartilham o mesmo id de execução, derivado do run_id, e
  registram suas etapas em banco.pipeline_execucoes (painel de observabilidade).
* `{{ ds }}` é a data de referência (D-1): reprocessar um dia = limpar e reexecutar a DAG run.
"""
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

CMD = "cd /opt/banco360 && python -m banco360 executar --data {{ ds }} --execucao-id '{{ run_id }}' --etapas %s %s"

with DAG(
    dag_id="banco360_diario",
    description="Lakehouse bancário: origem → Bronze → Silver → Gold → Data Warehouse",
    schedule="0 6 * * *",  # todo dia às 06:00 (horário de Brasília)
    start_date=pendulum.datetime(2026, 11, 1, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "engenharia-de-dados",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
        "execution_timeout": timedelta(hours=1),
    },
    tags=["banco360", "lakehouse", "credito"],
    doc_md=__doc__,
) as dag:

    def tarefa(etapa: str, simular: bool = False, ultima: bool = False) -> BashOperator:
        flags = ("--simular " if simular else "") + ("" if ultima else "--parcial")
        return BashOperator(task_id=etapa, bash_command=CMD % (etapa, flags))

    simular = tarefa("simular_origem", simular=True)
    ingestoes = [tarefa("ingestao_core"), tarefa("ingestao_arquivos"), tarefa("ingestao_bcb")]
    silver = tarefa("bronze_para_silver")
    dq_silver = tarefa("qualidade_silver")
    gold = tarefa("silver_para_gold")
    dq_gold = tarefa("qualidade_gold")
    carga = tarefa("carga_warehouse")
    manutencao = tarefa("manutencao")
    publicar = tarefa("sincronizar_saida", ultima=True)

    simular >> ingestoes >> silver >> dq_silver >> gold >> dq_gold >> carga >> manutencao >> publicar
