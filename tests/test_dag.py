"""A DAG do Airflow precisa chamar exatamente as etapas que o pipeline conhece."""
import ast
import re
from pathlib import Path

from banco360.pipeline import ETAPAS


def test_tarefas_da_dag_existem_no_pipeline():
    codigo = Path(__file__).resolve().parents[1].joinpath("dags", "banco360_dag.py").read_text(encoding="utf-8")
    ast.parse(codigo)
    tarefas = set(re.findall(r'tarefa\("([a-z_]+)"', codigo))
    assert tarefas and tarefas <= set(ETAPAS)
