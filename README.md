# Banco 360 — Lakehouse bancário com PySpark, Airflow e qualidade de dados

Pipeline de dados ponta a ponta para um banco de varejo **simulado**: ingestão de um core bancário relacional,
arquivos de cartão (CSV) e pagamentos (JSON) e indicadores macroeconômicos do **Banco Central (API SGS)**;
tratamento em camadas **Bronze → Silver → Gold** com **PySpark**; contratos de **qualidade de dados**;
anonimização para **LGPD**; e carga em um **Data Warehouse dimensional** consumido pelo portfólio.

> Demo ao vivo: <https://portfolio-leandro-weld.vercel.app/pt/demo/banco> ·
> Observabilidade do pipeline: <https://portfolio-leandro-weld.vercel.app/pt/demo/banco/pipeline>

O desenho segue a arquitetura AWS de referência (**S3 + Glue + Athena + Redshift + MWAA**), executada sem custo com
**Supabase Storage (API S3)**, **Postgres**, **Spark local** e **GitHub Actions**. Cada peça tem equivalente direto na AWS —
ver [`docs/aws.md`](docs/aws.md).

```
 Fontes                      Data Lake (Supabase Storage · S3)                      Warehouse (Postgres ≈ Redshift)
 ──────                      ──────────────────────────────────                     ───────────────────────────────
 core bancário (Postgres) ─┐
 cartões  .csv  ───────────┼─▶ bronze/ ──PySpark──▶ silver/ ──PySpark──▶ gold/ ──COPY──▶ banco.dim_* / fato_* / mart_*
 pagamentos .jsonl ────────┤   (bruto,     (tipado, deduplicado,   (estrela +            │
 API SGS do BCB ───────────┘    imutável)   LGPD, quarentena)       marts)               ▼
                                     ▲            ▲                    ▲          views públicas (RLS) ─▶ Next.js (Vercel)
                                     └──── regras de qualidade + reconciliação entre camadas ────┘
                                     orquestração: Airflow (DAG) · agendamento diário: GitHub Actions
```

## Destaques técnicos

| Tema | Como foi feito |
|---|---|
| **Medallion / Lakehouse** | Parquet particionado por `dt=AAAA-MM-DD`; *dynamic partition overwrite* → reprocessar um dia substitui só aquela partição |
| **PySpark (paridade Glue 5.0)** | Spark 3.5, jobs com argumentos no estilo Glue (`--data_inicio`, `--data_fim`), *broadcast join*, *window functions*, deduplicação com `row_number` |
| **Qualidade de dados** | 60+ regras declarativas em [`config/qualidade.yml`](config/qualidade.yml) (não nulo, único, domínio, intervalo, regex, integridade referencial, volume) + reconciliação arquivo→bronze→silver→gold→mart + atualidade. Severidade `erro` interrompe antes de publicar; `alerta` registra e segue |
| **Quarentena** | Linhas inválidas não são descartadas em silêncio: vão para `silver/transacoes_quarentena` com o motivo |
| **LGPD** | CPF vira `SHA-256(sal + cpf)`; nome, e-mail e data de nascimento não passam da Bronze (regra `colunas_proibidas` garante) |
| **Modelagem dimensional** | 2 fatos, 5 dimensões, 5 marts; chaves substitutas determinísticas (`xxhash64`) |
| **Risco de crédito** | Estágios inspirados na Res. CMN 4.966 / IFRS 9 (1: ≤30 dias, 2: 31–90, 3: >90 ou baixa), NPL 90+, provisão simplificada |
| **Idempotência** | Dimensões via *upsert*, fatos via *delete + insert* por partição, marts por substituição — rodar duas vezes o mesmo dia dá o mesmo resultado |
| **Observabilidade** | Cada execução grava etapas, durações, volumes e resultados de qualidade em `banco.pipeline_execucoes` e `banco.dq_resultados` |
| **Segurança** | RLS em todas as tabelas; o papel `anon` só lê marts agregados por meio de views; dados de cliente nunca saem do schema |
| **Orquestração** | DAG Airflow 2.10 (versão suportada pelo Amazon MWAA) + agendamento diário no GitHub Actions |

## Estrutura

```
config/qualidade.yml          contratos de qualidade por camada/dataset
dags/banco360_dag.py          DAG do Airflow (uma task por etapa)
sql/warehouse/001_banco.sql   DDL do warehouse (tabelas, RLS, views públicas)
sql/analises/                 consultas analíticas de exemplo (window functions, CTEs, safra)
src/banco360/
  simulador/                  core bancário determinístico (4 mil clientes, 3,6 mil contratos)
  ingestao/                   origem → bronze (Postgres, arquivos, API do BCB) + manifestos
  jobs/silver.py, gold.py     transformações PySpark
  qualidade/regras.py         motor de qualidade e reconciliação
  warehouse/carga.py          carga idempotente no warehouse + observabilidade
  armazenamento.py            sincronização do lake com o object storage (API S3)
  consulta.py                 SQL ad hoc sobre o lake com DuckDB (papel do Athena)
  pipeline.py                 orquestra as 12 etapas
tests/                        pytest (simulador, Silver, qualidade, DAG)
```

## Como rodar localmente

Pré-requisito: Docker.

```bash
docker compose up -d core-db warehouse-db
docker compose run --rm pipeline ddl                                   # cria o schema banco
docker compose run --rm pipeline backfill --inicio 2025-10-12 --simular  # 13 meses de histórico
docker compose run --rm pipeline executar --simular                    # execução diária (ontem)
docker compose run --rm pipeline consultar "select * from gold.mart_carteira_mensal limit 5"
docker compose run --rm pipeline pytest -q
docker compose --profile airflow up -d                                 # Airflow em http://localhost:8080
```

Sem `.env`, tudo roda contra o Postgres local e o lake fica no volume Docker. Com `.env` preenchido (ver
[`.env.example`](.env.example)), o lake é sincronizado com o Supabase Storage e a carga vai para o Supabase.

## Execução diária (GitHub Actions)

[`.github/workflows/pipeline-diario.yml`](.github/workflows/pipeline-diario.yml) roda todo dia às 06:30 (Brasília).
Secrets necessários: `WAREHOUSE_DB_URL`, `LAKE_S3_ENDPOINT`, `LAKE_S3_REGION`, `LAKE_S3_BUCKET`,
`LAKE_S3_ACCESS_KEY_ID`, `LAKE_S3_SECRET_ACCESS_KEY`, `PII_SALT`. Sem eles, o workflow apenas avisa e encerra.

## Dados

Todos os clientes, contas e contratos são **fictícios**, gerados por um simulador determinístico (semente fixa).
Os indicadores macroeconômicos (SELIC, IPCA, dólar PTAX) são **reais**, coletados da API pública SGS do Banco Central.
