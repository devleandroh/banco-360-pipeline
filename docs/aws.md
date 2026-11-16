# Equivalência com a AWS

O projeto foi desenhado para a arquitetura de referência de dados na AWS, mas roda sem custo. A tabela mostra o que
cada peça faz, o que foi usado aqui e o que seria usado em produção.

| Papel | Na AWS | Neste projeto | Observação |
|---|---|---|---|
| Data Lake (object storage) | **Amazon S3** | Supabase Storage pela **API compatível com S3** (`boto3`) | O mesmo código fala com o S3 real: basta deixar `LAKE_S3_ENDPOINT` vazio e usar credenciais IAM |
| Processamento distribuído | **AWS Glue** (jobs Spark, Glue 5.0) | **PySpark 3.5** local / no runner | Jobs recebem `--data_inicio`/`--data_fim` como os parâmetros de um Glue job; nada depende de `awsglue` |
| Catálogo de metadados | **Glue Data Catalog** | Convenção de caminhos `camada/dataset/dt=` + manifestos | Num Glue Crawler os mesmos caminhos virariam tabelas particionadas |
| SQL sobre o lake | **Amazon Athena** | **DuckDB** (`python -m banco360 consultar`) | Ambos leem Parquet direto do storage, sem carregar em banco |
| Data Warehouse | **Amazon Redshift** | **Postgres** (Supabase) | Modelo estrela, carga via `COPY`, *upsert* por staging — o mesmo padrão do Redshift |
| Orquestração | **Amazon MWAA** (Airflow gerenciado) | **Airflow 2.10** no Docker + agendamento no **GitHub Actions** | A DAG é compatível com MWAA |
| Segredos | **Secrets Manager** / SSM | `.env` local + **GitHub Secrets** | Nenhuma credencial no código |
| Controle de acesso | **IAM** + Lake Formation | **RLS** do Postgres + views públicas | O consumidor público só enxerga agregados |
| Observabilidade | **CloudWatch** | Tabelas `pipeline_execucoes` e `dq_resultados` + página do portfólio | |
| Qualidade de dados | **Glue Data Quality** (DQDL) | Motor próprio com regras em YAML | Mesmos tipos de regra: completude, unicidade, domínio, intervalo, integridade |
| Consumo / BI | **QuickSight** | Next.js + Recharts na Vercel | |

## Fluxo equivalente em produção

```
EventBridge (06:00) ─▶ MWAA DAG banco360_diario
   ├─ Glue job ingestao_core      (JDBC → s3://lake/bronze/core_bancario/)
   ├─ Glue job ingestao_arquivos  (s3://landing/ → s3://lake/bronze/)
   ├─ Lambda ingestao_bcb         (API SGS → s3://lake/bronze/bcb/)
   ├─ Glue job bronze_para_silver ─▶ Glue Data Quality (gate)
   ├─ Glue job silver_para_gold   ─▶ Glue Data Quality (gate)
   └─ Redshift COPY from s3://lake/gold/  ─▶ QuickSight
```
