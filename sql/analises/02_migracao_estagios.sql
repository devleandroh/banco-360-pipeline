-- Matriz de migração entre estágios de risco (IFRS 9 / Res. CMN 4.966) de um fechamento mensal para o seguinte.
-- Usa LAG para comparar cada contrato com a sua própria posição no mês anterior.
with fechamentos as (
  select max(t.data_sk) as data_sk, t.ano_mes
  from banco.dim_tempo t
  join banco.fato_posicao_contrato f on f.data_sk = t.data_sk
  group by t.ano_mes
),
serie as (
  select
    f.contrato_id,
    fe.ano_mes,
    f.estagio,
    f.saldo_devedor,
    lag(f.estagio) over (partition by f.contrato_id order by fe.ano_mes) as estagio_anterior
  from banco.fato_posicao_contrato f
  join fechamentos fe on fe.data_sk = f.data_sk
)
select
  estagio_anterior as de,
  estagio          as para,
  count(*)         as contratos,
  round(100.0 * count(*) / sum(count(*)) over (partition by estagio_anterior), 1) as pct_da_origem,
  round(sum(saldo_devedor), 2) as saldo
from serie
where estagio_anterior is not null
  and ano_mes = (select max(ano_mes) from fechamentos)
group by estagio_anterior, estagio
order by de, para;
