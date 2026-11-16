-- Curva de inadimplência por safra (vintage): % do valor contratado de cada safra que está 90+ dias em atraso,
-- medido a cada fechamento de mês após a contratação. Técnica clássica de risco de crédito.
with fechamentos as (
  select max(t.data_sk) as data_sk, t.ano_mes
  from banco.dim_tempo t
  join banco.fato_posicao_contrato f on f.data_sk = t.data_sk
  group by t.ano_mes
),
posicoes as (
  select
    f.safra,
    fe.ano_mes as mes_observacao,
    (extract(year from to_date(fe.ano_mes, 'YYYY-MM')) * 12 + extract(month from to_date(fe.ano_mes, 'YYYY-MM')))
      - (extract(year from to_date(f.safra, 'YYYY-MM')) * 12 + extract(month from to_date(f.safra, 'YYYY-MM'))) as meses_desde_safra,
    f.valor_contratado,
    case when f.dias_atraso > 90 then f.saldo_devedor else 0 end as saldo_90
  from banco.fato_posicao_contrato f
  join fechamentos fe on fe.data_sk = f.data_sk
)
select
  safra,
  meses_desde_safra,
  round(100 * sum(saldo_90) / nullif(sum(valor_contratado), 0), 2) as pct_90_sobre_contratado
from posicoes
where meses_desde_safra between 1 and 12
group by safra, meses_desde_safra
order by safra, meses_desde_safra;
