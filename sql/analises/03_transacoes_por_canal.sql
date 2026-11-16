-- Participação de cada canal no volume transacionado, média móvel de 7 dias e variação mês contra mês.
with diario as (
  select t.data, t.ano_mes, c.canal, count(*) as qtd, sum(f.valor) as valor
  from banco.fato_transacao f
  join banco.dim_tempo t on t.data_sk = f.data_sk
  join banco.dim_canal c on c.canal_sk = f.canal_sk
  group by t.data, t.ano_mes, c.canal
),
mensal as (
  select ano_mes, canal, sum(qtd) as qtd, sum(valor) as valor
  from diario
  group by ano_mes, canal
)
select
  m.ano_mes,
  m.canal,
  m.qtd,
  round(100.0 * m.valor / sum(m.valor) over (partition by m.ano_mes), 1) as pct_valor_no_mes,
  round(100.0 * (m.qtd::numeric / nullif(lag(m.qtd) over (partition by m.canal order by m.ano_mes), 0) - 1), 1) as var_qtd_mom,
  (select round(avg(d.qtd), 0)
     from diario d
    where d.canal = m.canal
      and d.data > (select max(data) from diario where ano_mes = m.ano_mes) - 7
      and d.data <= (select max(data) from diario where ano_mes = m.ano_mes)) as media_movel_7d_fim_mes
from mensal m
order by m.ano_mes desc, m.valor desc;
