-- Relação entre a taxa média das concessões do mês e a SELIC meta (dado real do Banco Central).
-- Taxa média ponderada pelo valor: sum(valor × taxa) / sum(valor).
select
  c.ano_mes,
  m.selic_meta,
  m.ipca_12m,
  sum(c.novos_contratos)                                                     as novos_contratos,
  round(sum(c.valor_concedido), 2)                                           as valor_concedido,
  round(100 * sum(c.valor_x_taxa_concedido) / nullif(sum(c.valor_concedido), 0), 2) as taxa_media_mensal_pct,
  round(100 * (power(1 + sum(c.valor_x_taxa_concedido) / nullif(sum(c.valor_concedido), 0), 12) - 1), 1) as taxa_media_anual_pct,
  round(100.0 * sum(c.saldo_atraso_90) / nullif(sum(c.saldo_carteira), 0), 2)       as npl_90_pct
from banco.mart_carteira_mensal c
left join banco.mart_macro_mensal m using (ano_mes)
group by c.ano_mes, m.selic_meta, m.ipca_12m
order by c.ano_mes;
