CREATE OR REFRESH STREAMING TABLE gold.mostvaluableclient
AS
SELECT
  customer_sk,
  COUNT(*) AS total_transacoes,
  SUM(gross_value) AS valor_total,
  AVG(gross_value) AS ticket_medio,
  MIN(data_hora) AS primeira_transacao,
  MAX(data_hora) AS ultima_transacao,
  SUM(fee_revenue) AS comissao_total,
  -- Contagem dos últimos 30 dias usando CASE WHEN
  COUNT(CASE WHEN data_hora >= current_timestamp() - INTERVAL 30 DAYS THEN 1 END) AS transacoes_ultimos_30_dias,
  current_timestamp() AS calculated_at
FROM STREAM(silver.fact_transaction_revenue)
GROUP BY customer_sk