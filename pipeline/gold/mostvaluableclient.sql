CREATE OR REFRESH STREAMING TABLE gold.mostvaluableclient
AS
WITH transaction_data AS (
  SELECT 
    customer_sk,
    data_hora,
    gross_value,
    fee_revenue
  FROM STREAM(silver.fact_transaction_revenue)
),
metrics AS (
  SELECT 
    customer_sk,
    COUNT(*) AS total_transacoes,
    SUM(gross_value) AS valor_total,
    AVG(gross_value) AS ticket_medio,
    MIN(data_hora) AS primeira_transacao,
    MAX(data_hora) AS ultima_transacao,
    SUM(
      CASE 
        WHEN data_hora >= current_timestamp() - INTERVAL 30 DAYS 
        THEN 1 
        ELSE 0
      END
    ) AS transacoes_ultimos_30_dias,
    SUM(fee_revenue) AS comissao_total
  FROM transaction_data
  GROUP BY customer_sk
)
SELECT 
  customer_sk,
  total_transacoes,
  valor_total,
  ticket_medio,
  primeira_transacao,
  ultima_transacao,
  COALESCE(transacoes_ultimos_30_dias, 0) AS transacoes_ultimos_30_dias,
  comissao_total,
  current_timestamp() AS calculated_at
FROM metrics
ORDER BY total_transacoes DESC