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

recent_transactions AS (
  SELECT 
    customer_sk
  FROM transaction_data
  WHERE data_hora >= (current_timestamp() - INTERVAL 30 DAYS)
),

metrics AS (
  SELECT 
    customer_sk,
    COUNT(*) AS total_transacoes,
    SUM(gross_value) AS valor_total,
    AVG(gross_value) AS ticket_medio,
    MIN(data_hora) AS primeira_transacao,
    MAX(data_hora) AS ultima_transacao,
    SUM(fee_revenue) AS comissao_total
  FROM transaction_data
  GROUP BY customer_sk
),

transacoes_ultimos_30_dias AS (
  SELECT 
    customer_sk,
    COUNT(*) AS transacoes_ultimos_30_dias
  FROM recent_transactions
  GROUP BY customer_sk
)

SELECT 
  m.customer_sk,
  m.total_transacoes,
  m.valor_total,
  m.ticket_medio,
  m.primeira_transacao,
  m.ultima_transacao,
  COALESCE(t.transacoes_ultimos_30_dias, 0) AS transacoes_ultimos_30_dias,
  m.comissao_total,
  current_timestamp() AS calculated_at
FROM metrics m
LEFT JOIN transacoes_ultimos_30_dias t
  ON m.customer_sk = t.customer_sk