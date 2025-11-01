CREATE OR REFRESH STREAMING TABLE gold.mostvaluableclient
AS
WITH base AS (
  SELECT
    customer_sk,
    data_hora,
    gross_value,
    fee_revenue
  FROM STREAM(silver.fact_transaction_revenue)
),
recent AS (
  SELECT
    customer_sk,
    COUNT(*) AS transacoes_ultimos_30_dias
  FROM base
  WHERE data_hora >= current_timestamp() - INTERVAL 30 DAYS
  GROUP BY customer_sk
)
SELECT
  b.customer_sk,
  COUNT(*) AS total_transacoes,
  SUM(b.gross_value) AS valor_total,
  AVG(b.gross_value) AS ticket_medio,
  MIN(b.data_hora) AS primeira_transacao,
  MAX(b.data_hora) AS ultima_transacao,
  SUM(b.fee_revenue) AS comissao_total,
  COALESCE(r.transacoes_ultimos_30_dias, 0) AS transacoes_ultimos_30_dias,
  current_timestamp() AS calculated_at
FROM base b
LEFT JOIN recent r
  ON b.customer_sk = r.customer_sk
GROUP BY
  b.customer_sk,
  r.transacoes_ultimos_30_dias