-- Gold Layer: mostvaluableclient
-- Agregação de métricas de negócio e segmentação de clientes por valor
-- Fonte: silver.fact_transaction_revenue

CREATE OR REFRESH STREAMING TABLE gold.mostvaluableclient
AS
WITH transaction_data AS (
  SELECT 
    customer_sk,
    data_hora,
    gross_value,
    fee_revenue,
    MAX(data_hora) OVER () AS max_data_hora_global
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
    COUNT(CASE 
      WHEN data_hora >= max_data_hora_global - INTERVAL 30 DAYS 
      THEN 1 
    END) AS transacoes_ultimos_30_dias,
    SUM(fee_revenue) AS comissao_total
  FROM transaction_data
  GROUP BY customer_sk
),
ranked_clients AS (
  SELECT 
    customer_sk,
    total_transacoes,
    valor_total,
    ticket_medio,
    primeira_transacao,
    ultima_transacao,
    COALESCE(transacoes_ultimos_30_dias, 0) AS transacoes_ultimos_30_dias,
    comissao_total,
    RANK() OVER (ORDER BY total_transacoes DESC) AS ranking_por_transacoes,
    CASE 
      WHEN RANK() OVER (ORDER BY total_transacoes DESC) = 1 THEN 'Top 1'
      WHEN RANK() OVER (ORDER BY total_transacoes DESC) = 2 THEN 'Top 2'
      WHEN RANK() OVER (ORDER BY total_transacoes DESC) = 3 THEN 'Top 3'
      ELSE 'Outros'
    END AS classificacao_cliente
  FROM metrics
)
SELECT 
  customer_sk,
  total_transacoes,
  valor_total,
  ticket_medio,
  primeira_transacao,
  ultima_transacao,
  transacoes_ultimos_30_dias,
  comissao_total,
  ranking_por_transacoes,
  classificacao_cliente,
  current_timestamp() AS calculated_at
FROM ranked_clients
ORDER BY total_transacoes DESC;

