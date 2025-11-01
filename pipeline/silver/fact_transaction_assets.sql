-- Silver Layer: fact_transaction_assets
-- União e transformação de transações Bitcoin e Commodities
-- Fonte: bronze.transaction_btc + bronze.transaction_commodities

CREATE OR REFRESH STREAMING TABLE silver.fact_transaction_assets
AS 
SELECT 
  transaction_id,
  CAST(data_hora AS TIMESTAMP) as data_hora,
  date_trunc('hour', CAST(data_hora AS TIMESTAMP)) as data_hora_aproximada,
  CASE 
    WHEN UPPER(ativo) IN ('BTC', 'BTC-USD') THEN 'BTC'
    WHEN UPPER(ativo) IN ('GOLD', 'GC=F') THEN 'GOLD'
    WHEN UPPER(ativo) IN ('OIL', 'CL=F') THEN 'OIL'
    WHEN UPPER(ativo) IN ('SILVER', 'SI=F') THEN 'SILVER'
    ELSE 'UNKNOWN'
  END as asset_symbol,
  quantidade,
  tipo_operacao,
  moeda,
  cliente_id,
  canal,
  mercado,
  NULL as unidade,
  arquivo_origem,
  importado_em,
  ingested_at
FROM STREAM(bronze.transaction_btc)
WHERE
  quantidade > 0
  AND data_hora IS NOT NULL
  AND tipo_operacao IN ('COMPRA', 'VENDA')
  AND CASE 
        WHEN UPPER(ativo) IN ('BTC', 'BTC-USD') THEN 'BTC'
        WHEN UPPER(ativo) IN ('GOLD', 'GC=F') THEN 'GOLD'
        WHEN UPPER(ativo) IN ('OIL', 'CL=F') THEN 'OIL'
        WHEN UPPER(ativo) IN ('SILVER', 'SI=F') THEN 'SILVER'
        ELSE 'UNKNOWN'
      END IN ('BTC', 'GOLD', 'OIL', 'SILVER')

UNION ALL

SELECT 
  transaction_id,
  CAST(data_hora AS TIMESTAMP) as data_hora,
  date_trunc('hour', CAST(data_hora AS TIMESTAMP)) as data_hora_aproximada,
  CASE 
    WHEN UPPER(commodity_code) IN ('BTC', 'BTC-USD') THEN 'BTC'
    WHEN UPPER(commodity_code) IN ('GOLD', 'GC=F') THEN 'GOLD'
    WHEN UPPER(commodity_code) IN ('OIL', 'CL=F') THEN 'OIL'
    WHEN UPPER(commodity_code) IN ('SILVER', 'SI=F') THEN 'SILVER'
    ELSE 'UNKNOWN'
  END as asset_symbol,
  quantidade,
  tipo_operacao,
  moeda,
  cliente_id,
  canal,
  mercado,
  unidade,
  arquivo_origem,
  importado_em,
  ingested_at
FROM STREAM(bronze.transaction_commodities)
WHERE
  quantidade > 0
  AND data_hora IS NOT NULL
  AND tipo_operacao IN ('COMPRA', 'VENDA')
  AND CASE 
        WHEN UPPER(commodity_code) IN ('BTC', 'BTC-USD') THEN 'BTC'
        WHEN UPPER(commodity_code) IN ('GOLD', 'GC=F') THEN 'GOLD'
        WHEN UPPER(commodity_code) IN ('OIL', 'CL=F') THEN 'OIL'
        WHEN UPPER(commodity_code) IN ('SILVER', 'SI=F') THEN 'SILVER'
        ELSE 'UNKNOWN'
      END IN ('BTC', 'GOLD', 'OIL', 'SILVER');

