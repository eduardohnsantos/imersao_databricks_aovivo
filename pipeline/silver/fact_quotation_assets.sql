-- Silver Layer: fact_quotation_assets
-- União e transformação de cotações Bitcoin e yFinance
-- Fonte: bronze.quotation_btc + bronze.quotation_yfinance

CREATE OR REFRESH STREAMING TABLE silver.fact_quotation_assets
AS 
SELECT 
  CASE 
    WHEN UPPER(ativo) IN ('BTC', 'BTC-USD') THEN 'BTC'
    WHEN UPPER(ativo) IN ('GOLD', 'GC=F') THEN 'GOLD'
    WHEN UPPER(ativo) IN ('OIL', 'CL=F') THEN 'OIL'
    WHEN UPPER(ativo) IN ('SILVER', 'SI=F') THEN 'SILVER'
    ELSE 'UNKNOWN'
  END as asset_symbol,
  preco,
  moeda,
  CAST(horario_coleta AS TIMESTAMP) as horario_coleta,
  date_trunc('hour', CAST(horario_coleta AS TIMESTAMP)) as data_hora_aproximada,
  ativo as ativo_original,
  ingested_at
FROM STREAM(bronze.quotation_btc)
WHERE
  preco > 0
  AND CAST(horario_coleta AS TIMESTAMP) <= current_timestamp()
  AND ativo IS NOT NULL 
  AND ativo != ''
  AND moeda = 'USD'

UNION ALL

SELECT 
  CASE 
    WHEN UPPER(ativo) IN ('BTC', 'BTC-USD') THEN 'BTC'
    WHEN UPPER(ativo) IN ('GOLD', 'GC=F') THEN 'GOLD'
    WHEN UPPER(ativo) IN ('OIL', 'CL=F') THEN 'OIL'
    WHEN UPPER(ativo) IN ('SILVER', 'SI=F') THEN 'SILVER'
    ELSE 'UNKNOWN'
  END as asset_symbol,
  preco,
  moeda,
  CAST(horario_coleta AS TIMESTAMP) as horario_coleta,
  date_trunc('hour', CAST(horario_coleta AS TIMESTAMP)) as data_hora_aproximada,
  ativo as ativo_original,
  ingested_at
FROM STREAM(bronze.quotation_yfinance)
WHERE
  preco > 0
  AND CAST(horario_coleta AS TIMESTAMP) <= current_timestamp()
  AND ativo IS NOT NULL 
  AND ativo != ''
  AND moeda = 'USD';

