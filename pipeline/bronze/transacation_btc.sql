-- Bronze Layer: Transaction BTC Table
-- Ingestão de transações Bitcoin a partir de volumes CSV
-- Fonte: /Volumes/lakehouse/raw_public/transacation_btc

CREATE OR REFRESH STREAMING TABLE bronze.transaction_btc
COMMENT "Tabela Bronze para ingestão de transações Bitcoin"
AS
SELECT
  *,
  current_timestamp() AS ingested_at
FROM cloud_files(
  "/Volumes/lakehouse/raw_public/transacation_btc",
  "csv",
  map(
    "header", "true",
    "inferSchema", "true"
  )
);

