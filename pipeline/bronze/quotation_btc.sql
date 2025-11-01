-- Bronze Layer: Quotation BTC Table
-- Ingestão de cotações Bitcoin a partir de volumes CSV
-- Fonte: /Volumes/lakehouse/raw_public/quotation_btc

CREATE OR REFRESH STREAMING TABLE bronze.quotation_btc
COMMENT "Tabela Bronze para ingestão de cotações Bitcoin"
AS
SELECT
  *,
  current_timestamp() AS ingested_at
FROM cloud_files(
  "/Volumes/lakehouse/raw_public/quotation_btc",
  "csv",
  map(
    "header", "true",
    "inferSchema", "true"
  )
);

