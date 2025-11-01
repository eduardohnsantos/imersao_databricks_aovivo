-- Bronze Layer: Quotation yFinance Table
-- Ingestão de cotações yFinance a partir de volumes CSV
-- Fonte: /Volumes/lakehouse/raw_public/quotation_yfinance

CREATE OR REFRESH STREAMING TABLE bronze.quotation_yfinance
COMMENT "Tabela Bronze para ingestão de cotações yFinance"
AS
SELECT
  *,
  current_timestamp() AS ingested_at
FROM cloud_files(
  "/Volumes/lakehouse/raw_public/quotation_yfinance",
  "csv",
  map(
    "header", "true",
    "inferSchema", "true"
  )
);

