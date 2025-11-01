-- Bronze Layer: Transaction Commodities Table
-- Ingestão de transações de commodities a partir de volumes CSV
-- Fonte: /Volumes/lakehouse/raw_public/transaction_commodities

CREATE OR REFRESH STREAMING TABLE bronze.transaction_commodities
COMMENT "Tabela Bronze para ingestão de transações de commodities"
AS
SELECT
  *,
  current_timestamp() AS ingested_at
FROM cloud_files(
  "/Volumes/lakehouse/raw_public/transaction_commodities",
  "csv",
  map(
    "header", "true",
    "inferSchema", "true"
  )
);

