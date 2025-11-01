-- Bronze Layer: Customers Table
-- Ingestão de dados de clientes a partir de volumes CSV
-- Fonte: /Volumes/lakehouse/raw_public/customers

CREATE OR REFRESH STREAMING TABLE bronze.customers
COMMENT "Tabela Bronze para ingestão de dados de clientes"
AS
SELECT
  *,
  current_timestamp() AS ingested_at
FROM cloud_files(
  "/Volumes/lakehouse/raw_public/customers",
  "csv",
  map(
    "header", "true",
    "inferSchema", "true"
  )
);

