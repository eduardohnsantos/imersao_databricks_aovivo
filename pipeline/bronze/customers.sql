-- Bronze Layer: customers
-- Ingestão de dados brutos de clientes do volume usando cloud_files

CREATE OR REFRESH STREAMING TABLE bronze.customers
AS SELECT 
  customer_id,
  customer_name,
  documento,
  segmento,
  pais,
  estado,
  cidade,
  created_at,
  current_timestamp() as ingested_at
FROM cloud_files(
  "/Volumes/lakehouse_aovivo/raw/customers",
  "csv",
  map("header", "true", "inferSchema", "true")
)

