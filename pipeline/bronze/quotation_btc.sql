-- Bronze Layer: quotation_btc
-- Ingestão de dados brutos de cotações Bitcoin do volume usando cloud_files

CREATE OR REFRESH STREAMING TABLE bronze.quotation_btc
AS SELECT 
  ativo,
  preco,
  moeda,
  horario_coleta,
  current_timestamp() as ingested_at
FROM cloud_files(
  "/Volumes/lakehouse_aovivo/raw/quotation_btc",
  "csv",
  map("header", "true", "inferSchema", "true")
)

