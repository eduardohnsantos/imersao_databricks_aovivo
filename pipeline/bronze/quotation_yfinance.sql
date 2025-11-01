-- Bronze Layer: quotation_yfinance
-- Ingestão de dados brutos de cotações yFinance do volume usando cloud_files

CREATE OR REFRESH STREAMING TABLE bronze.quotation_yfinance
AS SELECT 
  ativo,
  preco,
  moeda,
  horario_coleta,
  current_timestamp() as ingested_at
FROM cloud_files(
  "/Volumes/lakehouse/raw_public/quotation_yfinance",
  "csv",
  map("header", "true", "inferSchema", "true")
)