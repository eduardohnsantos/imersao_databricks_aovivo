-- Bronze Layer: transaction_commodities
-- Ingestão de dados brutos de transações de commodities do volume usando cloud_files

CREATE OR REFRESH STREAMING TABLE bronze.transaction_commodities
AS SELECT 
  transaction_id,
  data_hora,
  commodity_code,
  quantidade,
  tipo_operacao,
  unidade,
  moeda,
  cliente_id,
  canal,
  mercado,
  arquivo_origem,
  importado_em,
  current_timestamp() as ingested_at
FROM cloud_files(
  "/Volumes/lakehouse_aovivo/raw/transaction_commodities",
  "csv",
  map("header", "true", "inferSchema", "true")
)