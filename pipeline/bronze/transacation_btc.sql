-- Bronze Layer: transaction_btc
-- Ingestão de dados brutos de transações Bitcoin do volume usando cloud_files

CREATE OR REFRESH STREAMING TABLE bronze.transaction_btc
AS SELECT 
  transaction_id,
  data_hora,
  ativo,
  quantidade,
  tipo_operacao,
  moeda,
  cliente_id,
  canal,
  mercado,
  arquivo_origem,
  importado_em,
  current_timestamp() as ingested_at
FROM cloud_files(
  "/Volumes/lakehouse_aovivo/raw/transacation_btc",
  "csv",
  map("header", "true", "inferSchema", "true")
)