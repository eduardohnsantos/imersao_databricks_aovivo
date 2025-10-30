CREATE OR REFRESH STREAMING LIVE TABLE bitcoin
TBLPROPERTIES ("quality" = "bronze") 
AS
SELECT * 
FROM cloud_files(
  '/Volumes/lakehouse/raw_public/coinbase/coinbase/bitcoin_spot/',
  -- caminho da origem
  'json',
  map(
    -- Ingestao incompleta
    -- Se "false", o DLT vai processar apenas os novos arquivos
    -- que chegarem após a pipeline começar
    'cloudFiles.includeExistingFiles', 'false',

    -- Detecta automatimanticamente o tipo das colunas (útil em JSON)
    'cloudFiles.inferColumnTypes', 'true',
    
    -- Permite adicionar novas colunas automaticamente (tem também o rescue)
    'cloudFiles.schemaEvolutionMode', 'addNewColumns'
      )
);