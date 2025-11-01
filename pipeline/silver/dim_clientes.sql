-- Silver Layer: dim_clientes
-- Dimensão de clientes com anonimização e validação
-- Fonte: bronze.customers

CREATE OR REFRESH STREAMING TABLE silver.dim_clientes
AS SELECT 
  customer_id,
  customer_name,
  SHA2(documento, 256) as documento_hash,
  segmento,
  pais,
  estado,
  cidade,
  created_at,
  ingested_at
  
FROM STREAM(bronze.customers)
WHERE
  customer_id IS NOT NULL
  AND segmento IN ('Financeiro', 'Indústria', 'Varejo', 'Tecnologia')
  AND pais IN ('Brasil', 'Alemanha', 'Estados Unidos');

