-- models/bronze_cadastros.sql
WITH raw_data AS (
  SELECT * FROM {{ source('bronze_data', 'bronze_cadastros') }}
)
SELECT
  id,
  nome,
  data_nascimento,
  cpf,
  cep,
  cidade,
  estado,
  pais,
  genero,
  telefone,
  email
FROM raw_data
