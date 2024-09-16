-- models/bronze_pedidos.sql
WITH raw_data AS (
  SELECT * FROM {{ source('bronze_data', 'bronze_pedidos') }}
)
SELECT
  id_pedido,
  cpf,
  valor_pedido,
  valor_frete,
  valor_desconto,
  cupom,
  endereco_entrega_logradouro,
  endereco_entrega_numero,
  endereco_entrega_bairro,
  endereco_entrega_cidade,
  endereco_entrega_estado,
  endereco_entrega_pais,
  status_pedido
FROM raw_data
