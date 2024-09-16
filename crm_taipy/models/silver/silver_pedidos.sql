WITH bronze_pedidos_clean AS (
  -- Inclui os dados da camada bronze de pedidos
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
    status_pedido,
    -- Adiciona uma coluna booleana que indica se um cupom foi usado
    CASE 
      WHEN cupom IS NOT NULL THEN TRUE 
      ELSE FALSE 
    END AS cupom_usado,
    -- Calcula o valor total do pedido considerando o frete e o desconto
    (valor_pedido + valor_frete - valor_desconto) AS valor_total
  FROM {{ ref('bronze_pedidos') }}
)

-- Seleciona os dados já limpos e adiciona as colunas novas
SELECT
  id_pedido,
  cpf,
  valor_pedido,
  valor_frete,
  valor_desconto,
  valor_total,       -- Novo: Valor total do pedido
  cupom,
  cupom_usado,       -- Novo: Indicador de cupom usado (TRUE/FALSE)
  endereco_entrega_logradouro,
  endereco_entrega_numero,
  endereco_entrega_bairro,
  endereco_entrega_cidade,
  endereco_entrega_estado,
  endereco_entrega_pais,
  status_pedido
FROM bronze_pedidos_clean
