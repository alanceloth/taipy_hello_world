import duckdb
from datetime import datetime

# Caminhos dos arquivos de cadastros e pedidos
data_referencia = (datetime.today()).strftime('%Y-%m-%d')
caminho_bronze_cadastros = f'./datasets/bronze_data/cadastros/cadastros_2024-09-15.parquet'
caminho_bronze_pedidos = f'./datasets/bronze_data/pedidos/pedidos_2024-09-15.parquet'

# Conectando ao DuckDB
con = duckdb.connect()

# Criar tabela temporária de cadastros
con.execute(f"""
    CREATE TEMPORARY TABLE cadastros AS 
    SELECT * FROM '{caminho_bronze_cadastros}'
""")

# Criar tabela temporária de pedidos
con.execute(f"""
    CREATE TEMPORARY TABLE pedidos AS 
    SELECT * FROM '{caminho_bronze_pedidos}'
""")

# Fazer o join e consolidar os dados conforme solicitado e mostrar apenas 10 linhas
result = con.execute("""
    SELECT
        c.cpf,
        c.nome,
        c.cidade,
        c.estado,
        c.pais,
        SUM(p.valor_pedido) AS total_gasto,
        COUNT(DISTINCT p.id_pedido) AS total_pedidos
    FROM
        cadastros c
    JOIN
        pedidos p
    ON
        c.cpf = p.cpf
    WHERE
        p.status_pedido = 'faturado'
    GROUP BY
        c.cpf, c.nome, c.cidade, c.estado, c.pais
    LIMIT 10
""").fetchdf()

print("As 10 primeiras linhas da tabela consolidada:\n", result)

# Fechando a conexão
con.close()
