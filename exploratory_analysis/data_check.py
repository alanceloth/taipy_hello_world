import duckdb
from datetime import datetime, timedelta

# Caminhos dos arquivos de cadastros e pedidos
data_referencia = (datetime.today()).strftime('%Y-%m-%d')
caminho_bronze_cadastros = f'./datasets/bronze_data/cadastros/cadastros_2024-09-15.parquet'
caminho_bronze_pedidos = f'./datasets/bronze_data/pedidos/pedidos_2024-09-15.parquet'

# Conectando ao DuckDB
con = duckdb.connect()

# Consulta os dados de cadastros
result_cadastros = con.execute(f"SELECT * FROM '{caminho_bronze_cadastros}' LIMIT 5").fetchdf()
print("Dados de exemplo de cadastros:\n", result_cadastros)

# Consulta os dados de pedidos
result_pedidos = con.execute(f"SELECT * FROM '{caminho_bronze_pedidos}' LIMIT 5").fetchdf()
print("Dados de exemplo de pedidos:\n", result_pedidos)

# Fechando a conexão
con.close()