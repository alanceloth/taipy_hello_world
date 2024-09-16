import duckdb
import os
from dotenv import load_dotenv

# Carregar as variáveis de ambiente do arquivo .env
load_dotenv()

# Obter as variáveis de ambiente para a conexão PostgreSQL
POSTGRES_HOSTNAME = os.getenv('POSTGRES_HOSTNAME')
POSTGRES_DBNAME = os.getenv('POSTGRES_DBNAME')
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_PORT = os.getenv('POSTGRES_PORT')

# String de conexão para o PostgreSQL
postgres_conn = f"postgres://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOSTNAME}:{POSTGRES_PORT}/{POSTGRES_DBNAME}"

# Caminho para as pastas de arquivos Parquet
parquet_dir_cadastros = './datasets/raw_data/cadastros/'
parquet_dir_pedidos = './datasets/raw_data/pedidos/'

# Conexão com o DuckDB e o PostgreSQL
con = duckdb.connect()

# Instalar e carregar o scanner PostgreSQL no DuckDB
con.execute("""
    INSTALL postgres_scanner;
    LOAD postgres_scanner;
""")

# Função para carregar arquivos Parquet e transferir para PostgreSQL
def load_parquet_to_postgres(parquet_dir, postgres_table):
    parquet_files = os.path.join(parquet_dir, '*.parquet')
    
    # Carregar todos os arquivos Parquet do diretório para uma tabela temporária no DuckDB
    con.execute(f"CREATE OR REPLACE TEMPORARY VIEW temp_view AS SELECT * FROM read_parquet('{parquet_files}');")
    
    # Inserir os dados da tabela temporária no PostgreSQL
    con.execute(f"""
        COPY (SELECT * FROM temp_view) TO '{postgres_conn}.{postgres_table}'
        (FORMAT CSV, HEADER);
    """)
    print(f"Dados de {parquet_dir} carregados na tabela {postgres_table} no PostgreSQL")

# Carregar os arquivos de cadastros e pedidos para PostgreSQL
load_parquet_to_postgres(parquet_dir_cadastros, 'raw_cadastros')
load_parquet_to_postgres(parquet_dir_pedidos, 'raw_pedidos')

# Fechar a conexão
con.close()
