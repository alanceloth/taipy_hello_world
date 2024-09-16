import duckdb
import pandas as pd
from taipy.gui import Markdown
import os
from dotenv import load_dotenv
import plotly.graph_objects as go

# Carrega as variáveis de ambiente para o RDS PostgreSQL
load_dotenv()

# Variáveis globais
selected_date = None
df_filtered = None
total_cadastros = 0
total_pedidos = 0
ticket_medio = 0

fig_cadastros = go.Figure()
fig_pedidos = go.Figure()
fig_receita_ticket = go.Figure()

# Função para conectar ao banco de dados PostgreSQL via DuckDB
def connect_duckdb():
    POSTGRES_HOSTNAME = os.getenv('POSTGRES_HOSTNAME')
    POSTGRES_DBNAME = os.getenv('POSTGRES_DBNAME')
    POSTGRES_USER = os.getenv('POSTGRES_USER')
    POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
    POSTGRES_PORT = os.getenv('POSTGRES_PORT')

    con = duckdb.connect()
    con.execute("""
        INSTALL postgres_scanner;
        LOAD postgres_scanner;
    """)
    con.execute(f"""
        ATTACH 'dbname={POSTGRES_DBNAME} user={POSTGRES_USER} host={POSTGRES_HOSTNAME} port={POSTGRES_PORT} password={POSTGRES_PASSWORD}' 
        AS postgres_db (TYPE POSTGRES, SCHEMA 'public');
    """)
    return con

# Função para carregar os KPIs de pedidos e cadastros
def load_kpis():
    conn = connect_duckdb()
    query = """
        SELECT data_pedido, estado, total_pedidos_faturados, receita_total, ticket_medio
        FROM postgres_db.gold_kpi_faturados_por_dia_estado_regiao
    """
    df = conn.execute(query).fetchdf()
    conn.close()
    return df

# Função para inicializar e filtrar os dados
def initialize_kpis(df, selected_date=None):
    # Usar a data atual como valor padrão se `selected_date` for None
    if selected_date is None:
        selected_date = pd.Timestamp('today').normalize()  # Normalizar para remover a parte do tempo (hora)
    
    # Filtrar os dados a partir da data selecionada ou padrão (data atual)
    df_filtered = df[df['data_pedido'] >= pd.to_datetime(selected_date)]
    
    return df_filtered

# Função para atualizar os gráficos e KPIs
def update_dashboard(state):
    global total_cadastros, total_pedidos, ticket_medio, fig_cadastros, fig_pedidos, fig_receita_ticket
    df_filtered = initialize_kpis(load_kpis(), state.selected_date)
    
    # Atualizar KPIs
    total_cadastros = int(df_filtered.shape[0])  # Garantir que seja um número inteiro
    total_pedidos = int(df_filtered['total_pedidos_faturados'].sum())  # Converter para inteiro
    ticket_medio = round(df_filtered['ticket_medio'].mean(), 2) if not df_filtered['ticket_medio'].isna().all() else 0  # Ticket médio arredondado para 2 casas decimais


    # Criação dos gráficos
    fig_cadastros = go.Figure()
    df_cadastros = df_filtered.groupby(df_filtered['data_pedido'].dt.to_period('M')).size().reset_index(name='cadastros')
    fig_cadastros.add_trace(go.Bar(x=df_cadastros['data_pedido'].astype(str), y=df_cadastros['cadastros'], name="Cadastros"))
    state.fig_cadastros = fig_cadastros

    fig_pedidos = go.Figure()
    df_pedidos = df_filtered.groupby(df_filtered['data_pedido'].dt.to_period('M'))['total_pedidos_faturados'].sum().reset_index()
    fig_pedidos.add_trace(go.Bar(x=df_pedidos['data_pedido'].astype(str), y=df_pedidos['total_pedidos_faturados'], name="Pedidos"))
    state.fig_pedidos = fig_pedidos

    fig_receita_ticket = go.Figure()
    df_receita = df_filtered.groupby(df_filtered['data_pedido'].dt.to_period('M'))[['receita_total', 'ticket_medio']].sum().reset_index()
    fig_receita_ticket.add_trace(go.Bar(x=df_receita['data_pedido'].astype(str), y=df_receita['receita_total'], name="Receita", yaxis='y1'))
    fig_receita_ticket.add_trace(go.Scatter(x=df_receita['data_pedido'].astype(str), y=df_receita['ticket_medio'], mode='lines+markers', name="Ticket Médio", yaxis='y2'))
    
    # Configuração dos eixos independentes para o gráfico de receita e ticket médio
    fig_receita_ticket.update_layout(
        title="Receita e Ticket Médio ao Longo dos Meses",
        xaxis_title="Mês",
        yaxis_title="Receita",
        yaxis2=dict(
            title="Ticket Médio",
            overlaying='y',  # Sobrepõe o eixo y principal
            side='right',    # Eixo à direita
            showgrid=False   # Remove a grid do segundo eixo
        )
    )
    state.fig_receita_ticket = fig_receita_ticket

# Função para converter valores para texto
def to_text(value):
    return f"{value:,.2f}"

# Ler o arquivo .md com a codificação correta
def load_markdown_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        return file.read()
# Função para inicializar e processar os dados (sem filtro)
def initialize_dashboard_data():
    global total_cadastros, total_pedidos, ticket_medio, fig_cadastros, fig_pedidos, fig_receita_ticket
    
    # Carregar os dados
    df = load_kpis()
    
    # Atualizar KPIs
    total_cadastros = int(df.shape[0])  # Garantir que seja um número inteiro
    total_pedidos = int(df['total_pedidos_faturados'].sum())  # Converter para inteiro
    ticket_medio = round(df['ticket_medio'].mean(), 2) if not df['ticket_medio'].isna().all() else 0  # Ticket médio arredondado para 2 casas decimais


    # Criar gráficos
    fig_cadastros = go.Figure()
    df_cadastros = df.groupby(df['data_pedido'].dt.to_period('M')).size().reset_index(name='cadastros')
    fig_cadastros.add_trace(go.Bar(x=df_cadastros['data_pedido'].astype(str), y=df_cadastros['cadastros'], name="Cadastros"))

    fig_pedidos = go.Figure()
    df_pedidos = df.groupby(df['data_pedido'].dt.to_period('M'))['total_pedidos_faturados'].sum().reset_index()
    fig_pedidos.add_trace(go.Bar(x=df_pedidos['data_pedido'].astype(str), y=df_pedidos['total_pedidos_faturados'], name="Pedidos"))

    fig_receita_ticket = go.Figure()
    # Gráfico de receita ao longo dos meses e ticket médio com dois eixos y
    df_receita = df.groupby(df['data_pedido'].dt.to_period('M'))[['receita_total', 'ticket_medio']].sum().reset_index()
    fig_receita_ticket.add_trace(go.Bar(x=df_receita['data_pedido'].astype(str), y=df_receita['receita_total'], name="Receita", yaxis='y1'))
    fig_receita_ticket.add_trace(go.Scatter(x=df_receita['data_pedido'].astype(str), y=df_receita['ticket_medio'], mode='lines+markers', name="Ticket Médio", yaxis='y2'))
    # Configuração dos eixos independentes para o gráfico de receita e ticket médio
    fig_receita_ticket.update_layout(
        title="Receita e Ticket Médio ao Longo dos Meses",
        xaxis_title="Mês",
        yaxis_title="Receita",
        yaxis2=dict(
            title="Ticket Médio",
            overlaying='y',  # Sobrepõe o eixo y principal
            side='right',    # Eixo à direita
            showgrid=False   # Remove a grid do segundo eixo
        )
    )

# Chame a função para pré-carregar os dados
initialize_dashboard_data()

# Arquivo Markdown
#dashboard_md = Markdown("dashboard.md")

# Certifique-se de que o caminho para o arquivo markdown está correto
dashboard_md_content = load_markdown_file("frontend/dashboard/dashboard.md")
dashboard_md = Markdown(dashboard_md_content)
