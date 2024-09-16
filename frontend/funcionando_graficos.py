import duckdb
import pandas as pd
from taipy.gui import Gui 
import taipy.gui.builder as tgb 
import plotly.graph_objects as go
import os
from dotenv import load_dotenv

# Carrega as variáveis de ambiente para o RDS PostgreSQL
load_dotenv()

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

# Função para criar gráficos de barras e de linha
def create_graphs(df):
    fig_cadastros = go.Figure()
    fig_pedidos = go.Figure()
    fig_receita_ticket = go.Figure()

    # Converte as datas para string no formato 'YYYY-MM' para evitar problemas de serialização
    df['mes'] = df['data_pedido'].dt.to_period('M').astype(str)

    # Gráfico de cadastros ao longo dos meses
    df_cadastros = df.groupby('mes').size().reset_index(name='cadastros')
    fig_cadastros.add_trace(go.Bar(x=df_cadastros['mes'], y=df_cadastros['cadastros'], name="Cadastros"))

    # Gráfico de pedidos ao longo dos meses
    df_pedidos = df.groupby('mes')['total_pedidos_faturados'].sum().reset_index()
    fig_pedidos.add_trace(go.Bar(x=df_pedidos['mes'], y=df_pedidos['total_pedidos_faturados'], name="Pedidos"))

    # Gráfico de receita ao longo dos meses e ticket médio com dois eixos y
    df_receita = df.groupby('mes')[['receita_total', 'ticket_medio']].sum().reset_index()
    fig_receita_ticket.add_trace(go.Bar(x=df_receita['mes'], y=df_receita['receita_total'], name="Receita", yaxis='y1'))
    fig_receita_ticket.add_trace(go.Scatter(x=df_receita['mes'], y=df_receita['ticket_medio'], mode='lines+markers', name="Ticket Médio", yaxis='y2'))

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

    return fig_cadastros, fig_pedidos, fig_receita_ticket

# Carregar os dados
df_kpis = load_kpis()

# Criar os gráficos
fig_cadastros, fig_pedidos, fig_receita_ticket = create_graphs(df_kpis)

# Criar o layout do dashboard usando o builder do Taipy
with tgb.Page() as page:
    # Título do dashboard
    tgb.text("# Dashboard de KPIs - teste")

    # Cards com os KPIs principais
    tgb.layout(
        [
            {"type": "text", "value": f"Total Cadastros: {df_kpis.shape[0]}"},
            {"type": "text", "value": f"Total Pedidos: {df_kpis['total_pedidos_faturados'].sum()}"},
            {"type": "text", "value": f"Ticket Médio: {df_kpis['ticket_medio'].mean():.2f}"}
        ],
        columns=3
    )

    # Filtro de data no canto superior direito
    tgb.input(name="data_pedido", label="Filtro de Data", position="right")

    # Gráficos
    tgb.chart(figure="{fig_cadastros}")
    tgb.chart(figure="{fig_pedidos}")
    tgb.chart(figure="{fig_receita_ticket}")

# Executar o dashboard
Gui(page).run(use_reloader=True)
