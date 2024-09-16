from taipy.gui import Gui
from dashboard.dashboard import dashboard_md

# Definir as páginas - no seu caso, apenas uma página
pages = {
    '/': dashboard_md,  # A página principal do dashboard
}

if __name__ == '__main__':
    # Iniciar o GUI do Taipy e rodar o projeto
    gui = Gui(pages=pages)

    # Rodar o core do Taipy (se necessário)
    gui.run(title="Dashboard de KPIs", use_reloader=True)
