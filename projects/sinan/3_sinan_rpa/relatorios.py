import pandas as pd
import os
from sqlalchemy import create_engine
from datetime import datetime
from logger import log_info, log_erro

# Configurações do Banco (Ajuste conforme seu .env da API)
DB_USER = "postgres"
DB_PASS = "postgres"
DB_HOST = "localhost" # ou o IP do servidor onde está o banco
DB_PORT = "5433"      # Porta configurada no docker-compose da API
DB_NAME = "sinan_api"

# Conexão DB API (sinan_api)
#API_DB_HOST=localhost
#API_DB_PORT=5433
#API_DB_NAME=sinan_api
#API_DB_USER=postgres
#API_DB_PASSWORD=postgres


def gerar_relatorio_final():
    """
    Conecta ao banco, busca a tabela rpa_notificacao e gera um Excel com o status atual.
    """
    log_info("Gerando relatório final de execução em Excel...")
    
    try:
        # String de conexão (SQLAlchemy)
        connection_string = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(connection_string)

        # Query para buscar os dados
        query = "SELECT * FROM public.rpa_notificacoes" # Ajuste se o nome da tabela for diferente
        
        # Lê o SQL direto para um DataFrame
        df = pd.read_sql(query, engine)

        # Define o caminho do arquivo (Pasta de downloads ou do projeto)
        pasta_relatorios = os.path.abspath(os.path.join(os.path.dirname(__file__), "relatorios"))
        os.makedirs(pasta_relatorios, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        arquivo_excel = os.path.join(pasta_relatorios, f"Relatorio_Status_SINAN_{timestamp}.xlsx")

        # Exporta para Excel
        df.to_excel(arquivo_excel, index=False)
        
        log_info(f"✅ Relatório gerado com sucesso: {arquivo_excel}")
        
    except Exception as e:
        log_erro(f"Erro ao gerar relatório Excel: {e}")