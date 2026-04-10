import os
from flask import Flask, jsonify
from flask_cors import CORS # Necessário para permitir o acesso do seu arquivo HTML
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Carrega as variáveis de ambiente (necessário criar um arquivo .env)
load_dotenv() 

app = Flask(__name__)
# Habilita CORS (Cross-Origin Resource Sharing) para que o navegador 
# permita que o arquivo HTML (executado localmente ou em outro domínio) 
# acesse esta API.
CORS(app) 

# --- Configuração do Banco de Dados ---
# Usa os dados de acesso fornecidos:
DB_HOST = os.getenv('API_DB_HOST', 'localhost')
DB_PORT = os.getenv('API_DB_PORT', '5433')
DB_NAME = os.getenv('API_DB_NAME', 'sinan_api')
DB_USER = os.getenv('API_DB_USER', 'postgres')
DB_PASSWORD = os.getenv('API_DB_PASSWORD', 'postgres')

# URL de conexão no formato esperado pelo SQLAlchemy (PostgreSQL + psycopg2)
DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

try:
    # Cria o objeto de Engine de Conexão
    engine = create_engine(DATABASE_URL)
    print("✅ Conexão com o Banco de Dados configurada com sucesso.")
except Exception as e:
    print(f"❌ Erro ao configurar o Engine do Banco de Dados: {e}")
    # O aplicativo pode continuar, mas as rotas que acessam o BD falharão.

# --- Endpoint da API ---

@app.route('/api/notificacoes', methods=['GET'])
def get_notificacoes():
    """
    Consulta a tabela rpa_notificacoes e retorna os dados em formato JSON.
    """
    try:
        # CORREÇÃO CRÍTICA: Qualificar o nome da tabela com o esquema 'sinan_api'
        # O DBeaver mostra que a tabela está em 'sinan_api.rpa_notificacoes'
        sql_query = text("SELECT id, num_notificacao, record, status, agravo_id FROM rpa_notificacoes order by id asc;")
        
        # Abre uma conexão e executa a consulta
        with engine.connect() as connection:
            result = connection.execute(sql_query)
            
            # Obtém os nomes das colunas
            columns = result.keys()
            
            # Mapeia cada linha para um dicionário
            data = [dict(zip(columns, row)) for row in result]

        # Retorna a lista de dicionários como resposta JSON
        return jsonify(data)

    except Exception as e:
        print(f"❌ Erro ao executar a consulta ou conectar ao DB: {e}")
        # Retorna um erro 500 (Internal Server Error)
        return jsonify({"error": "Falha ao buscar dados do banco de dados", "details": str(e)}), 500

# --- Execução do Servidor ---

if __name__ == '__main__':
    # O servidor será executado na porta 5000 por padrão.
    # O CORS já está configurado para aceitar requisições de outras origens.
    print("\nIniciando servidor Flask...")
    print("Acesse http://127.0.0.1:5000/api/notificacoes para ver o JSON.")
    
    # Executa em modo de depuração para facilitar o desenvolvimento
    app.run(debug=True)