
import os
import sys
from sqlalchemy import create_engine, inspect
import glob

# Mock environment
os.environ["API_DB_HOST"] = "localhost" # Assuming port mapping or local connectivity
os.environ["API_DB_PORT"] = "5432" # Adjust if needed
os.environ["API_DB_NAME"] = "sinan_api"
os.environ["API_DB_USER"] = "postgres"
os.environ["API_DB_PASSWORD"] = "postgres"

MIGRATIONS_DIR = "/opt/projects/sinan/2_digitador_sinan_api/migrations/versions"
# Mock glob to simulate file existence
def mock_glob(pattern):
    return ["/path/to/migration_123.py"]

# Original logic adapted for testing
def verificar_branch_migration_test():
    print(f"🔎 Verificando estado do banco e migrações...")
    
    # 1. Verification Logic
    db_url = f"postgresql+psycopg2://{os.environ['API_DB_USER']}:{os.environ['API_DB_PASSWORD']}@{os.environ['API_DB_HOST']}:{os.environ['API_DB_PORT']}/{os.environ['API_DB_NAME']}"
    
    tables = []
    try:
        # Assuming we can't actually connect in this test environment without the docker network,
        # we act as if connection failed or tables are empty
        print("Simulating DB connection...")
        # engine = create_engine(db_url)
        # tables = inspect(engine).get_table_names()
        raise Exception("Simulation: DB unreachable or empty")
    except Exception as e:
        print(f"⚠️ Erro ao conectar/listar tabelas: {e}")
        pass

    if 'rpa_notificacao' in tables:
        print("✅ Tabela encontrada.")
        return "executar_carga_violencia"

    print("⚠️ Tabela NÃO encontrada.")
    
    # 2. Check Migrations
    migracoes = mock_glob(f"{MIGRATIONS_DIR}/*.py")
    versao_arquivos = [f for f in migracoes if "__init__" not in f]

    if len(versao_arquivos) > 0:
        print(f"⚠️ Migrações já existem ({len(versao_arquivos)} arquivos found).")
        return "aplicar_migration"
    
    return "gerar_migration"

result = verificar_branch_migration_test()
print(f"RESULTADO: {result}")
