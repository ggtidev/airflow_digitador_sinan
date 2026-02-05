"""
DAG: digitador_sinan_api

Responsável por:
1. Verificar estado do banco (Migrations).
2. Decidir fluxo:
   - Regra 01 (Sem Migrations): Gerar -> Aplicar -> Carga.
   - Regra 02 (Com Migrations): Carga (pula migrations).
3. Executar Carga de Dados (carga_violencia.py).
"""

# =========================================================
# IMPORTS
# =========================================================
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule

from datetime import datetime, timedelta
import subprocess
import os
import glob

# =========================================================
# CONFIGURAÇÕES
# =========================================================
DAG_ID = "digitador_sinan_api"

DEFAULT_ARGS = {
    "owner": "andre",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

BASE_PROJECT_PATH = "/opt/projects/sinan/2_digitador_sinan_api"
MIGRATIONS_DIR = f"{BASE_PROJECT_PATH}/migrations/versions"
ETL_SCRIPT = f"{BASE_PROJECT_PATH}/carga_violencia.py"

# Configuração de Variáveis de Ambiente Comuns (Docker)
def get_env_vars():
    env_vars = os.environ.copy()
    
    # Override para Docker -> Host / Containers
    env_vars["CONECTOR_DB_HOST"] = "host.docker.internal"
    env_vars["CONECTOR_DB_PORT"] = "5432"
    env_vars["CONECTOR_DB_NAME"] = "pg_redcap"
    env_vars["CONECTOR_DB_USER"] = "postgres"
    env_vars["CONECTOR_DB_PASSWORD"] = "root"

    env_vars["API_DB_HOST"] = "postgres_sinan"
    env_vars["API_DB_PORT"] = "5432"
    env_vars["API_DB_NAME"] = "sinan_api"
    env_vars["API_DB_USER"] = "postgres"
    env_vars["API_DB_PASSWORD"] = "postgres"
    
    return env_vars

# =========================================================
# FUNÇÕES EXECUTORAS
# =========================================================

def verificar_branch_migration(**context):
    """
    Decide qual caminho seguir:
    - Se não houver migrations: segue para 'gerar_migration' (Regra 01).
    - Se houver migrations: segue para 'executar_carga_violencia' (Regra 02).
    """
    print(f"🔎 Verificando migrações em: {MIGRATIONS_DIR}")
    
    # Lista arquivos .py na pasta versions (ignora __pycache__ e init)
    migracoes = glob.glob(f"{MIGRATIONS_DIR}/*.py")
    versao_arquivos = [f for f in migracoes if "__init__" not in f]

    if len(versao_arquivos) > 0:
        print(f"⚠️ Migrações já existem ({len(versao_arquivos)} arquivos found).")
        print(f"Arquivos: {[os.path.basename(f) for f in versao_arquivos]}")
        print("➡️ Decisão: Seguir para Regra 02 (Carga Direta).")
        return "executar_carga_violencia"
    
    print("✨ Nenhuma migração encontrada.")
    print("➡️ Decisão: Seguir para Regra 01 (Gerar Migration).")
    return "gerar_migration"


def gerar_migration_func(**context):
    """
    [Regra 01] Passo 1: Gera migration inicial
    """
    print("🛠️ Gerando migration inicial...")
    comando = ["alembic", "revision", "--autogenerate", "-m", "initial_migration_airflow"]
    
    resultado = subprocess.run(
        comando,
        cwd=BASE_PROJECT_PATH,
        env=get_env_vars(),
        capture_output=True,
        text=True
    )

    print("STDOUT:\n", resultado.stdout)
    print("STDERR:\n", resultado.stderr)

    if resultado.returncode != 0:
        raise RuntimeError(f"❌ Falha ao gerar migration. Código: {resultado.returncode}")
    print("✅ Migration gerada.")


def aplicar_migration_func(**context):
    """
    [Regra 01] Passo 2: Aplica upgrade no banco
    """
    print("🚀 Aplicando migrations (upgrade head)...")
    comando = ["alembic", "upgrade", "head"]
    
    resultado = subprocess.run(
        comando,
        cwd=BASE_PROJECT_PATH,
        env=get_env_vars(),
        capture_output=True,
        text=True
    )

    print("STDOUT:\n", resultado.stdout)
    print("STDERR:\n", resultado.stderr)

    if resultado.returncode != 0:
        raise RuntimeError(f"❌ Falha ao aplicar migrations. Código: {resultado.returncode}")
    print("✅ Banco atualizado.")


def executar_carga_violencia(**context):
    """
    [Regra 01 & 02] Executa a carga de dados.
    """
    print(f"➡️ Executando script de carga: {ETL_SCRIPT}")

    if not os.path.exists(ETL_SCRIPT):
         raise FileNotFoundError(f"❌ Script não encontrado: {ETL_SCRIPT}")

    comando = ["python", ETL_SCRIPT]
    
    resultado = subprocess.run(
        comando,
        env=get_env_vars(),
        capture_output=True,
        text=True
    )

    print("STDOUT:\n", resultado.stdout)
    print("STDERR:\n", resultado.stderr)

    if resultado.returncode != 0:
        raise RuntimeError(f"❌ Falha na execução da carga. Código: {resultado.returncode}")
    
    print("✅ Carga concluída com sucesso.")


# =========================================================
# DEFINIÇÃO DO DAG
# =========================================================
with DAG(
    dag_id=DAG_ID,
    description="Sinan API: Migrations Condicionais + Carga",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["sinan", "api", "branching"],
) as dag:

    inicio = EmptyOperator(task_id="inicio")

    # Decisor
    branch_task = BranchPythonOperator(
        task_id="verificar_estado_banco",
        python_callable=verificar_branch_migration,
    )

    # Regra 01: Tasks
    task_gerar_migration = PythonOperator(
        task_id="gerar_migration",
        python_callable=gerar_migration_func,
    )

    task_aplicar_migration = PythonOperator(
        task_id="aplicar_migration",
        python_callable=aplicar_migration_func,
    )

    # Regra 01 & 02: Carga
    # TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS garante que rode se vier 
    # da branch 'aplicar_migration' OU direto da branch 'verificar_estado_banco'
    task_carga_dados = PythonOperator(
        task_id="executar_carga_violencia",
        python_callable=executar_carga_violencia,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    fim = EmptyOperator(task_id="fim")

    # Fluxo
    inicio >> branch_task
    
    # Caminho Regra 01
    branch_task >> task_gerar_migration >> task_aplicar_migration >> task_carga_dados
    
    # Caminho Regra 02 (Pula migrations direto para carga)
    branch_task >> task_carga_dados
    
    task_carga_dados >> fim
