"""
DAG: digitador_sinan_api

Responsável por executar o ETL do Conector (Redcap) → Banco SINAN API.
Executa o script carga_violencia.py.
"""

# =========================================================
# IMPORTS
# =========================================================
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
import subprocess
import os

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

BASE_PROJECT_PATH = "/opt/projects/sinan"
ETL_SCRIPT = f"{BASE_PROJECT_PATH}/2_digitador_sinan_api/carga_violencia.py"
ENV_FILE = f"{BASE_PROJECT_PATH}/2_digitador_sinan_api/.env"

# =========================================================
# FUNÇÃO EXECUTORA
# =========================================================
def executar_carga_violencia(**context):
    """
    Executa o script carga_violencia.py configurando as variáveis de ambiente.
    """
    print(f"➡️ Preparando execução: {ETL_SCRIPT}")

    if not os.path.exists(ETL_SCRIPT):
         raise FileNotFoundError(f"❌ Script não encontrado: {ETL_SCRIPT}")

    # Configuração de Ambiente para o Script
    # Sobrescrevemos os hosts para garantir conectividade Docker
    env_vars = os.environ.copy()
    
    # Vars Específicas do Projeto (Baseado no .env do projeto 2_digitador_sinan_api)
    # Mas injetamos aqui para garantir override seguro
    env_vars["CONECTOR_DB_HOST"] = "host.docker.internal" # Banco Redcap (Origem)
    env_vars["CONECTOR_DB_PORT"] = "5432"
    env_vars["CONECTOR_DB_NAME"] = "pg_redcap"
    env_vars["CONECTOR_DB_USER"] = "postgres"
    env_vars["CONECTOR_DB_PASSWORD"] = "root"

    env_vars["API_DB_HOST"] = "postgres_sinan" # Banco API (Destino) - Nome do Serviço Docker
    env_vars["API_DB_PORT"] = "5432" # Porta interna do container
    env_vars["API_DB_NAME"] = "sinan_api"
    env_vars["API_DB_USER"] = "postgres"
    env_vars["API_DB_PASSWORD"] = "postgres"

    print("✅ Variáveis de ambiente configuradas.")

    # Execução do Script
    comando = ["python", ETL_SCRIPT]
    
    print(f"🚀 Iniciando subprocesso: {' '.join(comando)}")
    resultado = subprocess.run(
        comando,
        env=env_vars,
        capture_output=True,
        text=True
    )

    print("STDOUT:\n", resultado.stdout)
    print("STDERR:\n", resultado.stderr)

    if resultado.returncode != 0:
        raise RuntimeError(f"❌ Falha na execução do script. Código: {resultado.returncode}")
    
    print("✅ Execução concluída com sucesso.")

# =========================================================
# DEFINIÇÃO DO DAG
# =========================================================
with DAG(
    dag_id=DAG_ID,
    description="Carga Dados: Conector → API Sinan (carga_violencia.py)",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule=None, # Execução Manual ou Trigger
    catchup=False,
    tags=["sinan", "api", "etl", "violencia"],
) as dag:

    inicio = EmptyOperator(task_id="inicio")

    carga_dados = PythonOperator(
        task_id="executar_carga_violencia",
        python_callable=executar_carga_violencia,
    )

    fim = EmptyOperator(task_id="fim")

    inicio >> carga_dados >> fim
