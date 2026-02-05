"""
DAG: etl_redcap_sinan

Responsável por executar o ETL do REDCAP → Banco SINAN.
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
DAG_ID = "etl_redcap_sinan"

DEFAULT_ARGS = {
    "owner": "andre",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(hours=1),
}

BASE_PROJECT_PATH = "/opt/projects/sinan"
ETL_REDCAP_SCRIPT = f"{BASE_PROJECT_PATH}/1_etl_redcap_sinan/redcap.py"

# =========================================================
# FUNÇÃO EXECUTORA
# =========================================================
def executar_etl_redcap():
    """
    Executa o ETL REDCAP validando variáveis de ambiente.
    """

    # 🔎 Validação explícita das variáveis
    required_envs = [
        "REDCAP_API",
        "REDCAP_TOKEN",
        "DB_HOST",
        "DB_PORT",
        "DB_NAME",
        "DB_USER",
        "DB_PASSWORD",
    ]

    for var in required_envs:
        if var not in os.environ:
            raise EnvironmentError(f"❌ Variável de ambiente ausente: {var}")

    if not os.path.exists(ETL_REDCAP_SCRIPT):
        raise FileNotFoundError(
            f"❌ Script ETL não encontrado: {ETL_REDCAP_SCRIPT}"
        )

    print("✅ Variáveis de ambiente carregadas com sucesso")
    print(f"➡️ Executando: {ETL_REDCAP_SCRIPT}")

    comando = ["python", ETL_REDCAP_SCRIPT]

    resultado = subprocess.run(
        comando,
        capture_output=True,
        text=True,
    )

    print("STDOUT:\n", resultado.stdout)
    print("STDERR:\n", resultado.stderr)

    if resultado.returncode != 0:
        raise RuntimeError("❌ Falha na execução do ETL REDCAP")

# =========================================================
# DEFINIÇÃO DO DAG
# =========================================================
with DAG(
    dag_id=DAG_ID,
    description="ETL REDCAP → Banco (SINAN)",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["sinan", "etl", "redcap"],
) as dag:

    inicio = EmptyOperator(task_id="inicio")

    etl_redcap = PythonOperator(
        task_id="etl_redcap",
        python_callable=executar_etl_redcap,
    )

    fim = EmptyOperator(task_id="fim")

    inicio >> etl_redcap >> fim