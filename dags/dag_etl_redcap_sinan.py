"""
DAG: etl_redcap_sinan

Responsável apenas pelo ETL do REDCAP.
Pode rodar isolado ou ser reutilizado por outros DAGs.
"""

# =========================================================
# IMPORTS (Airflow 2.8+)
# =========================================================
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
import subprocess
import os

# =========================================================
# CONFIGURAÇÕES GERAIS
# =========================================================
DAG_ID = "etl_redcap_sinan"

DEFAULT_ARGS = {
    "owner": "andre",
    "depends_on_past": False,
    "retries": 3,                          # 🔁 retry automático
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(hours=1),
}

# Caminho do projeto montado no container
BASE_PROJECT_PATH = "/opt/projects/sinan"

# Script ETL REDCAP
ETL_REDCAP_SCRIPT = (
    f"{BASE_PROJECT_PATH}/1_etl_redcap_sinan/redcap.py"
)

# =========================================================
# FUNÇÃO EXECUTORA
# =========================================================
def executar_etl_redcap():
    """
    Executa o ETL do REDCAP.
    O Airflow controla retry, timeout e falha.
    """

    if not os.path.exists(ETL_REDCAP_SCRIPT):
        raise FileNotFoundError(
            f"Script ETL não encontrado: {ETL_REDCAP_SCRIPT}"
        )

    # Comando direto (SEM venv)
    comando = [
        "python",
        ETL_REDCAP_SCRIPT
    ]

    resultado = subprocess.run(
        comando,
        capture_output=True,
        text=True,
    )

    # Log explícito (aparece no Airflow UI)
    print("STDOUT:", resultado.stdout)
    print("STDERR:", resultado.stderr)

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
    schedule=None,          # manual (ideal para ETL)
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
