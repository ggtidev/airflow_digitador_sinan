"""
DAG: digitador_sinan_pipeline

Pipeline SINAN:
1. ETL REDCAP
2. Carga API
3. RPA SINAN

✔ Compatível com Airflow 2.8+
✔ Retry automático
✔ Execução isolada por task
✔ Seguro contra Broken DAG
"""
# =====================================================================
# Versão inicial que tentada rodar os 3 projetos.
# =====================================================================
__version__ = "1.0.0"

# =====================================================================
# IMPORTS OBRIGATÓRIOS (NUNCA use airflow.sdk)
# =====================================================================

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule

from datetime import datetime, timedelta
import subprocess
import os
import sys

# =====================================================================
# CONFIGURAÇÕES GERAIS
# =====================================================================

DAG_ID = "digitador_sinan_pipeline"

DEFAULT_ARGS = {
    "owner": "andre",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(hours=2),
}

# ⚠️ IMPORTANTE:
# Nunca use paths do Windows aqui
BASE_PROJECT_PATH = "/opt/projects/sinan"

ETL_SCRIPT = f"{BASE_PROJECT_PATH}/1_etl_redcap_sinan/redcap.py"
API_SCRIPT = f"{BASE_PROJECT_PATH}/2_digitador_sinan_api/carga_violencia.py"
RPA_SCRIPT = f"{BASE_PROJECT_PATH}/3_sinan_rpa/main.py"

VENV_ACTIVATE = f"{BASE_PROJECT_PATH}/.venv/bin/activate"

# =====================================================================
# FUNÇÃO AUXILIAR PARA EXECUTAR SCRIPTS PYTHON
# =====================================================================

def executar_script(script_path: str, **context):
    """
    Executa um script Python externo usando subprocess.
    Se falhar, o Airflow trata como erro e aplica retry.
    """

    if not os.path.exists(script_path):
        raise FileNotFoundError(f"Script não encontrado: {script_path}")

    comando = f"""
    set -e
    source {VENV_ACTIVATE}
    python {script_path}
    """

    resultado = subprocess.run(
        comando,
        shell=True,
        executable="/bin/bash",
        capture_output=True,
        text=True,
    )

    if resultado.returncode != 0:
        raise RuntimeError(
            f"Erro ao executar {script_path}\nSTDOUT:\n{resultado.stdout}\nSTDERR:\n{resultado.stderr}"
        )

# =====================================================================
# CONTROLE DE EXECUÇÃO (ETL / API / RPA / FULL)
# =====================================================================

def pode_executar(etapa: str) -> bool:
    """
    Controla execução via Airflow Variable:
    SINAN_MODO_EXECUCAO = FULL | ETL | API | RPA
    """
    modo = Variable.get("SINAN_MODO_EXECUCAO", default_var="FULL").upper()
    return modo in ("FULL", etapa)

# =====================================================================
# DEFINIÇÃO DO DAG
# =====================================================================

with DAG(
    dag_id=DAG_ID,
    description="Pipeline SINAN: Redcap → API → RPA",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["sinan", "dev"],
) as dag:

    # --------------------------------------------------------------
    # MARCADORES
    # --------------------------------------------------------------

    inicio = EmptyOperator(task_id="inicio")
    fim = EmptyOperator(
        task_id="fim",
        trigger_rule=TriggerRule.ALL_DONE
    )

    # --------------------------------------------------------------
    # ETL REDCAP
    # --------------------------------------------------------------

    etl_redcap = PythonOperator(
        task_id="etl_redcap",
        python_callable=executar_script,
        op_kwargs={"script_path": ETL_SCRIPT},
    )

    # --------------------------------------------------------------
    # CARGA API
    # --------------------------------------------------------------

    carga_api = PythonOperator(
        task_id="carga_api",
        python_callable=executar_script,
        op_kwargs={"script_path": API_SCRIPT},
    )

    # --------------------------------------------------------------
    # RPA SINAN
    # --------------------------------------------------------------

    rpa_sinan = PythonOperator(
        task_id="rpa_sinan",
        python_callable=executar_script,
        op_kwargs={"script_path": RPA_SCRIPT},
    )

    # --------------------------------------------------------------
    # ORQUESTRAÇÃO
    # --------------------------------------------------------------

    inicio >> etl_redcap >> carga_api >> rpa_sinan >> fim
