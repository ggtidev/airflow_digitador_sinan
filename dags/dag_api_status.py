from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import os

# =========================================================
# CONFIGURAÇÕES
# =========================================================
DAG_ID = "0_api_sinan_status" # 0 para ficar no topo da lista
API_HEALTH_URL = "http://sinan-api:8000/notificacoes"

DEFAULT_ARGS = {
    "owner": "sinan",
    "depends_on_past": False,
    "retries": 0,
}

def check_api_health():
    """Verifica se a API está online e respondendo."""
    try:
        print(f"🔎 Verificando saúde da API em: {API_HEALTH_URL}")
        response = requests.get(API_HEALTH_URL, timeout=10)
        
        if response.status_code == 200:
            print("✅ API está ONLINE e respondendo corretamente!")
            print(f"Itens na fila: {len(response.json())}")
        else:
            raise RuntimeError(f"❌ API respondeu com status {response.status_code}")
            
    except Exception as e:
        raise RuntimeError(f"❌ Falha ao conectar na API: {e}")

# =========================================================
# DEFINIÇÃO DO DAG
# =========================================================
with DAG(
    dag_id=DAG_ID,
    description="Monitoramento da Disponibilidade da API Digitador",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule=None, # Apenas manual ou disparada por outras DAGs
    catchup=False,
    tags=["infra", "status", "api"],
) as dag:

    health_check = PythonOperator(
        task_id="check_api_health",
        python_callable=check_api_health,
    )
