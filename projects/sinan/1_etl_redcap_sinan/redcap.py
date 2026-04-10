import os
import requests
import psycopg2
# [Joao]->PERGUNTAR PARA ANDERSON COMO ELE RODARIA EM QUANTO EM QUANTO TEMPO O SCRIPT DO REDCAP.PY (ESSE ARQUIVO). JUNTAMENTE COM O SCRIPT 
# [Joao]->Rodar diariamente ( todos os dias) as 19:00 da manhã, para garantir que os dados estejam sempre atualizados.
# [Joao]->DO carga_violencia.py DA API (OUTRO PROJETO) QUE É RESPONSAVEL POR CARREGAR OS DADOS DO REDCAP PARA O BANCO DA API.

# ==============================
# VARIÁVEIS DE AMBIENTE (AIRFLOW)
# ==============================
REDCAP_API = os.environ["REDCAP_API"]
REDCAP_TOKEN = os.environ["REDCAP_TOKEN"]

DB_HOST = os.environ["DB_HOST"]          # airflow-postgres
DB_PORT = os.environ.get("DB_PORT", 5432)
DB_NAME = os.environ["DB_NAME"]
DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]

# ==============================
# FUNÇÕES
# ==============================
def getRespostasRedCap(token):
    payload = {
        "token": token,
        "content": "record",
        "action": "export",
        "format": "json",
        "type": "eav",
        "returnFormat": "json",
    }

    response = requests.post(REDCAP_API, data=payload)
    response.raise_for_status()
    return response.json()


def salvar_no_postgres(data):
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS redcap_respostas (
            record TEXT,
            field_name TEXT,
            value TEXT
        )
    """)

    cur.execute("TRUNCATE TABLE redcap_respostas")

    for row in data:
        cur.execute(
            """
            INSERT INTO redcap_respostas (record, field_name, value)
            VALUES (%s, %s, %s)
            """,
            (row["record"], row["field_name"], row["value"]),
        )

    conn.commit()
    cur.close()
    conn.close()


def run():
    data = getRespostasRedCap(REDCAP_TOKEN)
    salvar_no_postgres(data)


if __name__ == "__main__":
    run()
