#!/usr/bin/env bash
#🔐 Permissão (obrigatório no Windows + WSL / Git Bash):
#chmod +x scripts/airflow-init.sh
set -e

echo "📦 Instalando dependências Python"
pip install --no-cache-dir -r /requirements.txt

echo "🗄️ Migrando banco do Airflow"
airflow db migrate

echo "👤 Verificando se usuário admin existe"

if airflow users list | grep -q "^.*| *${AIRFLOW_ADMIN_USERNAME} *|"; then
  echo "✅ Usuário '${AIRFLOW_ADMIN_USERNAME}' já existe. Pulando criação."
else
  echo "➕ Criando usuário '${AIRFLOW_ADMIN_USERNAME}'"
  airflow users create \
    --username "${AIRFLOW_ADMIN_USERNAME}" \
    --password "${AIRFLOW_ADMIN_PASSWORD}" \
    --firstname "${AIRFLOW_ADMIN_FIRSTNAME}" \
    --lastname "${AIRFLOW_ADMIN_LASTNAME}" \
    --role Admin \
    --email "${AIRFLOW_ADMIN_EMAIL}"
fi

echo "✅ Airflow inicializado com sucesso"
