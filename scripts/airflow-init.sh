#!/usr/bin/env bash
#ðŸ” PermissÃ£o (obrigatÃ³rio no Windows + WSL / Git Bash):
#chmod +x scripts/airflow-init.sh
set -e

echo "ðŸ“¦ Instalando dependÃªncias Python"
pip install --no-cache-dir -r /requirements.txt

echo "ðŸ—„ï¸ Migrando banco do Airflow"
airflow db migrate

echo "ðŸ‘¤ Verificando se usuÃ¡rio admin existe"

if airflow users list | grep -q "^.*| *${AIRFLOW_ADMIN_USERNAME} *|"; then
  echo "âœ… UsuÃ¡rio '${AIRFLOW_ADMIN_USERNAME}' jÃ¡ existe. Pulando criaÃ§Ã£o."
else
  echo "âž• Criando usuÃ¡rio '${AIRFLOW_ADMIN_USERNAME}'"
  airflow users create \
    --username "${AIRFLOW_ADMIN_USERNAME}" \
    --password "${AIRFLOW_ADMIN_PASSWORD}" \
    --firstname "${AIRFLOW_ADMIN_FIRSTNAME}" \
    --lastname "${AIRFLOW_ADMIN_LASTNAME}" \
    --role Admin \
    --email "${AIRFLOW_ADMIN_EMAIL}"
fi

echo "âœ… Airflow inicializado com sucesso"
