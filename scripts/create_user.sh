airflow-init:
  image: apache/airflow:2.8.4
  depends_on:
    airflow-postgres:
      condition: service_healthy
  volumes:
    - ./dags:/opt/airflow/dags
    - ./projects:/opt/projects
    - ./requirements.txt:/requirements.txt
    - ./scripts:/opt/scripts
  environment:
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
  entrypoint: >
    bash -c "
    pip install --no-cache-dir -r /requirements.txt &&
    airflow db upgrade &&
    chmod +x /opt/scripts/create_user.sh &&
    /opt/scripts/create_user.sh
    "
