#!/usr/bin/env bash

AIRFLOW_HOME="/usr/local/airflow"
AIRFLOW__CORE__EXECUTOR="LocalExecutor"
AIRFLOW__CORE__FERNET_KEY="46BKJoQYlPPOexq0OhDZnIlNepKFf87WFwLbfzqDDho="
AIRFLOW__CORE__LOAD_EXAMPLES=False
JAVA_HOME="/usr"

export \
  AIRFLOW_HOME \
  AIRFLOW__CORE__EXECUTOR \
  AIRFLOW__CORE__FERNET_KEY \
  AIRFLOW__CORE__LOAD_EXAMPLES \
  JAVA_HOME

if [ -e "/usr/local/airflow/config/requirements.txt" ]; then
    $(command -v pip) install --user -r /usr/local/airflow/config/requirements.txt --no-deps
fi

airflow db init
airflow users create --username aulafia \
                     --password aulafia@123 \
                     --firstname Aula \
                     --lastname FIA \
                     --role Admin \
                     --email aulafia@fia.com
airflow connections import /usr/local/airflow/config/connections.json
airflow variables import /usr/local/airflow/config/variables.json
airflow scheduler & airflow webserver