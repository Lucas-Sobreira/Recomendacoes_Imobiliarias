FROM apache/airflow:2.3.3

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
           libaio1 \
           wget \
           unzip \
           vim \
           telnet \
           curl \
           iputils-ping \
           systemctl \
           default-jdk \
           ca-certificates \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir /usr/local/airflow \
    && ldconfig

ARG AIRFLOW_USER_HOME=/usr/local/airflow
ENV AIRFLOW_HOME=${AIRFLOW_USER_HOME}

RUN chown -R airflow: ${AIRFLOW_HOME}

USER airflow

RUN python -m pip install --upgrade pip && \
    pip install pyspark==3.2.1 && \
    pip install apache-airflow-providers-apache-spark==4.0.0 && \
    pip install s3fs==0.4.2

ENV FLASK_APP="airflow.www.app flask fab create-admin"

COPY ./entrypoint.sh /entrypoint.sh
COPY ./config/airflow.cfg ${AIRFLOW_HOME}/airflow.cfg

USER root
RUN chmod 777 /entrypoint.sh

USER airflow
WORKDIR ${AIRFLOW_HOME}

ENTRYPOINT ["/entrypoint.sh"]
