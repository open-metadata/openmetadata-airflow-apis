FROM python:3.9-slim as airflow
ENV AIRFLOW_HOME=/airflow

RUN apt-get update && \
    apt-get install -y gcc libsasl2-dev curl build-essential libssl-dev libffi-dev librdkafka-dev unixodbc-dev python3.9-dev libevent-dev wget --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*

ENV AIRFLOW_VERSION=2.1.4
ENV CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.9.txt"
# Add docker provider for the DockerOperator
RUN pip install "apache-airflow[docker]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
RUN pip install pymysql openmetadata-ingestion

FROM airflow
WORKDIR /app

COPY src/ ./src
COPY setup.py .
COPY README.md .

RUN pip install .

COPY /examples/airflow.cfg /airflow/airflow.cfg

RUN cp -r ./src/plugins /airflow/plugins
RUN cp -r ./src/plugins/dag_templates /airflow/

RUN mkdir -p /airflow/dag_generated_configs

EXPOSE 8080

COPY scripts/ingestion_dependency.sh .
RUN chmod 755 ingestion_dependency.sh
CMD [ "./ingestion_dependency.sh" ]
