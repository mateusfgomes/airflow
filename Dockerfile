FROM apache/airflow:2.10.2
USER root
RUN apt-get update
RUN apt install -y default-jdk
RUN apt-get autoremove -yqq --purge
RUN apt-get clean
RUN rm -rf /var/lib/apt/lists/*
RUN mkdir -p datalake/bronze && \
    chmod -R 775 datalake/bronze

USER airflow
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

ADD requirements.txt .
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt
