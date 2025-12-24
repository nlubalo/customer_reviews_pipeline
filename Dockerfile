FROM apache/airflow:2.9.3-python3.12

USER root

RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    procps \
    && apt-get clean

# Dynamically resolve JAVA_HOME (works for arm64 & amd64)
RUN JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java)))) && \
    echo "JAVA_HOME=${JAVA_HOME}" >> /etc/environment && \
    echo "export JAVA_HOME=${JAVA_HOME}" > /etc/profile.d/java.sh

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
