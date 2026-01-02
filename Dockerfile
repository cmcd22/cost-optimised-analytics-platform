FROM apache/airflow:2.7.3-python3.8

USER root

# Install Java + curl
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Spark (stable Apache archive)
RUN curl -fL https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz -o /tmp/spark.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt && \
    mv /opt/spark-3.5.0-bin-hadoop3 /opt/spark && \
    rm /tmp/spark.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

USER airflow
