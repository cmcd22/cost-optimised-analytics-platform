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

# Add Hadoop AWS support
RUN curl -fL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    -o /opt/spark/jars/hadoop-aws-3.3.4.jar && \
    curl -fL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

USER airflow
