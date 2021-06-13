FROM apache/airflow:2.1.0-python3.6

ARG SPARK_HOME=/opt/spark

RUN pip install apache-airflow-providers-apache-spark pyspark==3.1.2

USER root

RUN apt update && apt install default-jdk scala git wget -y
RUN wget https://downloads.apache.org/spark/spark-3.1.2/spark-3.1.2-bin-hadoop3.2.tgz && tar -xvzf spark-*
RUN mv spark-3.1.2-bin-hadoop3.2 ${SPARK_HOME} && rm spark-3.1.2-bin-hadoop3.2.tgz

ENV SPARK_HOME=${SPARK_HOME}
ENV PATH=$PATH:${SPARK_HOME}/bin:${SPARK_HOME}/sbin
ENV PYSPARK_PYTHON=/usr/bin/python3
ENV SPARK_MASTER_WEBUI_PORT=8082
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64

RUN rm ${SPARK_HOME}/jars/guava-14.0.1.jar	

RUN curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.375/aws-java-sdk-bundle-1.11.375.jar \
    --output ${SPARK_HOME}/jars/aws-java-sdk-bundle-1.11.375.jar && \
    curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.2.0/hadoop-aws-3.2.0.jar \
    --output ${SPARK_HOME}/jars/hadoop-aws-3.2.0.jar && \
    curl https://repo1.maven.org/maven2/org/postgresql/postgresql/42.2.20/postgresql-42.2.20.jar \
    --output ${SPARK_HOME}/jars/postgresql-42.2.20.jar && \
    curl https://repo1.maven.org/maven2/com/google/guava/guava/30.1.1-jre/guava-30.1.1-jre.jar \
    --output ${SPARK_HOME}/jars/guava-30.1.1-jre.jar && \
    curl https://repo1.maven.org/maven2/net/java/dev/jets3t/jets3t/0.9.4/jets3t-0.9.4.jar \
    --output ${SPARK_HOME}/jars/jets3t-0.9.4.jar

# COPY pyspark_apps /opt/spark/pyspark_apps

RUN chown airflow ${SPARK_HOME} && chgrp airflow ${SPARK_HOME}
USER airflow
