FROM ubuntu:18.04

ARG SPARK_HOME=/opt/spark

RUN apt update && apt install -y \
    openjdk-8-jdk \
    scala \
    git \
    wget \
    curl

RUN wget -q https://downloads.apache.org/spark/spark-3.1.2/spark-3.1.2-bin-hadoop3.2.tgz && \
    tar -xvzf spark-* && \
    mv spark-3.1.2-bin-hadoop3.2 ${SPARK_HOME}

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

ENV SPARK_HOME=${SPARK_HOME}
ENV PATH=$PATH:${SPARK_HOME}/bin:${SPARK_HOME}/sbin
ENV PYSPARK_PYTHON=/usr/bin/python3
ENV SPARK_WORKER_CORES=6
ENV SPARK_WORKER_MEMORY=8g
ENV SPARK_WORKER_WEBUI_PORT=8081
ENV SPARK_WORKER_LOG ${SPARK_HOME}/worker/logs
ENV SPARK_MASTER spark://spark:7077

WORKDIR ${SPARK_HOME}
COPY entrypoint.sh entrypoint.sh
EXPOSE 8081

ENTRYPOINT [ "/bin/bash" ]
CMD [ "entrypoint.sh" ]