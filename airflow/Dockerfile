FROM apache/airflow:2.1.0

RUN pip install apache-airflow-providers-apache-spark

USER root

WORKDIR /opt/spark
RUN apt update && apt install default-jdk scala git -y
RUN wget https://downloads.apache.org/spark/spark-3.1.2/spark-3.1.2-bin-hadoop3.2.tgz && tar -xvzf spark-*
RUN mv spark-3.1.2-bin-hadoop3.2 /opt/spark && rm spark-3.1.2-bin-hadoop3.2.tgz
RUN echo "export SPARK_HOME=/opt/spark" >> ~/.profile && \
    echo "export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin" >> ~/.profile && \
    echo "export PYSPARK_PYTHON=/usr/bin/python3" >> ~/.profile && \
    echo "export SPARK_MASTER_WEBUI_PORT=8082" >> ~/.profile
RUN source ~/.profile

COPY pyspark_apps .

RUN rm /opt/bitnami/spark/jars/guava-14.0.1.jar	

RUN curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.375/aws-java-sdk-bundle-1.11.375.jar \
    --output /spark/jars/aws-java-sdk-bundle-1.11.375.jar && \
    curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.2.0/hadoop-aws-3.2.0.jar \
    --output /spark/jars/hadoop-aws-3.2.0.jar && \
    curl https://repo1.maven.org/maven2/org/postgresql/postgresql/42.2.20/postgresql-42.2.20.jar \
    --output /spark/jars/postgresql-42.2.20.jar && \
    curl https://repo1.maven.org/maven2/com/google/guava/guava/30.1.1-jre/guava-30.1.1-jre.jar \
    --output /spark/jars/guava-30.1.1-jre.jar && \
    curl https://repo1.maven.org/maven2/net/java/dev/jets3t/jets3t/0.9.4/jets3t-0.9.4.jar \
    --output /spark/jars/jets3t-0.9.4.jar

RUN chown airflow /spark && chgrp airflow /spark

ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64

USER airflow