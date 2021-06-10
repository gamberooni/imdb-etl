FROM apache/airflow:2.1.0

RUN pip install apache-airflow-providers-apache-spark

USER root

RUN apt update && apt install default-jdk scala git -y
RUN java -version; javac -version; scala -version; git --version

RUN mkdir -p /pyspark_apps
COPY pyspark_apps/ /pyspark_apps
RUN chown airflow /pyspark_apps && \
    chgrp airflow /pyspark_apps

ENV PYSPARK_HOME /pyspark_apps   

USER airflow