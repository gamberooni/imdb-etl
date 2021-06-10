FROM bitnami/spark:3.1.2

USER root
RUN rm /opt/bitnami/spark/jars/guava-14.0.1.jar	

RUN curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.375/aws-java-sdk-bundle-1.11.375.jar \
    --output /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.11.375.jar && \
    curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.2.0/hadoop-aws-3.2.0.jar \
    --output /opt/bitnami/spark/jars/hadoop-aws-3.2.0.jar && \
    curl https://repo1.maven.org/maven2/org/postgresql/postgresql/42.2.20/postgresql-42.2.20.jar \
    --output /opt/bitnami/spark/jars/postgresql-42.2.20.jar && \
    curl https://repo1.maven.org/maven2/com/google/guava/guava/30.1.1-jre/guava-30.1.1-jre.jar \
    --output /opt/bitnami/spark/jars/guava-30.1.1-jre.jar && \
    curl https://repo1.maven.org/maven2/net/java/dev/jets3t/jets3t/0.9.4/jets3t-0.9.4.jar \
    --output /opt/bitnami/spark/jars/jets3t-0.9.4.jar

USER 1001    