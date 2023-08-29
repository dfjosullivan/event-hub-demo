FROM bitnami/spark:latest

USER root

RUN apt-get update && apt-get install -y curl

RUN curl -O https://repo1.maven.org/maven2/com/microsoft/azure/azure-eventhubs-spark_2.12/2.3.22/azure-eventhubs-spark_2.12-2.3.22.jar \
    && mv azure-eventhubs-spark_2.12-2.3.22.jar /opt/bitnami/spark/jars

RUN curl -O https://repo1.maven.org/maven2/com/microsoft/azure/azure-eventhubs/3.3.0/azure-eventhubs-3.3.0.jar \
    && mv azure-eventhubs-3.3.0.jar /opt/bitnami/spark/jars

RUN curl -O https://repo1.maven.org/maven2/org/scala-lang/modules/scala-java8-compat_3/1.0.2/scala-java8-compat_3-1.0.2.jar \
    && mv scala-java8-compat_3-1.0.2.jar /opt/bitnami/spark/jars

RUN curl -O https://repo1.maven.org/maven2/org/apache/qpid/proton-j/0.34.1/proton-j-0.34.1.jar \
    && mv proton-j-0.34.1.jar /opt/bitnami/spark/jars

#RUN curl -O https://repo1.maven.org/maven2/org/elasticsearch/elasticsearch-hadoop/8.9.1/elasticsearch-hadoop-8.9.1.jar \
#    && mv elasticsearch-hadoop-8.9.1.jar /opt/bitnami/spark/jars

RUN curl -O https://repo1.maven.org/maven2/org/elasticsearch/elasticsearch-spark-30_2.12/8.9.1/elasticsearch-spark-30_2.12-8.9.1.jar \
    && mv elasticsearch-spark-30_2.12-8.9.1.jar /opt/bitnami/spark/jars