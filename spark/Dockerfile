FROM bitnami/spark:latest

USER root

RUN apt-get update && apt-get install -y curl

RUN curl -O https://repo1.maven.org/maven2/com/microsoft/azure/azure-eventhubs-spark_2.12/2.3.22/azure-eventhubs-spark_2.12-2.3.22.jar \
    && mv azure-eventhubs-spark_2.12-2.3.22.jar /opt/bitnami/spark/jars
