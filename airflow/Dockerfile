FROM apache/airflow:2.7.1-python3.11

USER root
RUN apt-get update && \
    apt-get install -y gcc python3-dev openjdk-11-jdk && \
    apt-get clean

# WORKDIR /opt/airflow/jars

RUN mkdir -p /opt/airflow/jars

RUN curl -o /opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
RUN curl -o /opt/airflow/jars/commons-pool2-2.12.0.jar https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar
RUN curl -o /opt/airflow/jars/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
RUN curl -o /opt/airflow/jars/iceberg-spark-runtime-3.5_2.12-1.4.3.jar https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.3/iceberg-spark-runtime-3.5_2.12-1.4.3.jar
RUN curl -o /opt/airflow/jars/kafka-clients-3.6.1.jar https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.6.1/kafka-clients-3.6.1.jar
RUN curl -o /opt/airflow/jars/mysql-connector-j-8.3.0.jar https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.3.0/mysql-connector-j-8.3.0.jar
RUN curl -o /opt/airflow/jars/nessie-spark-extensions-3.5_2.12-0.76.3.jar https://repo1.maven.org/maven2/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.5_2.12/0.76.3/nessie-spark-extensions-3.5_2.12-0.76.3.jar
RUN curl -o /opt/airflow/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar
RUN curl -o /opt/airflow/jars/spark-streaming-kafka-0-10-assembly_2.12-3.5.0.jar https://repo1.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-10-assembly_2.12/3.5.0/spark-streaming-kafka-0-10-assembly_2.12-3.5.0.jar

# Set JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

USER airflow

RUN pip install apache-airflow apache-airflow-providers-apache-spark pyspark