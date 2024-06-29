# On-Premises Batch and Stream Data Processing build on Docker Project
## Data Architect
![image](https://github.com/tma-Jon/On-premises-end-to-end-data-project/assets/105640122/7538e543-54b1-4645-995b-eb5449477bb8)

- MySQL as data source with binary log enabled (localhost:3306)
- Spark cluster (standalone) as distributed data processing, read batch data directly from MySQL (localhost:9090)
- Minio as object storage, store data which from Spark (localhost:9001)
- Iceberg as data lakehouse format table
- Nessie as data catalog for Iceberg (localhost:19120)
- Mongodb as backed storage for Nessie, store metadata
- Mongodb Express as UI for Mongodb (localhost:8082)
- Airflow as orchestration tool, submit PySpark batch and streaming job to Spark master (localhost:8080)
- PostgreSQL as backed storage for Airflow, store metadata
- Dremio as data LakeHouse query engine, query data from Minio using Nessie catalog (localhost:9047)
- Debezium as data change capture tool, read binary log from MySQL then push to Redpanda topics (localhost:8085)
- Redpanda as Pub/Sub message queue (like Kafka), store change data from MySQL which pushed by Debezium, Spark streaming job read data from topics of Redpanda (localhost:8084)
- Streamlit as Dashboard, using REST API of Dremio to get data and then visualize (localhost:8501)

### 1. Docker basic learning
  - Install WSL version 2 and Docker Desktop (for Windows)
  https://docs.docker.com/desktop/install/windows-install/
  - In the folder C:\Users\<your-user>, create .wslconfig and write the number of memory that you want to allocate for the wsl2 system

![image](https://github.com/tma-Jon/On-prem-batch-data-project/assets/105640122/a80695c5-945f-4295-90de-259068b24473)

  - Learn Docker basic
https://www.youtube.com/watch?v=Y3zqsFpUzMk&list=PLncHg6Kn2JT4kLKJ_7uy0x4AdNrCHbe0n

### 2. Jars file (auto download when build airflow image)
  - mysql-connector-j-8.3.0.jar
  - iceberg-spark-runtime-3.5_2.12-1.4.3.jar
  - nessie-spark-extensions-3.5_2.12-0.76.3.jar
  - hadoop-aws-3.3.4.jar
  - aws-java-sdk-bundle-1.12.262.jar
  - spark-sql-kafka-0-10_2.12-3.5.0
  - spark-streaming-kafka-0-10-assembly_2.12-3.5.0
  - kafka-clients-3.6.1
  - commons-pool2-2.12.0

### 3. Run Docker Compose
  - Git clone the source code, create new branch if you want to edit source code then create pull request later
  - Learn more about how to build airflow docker here https://www.youtube.com/watch?v=o_pne3aLW2w&t=2866s
  - In the docker compose file folder, run
```
docker compose -p de-project up -d
```
#### Note: airflow and dremio are heavy, so you should not run them at the same time if your memory is not big enough.

### 4. Create data in MySQL
  - Using MySQL Workbench to connect MySQL
  - Create table and import data sample (data-test.csv), detail in the mysql-query folder

### 5. Create bucket and access key in Minio
  - Connect to Minio at localhost:9000
  - Create bucket name de-1
  - Create access key and secret key

### 6. Submit spark job directly to spark cluster
  - Copy jars file to same folder with docker compose file
  - Input Minio access key and secret key in spark-test.py
  - run command
```
docker exec de-project-spark-master-1 spark-submit --master spark://spark-master:7077 /opt/bitnami/spark/spark-test.py
```
#### Note: edit spark-test.py before run

### 7. Submit spark job using airflow
  - Input Minio access key and secret key in airflow/jobs/spark-test.py
  - Go to localhost:8080, in the DAGs, choose the job and start
  - Learn more here https://www.youtube.com/watch?v=waM3Z6ofj9c&t=464s
#### Note: edit airflow/jobs/spark-test.py before run

### 8. Query data using Dremio
  - Go to localhost:9047 to access Dremio
  - Config Nessie catalog and Minio, follow https://www.dremio.com/blog/intro-to-dremio-nessie-and-apache-iceberg-on-your-laptop/

### 9. Setup Debezium
  - Using Postman run Post request to ```http://localhost:8083/connectors/``` with request body is
```
{
"name": "mysql-connector",
"config": {
"connector.class": "io.debezium.connector.mysql.MySqlConnector",
"tasks.max": "1",
"database.hostname": "mysql",
"database.port": "3306",
"database.user": "root",
"database.password": "12345678",
"topic.prefix": "mysql-server",
"database.server.id": "184054",
"database.include.list": "de_db",
"schema.history.internal.kafka.bootstrap.servers": "redpanda:9092",
"schema.history.internal.kafka.topic": "schema-changes.de_db"
}
}
```
  - Or using cmd to run command
```
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" localhost:8083/connectors/ -d "{ \"name\": \"mysql-connector\", \"config\": { \"connector.class\": \"io.debezium.connector.mysql.MySqlConnector\", \"tasks.max\": \"1\", \"database.hostname\": \"mysql\", \"database.port\": \"3306\", \"database.user\": \"root\", \"database.password\": \"12345678\", \"database.server.id\": \"184054\", \"topic.prefix\": \"mysql-server\", \"database.include.list\": \"de_db\", \"schema.history.internal.kafka.bootstrap.servers\": \"redpanda:9092\", \"schema.history.internal.kafka.topic\": \"schema-changes.de_db\" } }"
```
  - Check status of mysql connection by using localhost:8085 or call GET request to ```localhost:8083/connectors/mysql-connector/status```, or using cmd ```curl -H "Accept:application/json" localhost:8083/connectors/mysql-connector/status```

