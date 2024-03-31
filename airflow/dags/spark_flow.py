import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

dag = DAG(
    dag_id = "sparking_flow",
    default_args = {
        "owner": "Vi Nguyen",
        "start_date": airflow.utils.dates.days_ago(1)
    },
    schedule_interval = "@daily"
)

start = PythonOperator(
    task_id="start",
    python_callable = lambda: print("Jobs started"),
    dag=dag
)

#jar file
jar_list = ['mysql-connector-j-8.3.0.jar',
            'iceberg-spark-runtime-3.5_2.12-1.4.3.jar',
            'nessie-spark-extensions-3.5_2.12-0.76.3.jar',
            'hadoop-aws-3.3.4.jar',
            'aws-java-sdk-bundle-1.12.262.jar'
            ]
jar_link_list = []
for jar in jar_list:
    jar_link = '/opt/airflow/jars/' + jar
    jar_link_list.append(jar_link)
jar_config = ','.join(jar_link_list)

python_job = SparkSubmitOperator(
    task_id="python_job",
    conn_id="spark-conn",
    application="jobs/spark-test.py",
    verbose=False,
    jars=jar_config,
    dag=dag
)

end = PythonOperator(
    task_id="end",
    python_callable = lambda: print("Jobs completed successfully"),
    dag=dag
)

start >> python_job >> end