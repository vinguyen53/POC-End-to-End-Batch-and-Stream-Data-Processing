# Import the necessary modules
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

#var
minio_port = 'minio:9000'
mino_access_key = 'c31rxXYArA5TLam3eRa2'
mino_secret_key = 'dD3625L8t8sbOH9CWNSdeF1ao8OZDPH3bTkRrBhG'
nessie_warehouse = 's3a://de-1/'
nessie_uri = 'http://nessie:19120/api/v1'

#jar file
jar_list = ['mysql-connector-j-8.3.0.jar',
            'iceberg-spark-runtime-3.5_2.12-1.4.3.jar',
            'nessie-spark-extensions-3.5_2.12-0.76.3.jar',
            'spark-sql-kafka-0-10_2.12-3.5.0.jar',
            'kafka-clients-3.6.1.jar',
            'spark-streaming-kafka-0-10-assembly_2.12-3.5.0.jar',
            'commons-pool2-2.12.0.jar'
            ]
jar_link_list = []
for jar in jar_list:
    jar_link = '/opt/bitnami/spark/jars/' + jar
    jar_link_list.append(jar_link)
jar_config = ','.join(jar_link_list)



#config minio, nessie
conf = (SparkConf()
      .set("spark.hadoop.fs.s3a.endpoint", f"http://{minio_port}")
      .set("spark.hadoop.fs.s3a.access.key", f"{mino_access_key}")
      .set("spark.hadoop.fs.s3a.secret.key", f"{mino_secret_key}")
      .set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      .set("spark.hadoop.fs.s3a.path.style.access", "true")
      .set("spark.hadoop.fs.s3a.committer.name", "directory")
      .set("spark.hadoop.fs.s3a.committer.staging.conflict-mode", "replace")
      .set("spark.hadoop.fs.s3a.committer.staging.tmp.path", "/tmp/staging")
      .set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions')
      .set('spark.sql.catalog.nessie', 'org.apache.iceberg.spark.SparkCatalog') #create a new catalog nessie as an iceberg catalog
      .set('spark.sql.catalog.nessie.catalog-impl', 'org.apache.iceberg.nessie.NessieCatalog') #tell the catalog that its a Nessie catalog
      .set('spark.sql.catalog.nessie.warehouse', f'{nessie_warehouse}') #select the location for the catalog to store data
      .set('spark.sql.catalog.nessie.io-impl', 'org.apache.iceberg.hadoop.HadoopFileIO') #config to write to object store
      .set('spark.sql.catalog.nessie.uri', f'{nessie_uri}') #set the location of the nessie server
      .set('spark.sql.catalog.nessie.ref', 'main') #default branch for the catalog to work on
      .set('spark.sql.catalog.nessie.authentication.type', 'NONE') #authentication mechanism
      .set('spark.sql.catalog.nessie.s3.endpoint', f'http://{minio_port}')
      #.set('spark.jars.packages', 'software.amazon.awssdk:bundle:2.20.18,software.amazon.awssdk:url-connection-client:2.20.18')
      )

# Create a SparkSession
spark = (SparkSession
         .builder
         .config('spark.jars',jar_config)
         .config(conf=conf)
         .appName("My App")
         .getOrCreate())

#table schema
table_schema = StructType([
    StructField('id', IntegerType()),
    StructField('user_id', StringType()),
    StructField('first_name', StringType()),
    StructField('last_name', StringType()),
    StructField('sex', StringType()),
    StructField('phone', StringType()),
    StructField('email', StringType()),
    StructField('date_of_birth', StringType()),
    StructField('job_title', StringType())
])

#mysql event schema
event_schema = StructType([
    StructField('schema', StringType()),
    StructField('payload',StructType([
    StructField('before', StringType()),
    StructField('after', StringType()),
    StructField('source', StringType()),
    StructField('op', StringType()), #op=c -> create/insert, op=d -> delete, op=u -> update, op=r -> read
    StructField('ts_ms', StringType()),
    StructField('transaction', StringType())
    ]))
])

#Read stream from redpanda
df = (spark.readStream.format('kafka')
      .option("kafka.bootstrap.servers", "redpanda:9092")
      .option("subscribe", "mysql-server.de_db.people")
      .option('startingOffsets','earliest')
      .option('maxFilesPerTrigger', 10)
      .load()
      )

#convert to table
df = (
    df.select(from_json(col('value').cast(StringType()), event_schema).alias('value'))
    .select('value.payload.*')
    .withColumn('data', expr("case when op in ('r','u','c') then after else before end"))
    .select(from_json('data', table_schema).alias('data'),'op','ts_ms')
    .select('data.*','op','ts_ms').where('id is not null')
    )
#df.printSchema()
#Write stream to terminal
#query = df.limit(10).writeStream.outputMode('append').format('console').start()

#Create table
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.iceberg_stream;")
spark.sql('''CREATE TABLE IF NOT EXISTS nessie.iceberg_stream.people
           (id int,
          user_id string,
          first_name string,
          last_name string,
          sex string,
          phone string,
          email string,
          date_of_birth string,
          job_title string,
          op string,
          ts_ms string) 
          using iceberg;''')
# new_df.write.format("iceberg").mode("overwrite").save("nessie.iceberg_test_1.people")

#Write stream to Minio
# write_minio = (df
#                .writeStream
#                .outputMode("append")
#                .format('parquet')
#                .option('path','s3a://de-1/stream-people/data/')
#                .option('checkpointLocation','s3a://de-1/stream-people/checkpoint')
#                .start())

#Read from iceberg table
# iceberg_df = spark.table('nessie.iceberg_stream.people')
# iceberg_df.printSchema()
# sort_iceberg_df = iceberg_df.orderBy("id", ascending=False)
# sort_iceberg_df.limit(10).show()

#Write stream and upsert into iceberg table
def upsertData(batchDF,batchId):
    batchDF.createOrReplaceTempView('tempTable')
    batchDF._jdf.sparkSession().sql('''
        MERGE INTO nessie.iceberg_stream.people as t
        USING (
              select id,user_id,first_name,last_name,sex,phone,email,date_of_birth,job_title,op,ts_ms
                from (
                    SELECT id,user_id,first_name,last_name,sex,phone,email,date_of_birth,job_title,op,ts_ms,
                    row_number() OVER (PARTITION BY id ORDER BY ts_ms desc) row_num 
                    FROM tempTable
                ) 
                where row_num=1
        ) as s
        ON t.id = s.id
        WHEN MATCHED and s.op = 'd' and s.ts_ms > t.ts_ms THEN DELETE
        WHEN MATCHED and s.op in ('u','c','r') and s.ts_ms > t.ts_ms THEN UPDATE SET t.id = s.id, t.user_id = s.user_id, t.first_name = s.first_name, t.last_name = s.last_name, t.sex = s.sex, t.phone = s.phone, t.email = s.email, t.date_of_birth = s.date_of_birth, t.job_title = s.job_title, t.op = s.op, t.ts_ms = s.ts_ms
        WHEN NOT MATCHED THEN INSERT (id,user_id,first_name,last_name,sex,phone,email,date_of_birth,job_title,op,ts_ms) VALUES (s.id,s.user_id,s.first_name,s.last_name,s.sex,s.phone,s.email,s.date_of_birth,s.job_title,s.op,s.ts_ms)
    ''')
write_iceberg_stream = (df
                        .writeStream
                        .foreachBatch(upsertData)
                        .trigger(processingTime='5 seconds')
                        .outputMode("append")
                        .option('checkpointLocation','s3a://de-1/iceberg_stream/checkpoint')
                        .start()
                        )

# spark.stop()
#write_minio.awaitTermination()
write_iceberg_stream.awaitTermination()