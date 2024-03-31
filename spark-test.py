# Import the necessary modules
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import *

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

#Read from mySQL
# df = (spark.read.format('jdbc')
#       .option('url','jdbc:mysql://mysql:3306/de_db')
#       .option('driver','com.mysql.cj.jdbc.Driver')
#       .option('dbtable','people')
#       .option('user','admin')
#       .option('password','12345678')
#       .load()
#       )
# new_df = df.limit(5)

#write to minio
# spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.iceberg_test_1;")
# spark.sql("CREATE TABLE IF NOT EXISTS nessie.iceberg_test_1.people (id int,user_id string,first_name string,last_name string,sex string,phone string,email string,date_of_birth string,job_title string) using iceberg;")
# new_df.write.format("iceberg").mode("overwrite").save("nessie.iceberg_test_1.people")
# new_df.write.mode("overwrite").parquet('s3a://de-1/people')

#Read from Minio
iceberg_df = spark.table('nessie.iceberg_test_1.people')
iceberg_df.show()

spark.stop()