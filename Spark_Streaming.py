import pyspark
from pyspark.sql import SparkSession
from kafka import KafkaConsumer
import json
# from dotenv import dotenv_values
import boto3
from kafka import KafkaConsumer
from datetime import datetime
from pyspark.sql.functions import explode,from_json,col
from pyspark.sql.types import StringType,IntegerType,StructType,StructField
import logging
current_date=str(datetime.now())
from os import environ
# env_var=dotenv_values('.env')
logger = logging.getLogger("spark_structured_streaming")




cluster_manager="spark://164.92.85.68:7077"
packages = [ "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
                    #  "org.apache.hadoop:hadoop-common:3.3.6",
                    #  "org.scala-lang:scala-library:2.13.0"
]
jars = ",".join(packages)  
gcs_keyfile = ""
gcs_keyfilee = gcs_keyfile
def create_sparksession():
    spark = SparkSession.builder.appName('Kafka_BatchStreaming')\
                .config('spark.jars.packages',jars)\
                .config('spark.master',cluster_manager)\
                .getOrCreate()
      # Enable GCS settings
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")

    # Set GCS credentials
    spark.sparkContext._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile",gcs_keyfilee)
    spark.sparkContext.setLogLevel("ERROR")
    logging.info('Spark session created successfully')
    return spark   



def schema():
    columns = StructType([StructField('first_name',
    StringType(), False),
    StructField('last_name', StringType(), False),
    StructField('address', StringType(), False),
    StructField('email', StringType(), False),
    StructField('credit_no', IntegerType(), False),
    StructField('company', StringType(), False),
    StructField('quantity', IntegerType(), False),
    StructField('price',IntegerType(), False)])
    return columns


def read_stream():
    kafka_boostrap_server = "164.92.85.68" + ":9092"
    print(kafka_boostrap_server)
    kafka_topic = "faker_topic"
    print(kafka_topic)
    
    stream=create_sparksession()
    # print(stream)
    df1 =stream.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_boostrap_server) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .option('includeHeaders','true')\
    .load()
    df2=df1.selectExpr("CAST(value AS STRING)",'headers')
    df3=df2.select(from_json('value',schema=schema()).alias('temp')).select('temp.*')
    return df3



def write_stream():
  """
    Starts the streaming to table spark_streaming.random data in Google Cloud buckets
  """
  logging.info("Streaming is being started...")
  bucket_streamm = read_stream()
  bucket_uri = ""
  checkpoint_uri = ""


  # Google BigQuery configuration
gcp_credentials = environ.get('GCP_CREDENTIALS')
gcp_project = environ.get('GCP_PROJECT')
bigquery_dataset = environ.get('GCP_BIGQUERY_DATASET')
bigquery_table = environ.get('GCP_BIGQUERY_TABLE')
bigquery_uri = f'{gcp_project}:{bigquery_dataset}.{bigquery_table}'
storage_stream = bucket_streamm.writeStream \
  .format('csv')\
  .option('path',bucket_uri)\
  .option('checkpointLocation',checkpoint_uri)\
  .start().awaitTermination()
  print('Streaming to GCS.......')
    
write_stream()