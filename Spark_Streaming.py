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
import configparser

config = configparser.ConfigParser()
config.read_file(open('C:/Users/user/Desktop/Batch_Data_Pipeline/clusters.config'))
print(config.get("AWS","AWS_KEY"))




cluster_manager="spark://164.92.85.68:7077"
packages = [ "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                     "org.apache.hadoop:hadoop-common:3.3.6",
                    #  "org.scala-lang:scala-library:2.13.0"
                    "com.amazonaws:aws-java-sdk:1.11.469"
]
jars = ",".join(packages)  
def create_sparksession():
    spark = SparkSession.builder.appName('Kafka_Batch_Streaming')\
                .config('spark.jars.packages',jars)\
                .config('spark.hadoop.fs.s3a.aws.credentials.provider','org.apche.hadoop.fs.s3a.impl.SimpleAWSCredentialsProvider')\
                .config('spark.master',cluster_manager)\
                .getOrCreate()
                # Enable hadoop s3a settings
    spark.sparkContext._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", \
                                        "com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key",config.get("AWS","AWS_KEY"))
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key",config.get("AWS","AWS_SECRET"))
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.us-east-1.amazonaws.com")
                #  .config('spark.hadoop.fs.s3a.endpoint', 's3://d2b-internal-assessment-dwh.cxeuj0ektqdz.eu-central-1.rds.amazonaws.com')\
                # .config('spark.hadoop.fs.s3a.path.style.access', 'true')\
                # .config('spark.hadoop.fs.s3a.connection.ssl.enabled', 'true')\
                # .config('spark.hadoop.fs.s3a.fast.upload', 'true')\
                # .config('spark.hadoop.fs.s3a.connection.maximum', '1000')\

    spark.sparkContext.setLogLevel('WARN')
    logging.info('Spark session created successfully')
    return spark   
# create_sparksession()



def kafka_schema():
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
# kafka_schema()



def read_stream(kafka_topic,schema):
    kafka_boostrap_server = "164.92.85.68" + ":9092"
    print(kafka_boostrap_server)
    kafka_topic = "faker_topic"
    print(kafka_topic)
    schemaa =kafka_schema()
    
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
    df3=df2.select(from_json('value',schema=schema).alias('temp')).select('temp.*')
    # df4 = df3.select('data.*')
    return df3

kafka_df = read_stream("faker_topic",schema=kafka_schema()).alias('kafka_schema')
# print(kafka_df)

def write_Stream():
    """
    Starts the streaming to table spark_streaming.random data in aws s3 buckets
    """
    logging.info("Streaming is being started...")
    input = read_stream("faker_topic", schema=kafka_schema())
    bucket_uri = "d2b-internal-assessment-bucket/analytics_export/ajiyemma1238"
    file_name = "kafka_raw_data.csv"
    checkpoint_uri = "s3://d2b-internal-assessment-bucket/analytics_export/ajiyemma1238"
    current_time= datetime.now()
    Bucket_file_name =   str(current_time)
    Bucket_file_name = Bucket_file_name + file_name 
    print(Bucket_file_name)

     # Specify the 'path' option for writing CSV files
    output_path = f"s3://{bucket_uri}/{Bucket_file_name}"
    print(output_path)
    print(checkpoint_uri)
    
    writedata = (input.writeStream \
                  .format('csv') \
                  .option('checkpoint_location', checkpoint_uri) \
                  .option('path', output_path) \
                  .outputMode('append') \
                  .start())

    writedata.awaitTermination()  
    print('Streaming to AWS S3 .......')
write_Stream()
# print(kafka_data)



#   # Google BigQuery configuration
# gcp_credentials = environ.get('GCP_CREDENTIALS')
# gcp_project = environ.get('GCP_PROJECT')
# bigquery_dataset = environ.get('GCP_BIGQUERY_DATASET')
# bigquery_table = environ.get('GCP_BIGQUERY_TABLE')
# bigquery_uri = f'{gcp_project}:{bigquery_dataset}.{bigquery_table}'
# storage_stream = bucket_streamm.writeStream \
#   .format('csv')\
#   .option('path',bucket_uri)\
#   .option('checkpointLocation',checkpoint_uri)\
#   .start().awaitTermination()
# print('Streaming to GCS.......')
    
# write_stream()