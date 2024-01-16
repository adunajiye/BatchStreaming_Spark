# Batch Streaming Data Architecture 

![Screenshot (60)](https://github.com/adunajiye/BatchStreaming_Spark/assets/80220180/43476b56-21ed-4853-9dac-ceeb567dbe3c)


# Project Overview

In this project, we created continous streams of data using the python's Faker library. These streams were then pushed to a Kafka topic deployed on a remote docker container. When these data streams come into the Kafka topic, the streams are then transformed using Apache Spark's structured streaming. This transformed data is converted to a dataframe and pushed into Google cloud storage buckets as a part csv file. These csv files in the gcs bucket are then processed by Google data proc on a daily basis (batch) and the processed data moved into another Gcs buckets.
