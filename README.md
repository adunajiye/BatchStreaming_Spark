# Batch Streaming Data Architecture 

![Screenshot (60)](https://github.com/adunajiye/BatchStreaming_Spark/assets/80220180/43476b56-21ed-4853-9dac-ceeb567dbe3c)


# Project Overview

In this project, we created continous streams of data using the python's Faker library. These streams were then pushed to a Kafka topic deployed on a remote docker container. When these data streams come into the Kafka topic, the streams are then transformed using Apache Spark's structured streaming. This transformed data is converted to a dataframe and pushed into Google cloud storage buckets as a part csv file. These csv files in the gcs bucket are then processed by Google data proc on a daily basis (batch) and the processed data moved into another Gcs buckets.

# Steps

 <div class="code-container">
  <button class="copy-button" data-clipboard-target="#example-code">Start Docker-Kafka</button>

  ```python
  docker-compose -f docker-compose.yml up -d
```

To see if the container is running, you can run docker ps This lists out all the running containers.

From here, you will be able to get the container id, container name, ports and so on.

You can create the kafka topic by navigating to the docker container checkng list of topics created
<div class="code-container">
  <button class="copy-button" data-clipboard-target="#example-code">Run the following command to check list of Kafka Topic</button>

  ```python
  docker exec -it container_name kafka-topics --list --bootstrap-server localhost:9092
 ```


This command opens up a terminal similar to your regular terminal. This terminal consists of all the directories. You can change directory into opt/bitnami/kafka/bin and run the command

kafka-topics.sh
--bootstrap-server localhost:9092
--topic faker_topic
--partitions 3
--replication-factor 1
--create

Here, we created a topic with name faker_topic


The code for producing messages to this topic is in streaming_pipeline/producer.py
![Screenshot (53)](https://github.com/adunajiye/BatchStreaming_Spark/assets/80220180/6d641bf0-f08f-4fa1-a35b-f6a7a09a2bc4)
