from faker import Faker
# from kafka import KafkaProducer
import simplejson as json
# from dotenv import dotenv_values
import random
from confluent_kafka import SerializingProducer
import json

fake=Faker()
credit_no=fake.credit_card_number()
def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')
# delivery_report()

def Produce_kafka():
    for i in range(1000):
        first_name=fake.first_name()
        last_name=fake.last_name()
        address=fake.address()
        email=fake.email()
        credit_no=fake.credit_card_number()
        company=fake.company()
        quantity=fake.random_digit_not_null_or_empty()
        price=random.randint(50000,1000000)
        producer_data={'first_name':first_name,'last_name':last_name,'address':address,'email':email,
                    'credit_no':credit_no,'company':company,'quantity':quantity,'price':price}
        # print(producer_data)
        kafka_host = "164.92.85.68:9092"
        # Kafka Topics
        faker_topic = 'faker_topic'
        producer = SerializingProducer({'bootstrap.servers': f'{kafka_host}',})

        """docker exec -it 4b7b67a5435b kafka-topics --list --bootstrap-server 164.92.85.68:9092
        This code runs the list of topic within our kafka topic
        docker exec -it 4b7b67a5435b kafka-console-consumer --topic faker_topic --bootstrap-server 164.92.85.68:9092 --from-beginning
        This code verify the data with in the kafka topic and message pusblished into
        """


        
        
        # ./kafka-console-consumer.sh --bootstrap-server 164.92.85.68:9092 --topic faker_topic --from-beginning
        # Produce a message to a Kafka topic
        producer.produce(faker_topic,
                        key=credit_no,
                        value=json.dumps(producer_data),
        on_delivery=delivery_report
    )

        print('Produced data {}, data: {}'.format(i, producer_data))
        producer.flush()
Produce_kafka()