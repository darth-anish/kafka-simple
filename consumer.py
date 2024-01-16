import random
import time 
import json
from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.errors import KafkaError
import pandas as pd

TOPIC = 'measurements'
BOOTSTRAP_SERVER = 'localhost:9092'

# data to store all received messages
data = {'stock':[], 'price':[], 'timestamp':[]}
# this consumer retrieves message from producer 
consumer = KafkaConsumer(TOPIC,bootstrap_servers=BOOTSTRAP_SERVER,auto_offset_reset='latest',enable_auto_commit=True,consumer_timeout_ms=10000,group_id='stock-user')
print('Consumer started! Waiting to receive data ...')
for msg in consumer:
    if msg:
        msg_data = msg.value.decode()
        msg_data = json.loads(msg_data)
        print(f"Received message: {msg_data}")
        data['stock'].append(msg_data['key'])
        data['price'].append(msg_data['value']['price'])
        data['timestamp'].append(msg_data['value']['timestamp'])

consumer.close()
# dataframe to store all received messages
df = pd.DataFrame(data)
print(df.head())
print(df.shape)
