import random
import time 
from datetime import datetime
import json
from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.errors import KafkaError

TOPIC = 'measurements'
BOOTSTRAP_SERVER = 'localhost'
SAMPLES_NUM = 10
STOCKS = ['TSLA','MSFT','AAPL','JPM','MA','F','JMJ'] # Stock symbols for some big companies

def generate_measurement():
    
    stock_price = round(random.uniform(50, 500), 2) # stock price random value 
    stock = random.choice(STOCKS)
    
    measurement = {
        'key':stock,
        'value':{
            'type': 'stock price',
            'price': stock_price,
            'timestamp':  datetime.now().strftime(format = "%Y-%m-%dT%H:%M:%S")
        }
        
    }

    return measurement

producer = KafkaProducer(bootstrap_servers=[BOOTSTRAP_SERVER],value_serializer=lambda message: json.dumps(message).encode("utf-8"))

for _ in range(SAMPLES_NUM):
    measurement_data = generate_measurement() # random measurement data in bytes format
    producer.send(topic=TOPIC, value=measurement_data)

# flush out all messages from producer's buffer
producer.flush()


