import random
import time, calendar
from random import randint
from kafka import KafkaProducer
from kafka import errors 
from json import dumps
from time import sleep


KAFKA_BROKERS = ['localhost:29092']
KAFKA_TOPIC = "payment_msg"

def write_data(producer, topic):
    data_cnt = 20000
    order_id = calendar.timegm(time.gmtime())
    max_price = 1000

    for i in range(data_cnt):
        ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        rd = random.random()
        order_id += 1
        pay_amount = round(max_price * rd, 2)
        pay_platform = 0 if random.random() < 0.9 else 1
        province_id = randint(0, 6)
        cur_data = {"createTime": ts, "orderId": order_id, "payAmount": pay_amount, "payPlatform": pay_platform, "provinceId": province_id}
        producer.send(topic, value=cur_data)
        print(f'data produced to topic {topic} - {cur_data}')
        sleep(random.randint(1, 5))

def create_producer(bootstrap_servers):
    print("Connecting to Kafka brokers {}", bootstrap_servers)
    for _ in range(0, 6):
        try:
            producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                            value_serializer=lambda x: dumps(x).encode('utf-8'))
            print("Connected to Kafka")
            return producer
        except errors.NoBrokersAvailable:
            print("Waiting for brokers to become available")
            sleep(10)

    raise RuntimeError("Failed to connect to brokers within 60 seconds")

if __name__ == '__main__':
    producer = create_producer(KAFKA_BROKERS)
    write_data(producer, KAFKA_TOPIC)
