from kafka import KafkaConsumer


import logging

import uuid

import json

consumer = KafkaConsumer(bootstrap_servers='5.35.98.86:9092', group_id="kafka_1")
consumer.subscribe(["ruslankrutou"])

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        else:
            # действия с полученным сообщением
            logging.info(f"Received message: {msg.value().decode('utf-8')}")
except KeyboardInterrupt:
    pass
finally:
    consumer.close()