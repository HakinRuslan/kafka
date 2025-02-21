from kafka import KafkaConsumer


import logging

import uuid

import json

consumer = KafkaConsumer(bootstrap_servers='5.35.98.86:9092', group_id="kafka_1")

def cons_loop(consumer, topics):
    try:
        consumer.subscribe(["ruslankrutou"])

        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            else:
                data = json.loads(msg.value().decode('utf-8'))
                logging.info(f"Received message: {data}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
    
def main():
    cons_loop(consumer)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    main()