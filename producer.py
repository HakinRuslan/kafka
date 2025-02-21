from kafka import KafkaConsumer

import logging

import uuid

import json

from kafka import KafkaProducer

from faker import Faker

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)

fak = Faker("ru_RU")

producer = KafkaProducer(bootstrap_servers='5.35.98.86:9092')

while True:
    fullname = fak.name()
    fullnames = [str(name) for name in fullname.split(' ')]
    logging.info(fullnames)
    user = {"id": str(uuid.uuid4()), "firstname": fullnames[0], "Lastname": fullnames[1], "Secondname": fullnames[2], "addres": fak.street_address(), "index": fak.postcode() , "number": fak.phone_number(), "work": fak.job()}
    json_data = json.dumps(user)
    byte_data = json_data.encode("utf-8")
    logging.info(user)
    future = producer.send('ruslankrutou', byte_data)
    result = future.get()
    logging.info(result)