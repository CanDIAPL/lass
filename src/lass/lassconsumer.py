import pickle
from confluent_kafka import Consumer
from tqdm import tqdm
import logging
from logging import getLogger, DEBUG

logging.basicConfig(level=DEBUG)
logger = getLogger(__name__)

if __name__ == '__main__':
    c = Consumer(
        {
            "bootstrap.servers": "localhost:19092",
            "group.id": "mygroup",
            "auto.offset.reset": "earliest",
            # "logger": logger,
            # "debug": "broker,topic,metadata",
        }
    )

    c.subscribe(["lass-topic"])

    with tqdm() as pbar:
        while True:
            msg = c.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                print("Consumer error: {}".format(msg.error()))
                continue

            alert = pickle.loads(msg.value())
            pbar.update()
            pbar.set_description("Received message: {}".format(alert["diaSourceId"]))
        c.close()
