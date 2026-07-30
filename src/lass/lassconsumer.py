import argparse
import pickle

import lsst.alert.packet as lap
from confluent_kafka import Consumer
from tqdm import tqdm
import logging
from logging import getLogger, DEBUG

logging.basicConfig(level=DEBUG)
logger = getLogger(__name__)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Consume alerts from LASS.")
    parser.add_argument(
        '--avro',
        action='store_true',
        help="Deserialize schemaless Avro payloads instead of pickle.",
    )
    parser.add_argument(
        '-s', '--schemaversion', type=int, default=10,
        help="Major LSST alert schema version used by the producer.",
    )
    args = parser.parse_args()

    deserialize = pickle.loads
    if args.avro:
        schema_ver = (args.schemaversion, 0)
        schemafile = (
            lap.get_schema_path(*schema_ver)
            + f"/lsst.v{schema_ver[0]}_{schema_ver[1]}.alert.avsc"
        )
        deserialize = lap.Schema.from_file(filename=schemafile).deserialize

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

            alert = deserialize(msg.value())
            pbar.update()
            pbar.set_description("Received message: {}".format(alert["diaSourceId"]))
        c.close()
