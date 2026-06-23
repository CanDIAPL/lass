import json
import pickle
import secrets, random

import lsst.alert.packet as lap
import matplotlib.pyplot as plt
import numpy as np
from confluent_kafka import Producer
from tqdm import tqdm

def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result.
    Triggered by poll() or flush()."""
    if err is not None:
        return "Message delivery failed: {}".format(err)
    else:
        # print("Message delivered to {} [{}]".format(msg.topic(), msg.partition()))
        return "Message delivered to {} [{}]".format(msg.topic(), msg.partition())

def randomSmallInt():
    """Provide a random value of the Avro (32 bit, signed) `int` type.
    """
    return int(np.random.randint(-2**15, 2**15 - 1, dtype=np.int16))

def pprint_alert(alert: dict, save=False):
    alert["cutoutDifference"] = f"<redacted {len(alert['cutoutDifference'])} bytes>"
    alert["cutoutScience"]    = f"<redacted {len(alert['cutoutScience'])} bytes>"
    alert["cutoutTemplate"]   = f"<redacted {len(alert['cutoutTemplate'])} bytes>"
    print(json.dumps(alert, sort_keys=True, indent=2))
    if save:
        with open('example-alert.json', 'w') as f:
            json.dump(alert, f, sort_keys=True, indent=2)

def plotAlertSizeDistibution(alertsizes):
    plt.hist(alertsizes, bins=20)
    plt.xlabel("Alert Size (kB)")
    plt.ylabel("# of Alerts")
    plt.savefig("fig_size_dist.png")
    plt.show()

# if __name__ == "__main__":
def main(): # entry point for lass cli tool
    numAlerts = 10000  # a few seconds worth of alerts
    alerts = []
    schema = lap.Schema.from_file(
        # filename='/Users/mchamma/dev/alert_packet/schema/10/0/lsst.v10_0.alert.avsc'
        filename='../../config/lsst.v10_0.alert.avsc'
    )
    print(f"Schema: {schema.definition['name']}")

    alertsizes = []
    for i in tqdm(range(0, numAlerts), desc="Generating alerts"):
        alert = lap.simulate_alert(
            schema.definition,
            keepNull=['ssObject', 'ssSource', 'mpc_orbits'],
            arrayCount={ # simulate_alert does not seem to correctly implement this
                # 'prvDiaSources': 100,
                # 'prvDiaForcedSources': 100//12,
                # 'prvDiaNondetectionLimits': 0
            }
        )

        ### Modify alerts to fit FASTDB schema
        if alert['diaSource']['band']: # FASTDB stores band as char(1) not string
            alert['diaSource']['band'] = random.choice(['u', 'g', 'r', 'i', 'z', 'y'])
        alert['diaSource']['detector'] = randomSmallInt()

        # Match IDs
        alert['diaObject']['diaObjectId'] = secrets.randbits(63)
        alert['diaSource']['diaObjectId'] = alert['diaObject']['diaObjectId']
        alert['diaSourceId'] = secrets.randbits(63)
        alert['diaSource']['diaSourceId'] = alert['diaSourceId']
        ###

        alerts.append(alert)
        alertsizes.append(len(pickle.dumps(alert))/1024)

    # plotAlertSizeDistibution(alertsizes)
    # pprint_alert(alerts[0], save=True)
    # exit()


    p = Producer({
        "bootstrap.servers": "localhost:19092"
    })

    pbar = tqdm(alerts)
    for alert in pbar:
        # Trigger any available delivery report callbacks from previous produce() calls
        p.poll(0)

        # Asynchronously produce a message. The delivery report callback will
        # be triggered from the call to poll() above, or flush() below, when the
        # message has been successfully delivered or failed permanently.
        p.produce(
            "lass-topic",
            pickle.dumps(alert),
            callback=lambda e, m: pbar.set_description(delivery_report(e, m)),
        )

    # Wait for any outstanding messages to be delivered and delivery report
    # callbacks to be triggered.
    p.flush()
