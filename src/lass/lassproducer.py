import argparse
import importlib.metadata
import json
import pickle
import random
import secrets
import subprocess
from importlib import resources
from pathlib import PosixPath

import lsst.alert.packet as lap
from lsst.alert.packet.simulate import randomBytes
import matplotlib.pyplot as plt
import numpy as np
from confluent_kafka import Producer
from tqdm import tqdm
from typing import Any, Callable, Dict, Tuple

def _get_ref(*args) -> PosixPath:
    """Return the package resource file path object.
    """
    return resources.files("lass").joinpath(*args)

def _delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result.
    Triggered by poll() or flush()."""
    if err is not None:
        return "Message delivery failed: {}".format(err)
    else:
        # print("Message delivered to {} [{}]".format(msg.topic(), msg.partition()))
        return "Message delivered to {} [{}]".format(msg.topic(), msg.partition())

def randomSmallInt():
    """Provide a random value of the Avro (32 bit, signed) `int` type."""
    return int(np.random.randint(-2**15, 2**15 - 1, dtype=np.int16))

def pprint_alert(alert: dict, save=False):
    """Pretty print alerts in JSON format and redact the cutout bytes for brevity.
    If `save` is True, save the alert in a file named 'example-alert.json'.
    """
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

def default_fastdb_writer(alert: dict[str, Any]):
    """ Modify alerts to fit FASTDB schema. """
    if alert['diaSource']['band']: # FASTDB stores band as char(1) not string
        alert['diaSource']['band'] = random.choice(['u', 'g', 'r', 'i', 'z', 'y'])
    alert['diaSource']['detector'] = randomSmallInt()

    # Match IDs
    alert['diaObject']['diaObjectId'] = secrets.randbits(63)
    alert['diaSource']['diaObjectId'] = alert['diaObject']['diaObjectId']
    alert['diaSourceId'] = secrets.randbits(63)
    alert['diaSource']['diaSourceId'] = alert['diaSourceId']

    # regenerate cutouts
    cutoutsize = len(alert['cutoutScience']) # pick a size from one of the cutouts (1-32kb by default)
    alert['cutoutDifference'] = randomBytes(cutoutsize, cutoutsize)
    alert['cutoutScience']    = randomBytes(cutoutsize, cutoutsize)
    alert['cutoutTemplate']   = randomBytes(cutoutsize, cutoutsize)
    return alert

def _get_alert_schema(schema_ver: tuple[int, int]) -> lap.Schema:
    schemafile = (
        lap.get_schema_path(*schema_ver)
        + f"/lsst.v{schema_ver[0]}_{schema_ver[1]}.alert.avsc"
    )
    return lap.Schema.from_file(filename=schemafile)

def _alert_serializer(
    avro: bool = False,
    schema_ver: tuple[int, int] = (10, 0),
) -> Callable[[dict[str, Any]], bytes]:
    """Return the selected Kafka payload serializer."""
    if avro:
        return _get_alert_schema(schema_ver).serialize
    return pickle.dumps

def generate(
    alertwriter : Callable[dict[str, Any], dict[str, Any]] = default_fastdb_writer,
    numalerts : int = 10000,
    nprvdiasources : int | tuple[int, int] = 0,
    schema_ver: tuple[int, int] = (10,0),
    # plotsizes: bool = False,
    # save: bool = False
):
    """Generate LSST alerts.

    Args:
    ---
        alertwriter: Callable[dict]
            A callable function that writes data to the alert content.
            By default uses `lass.default_fastdb_writer`.
        numalerts: int
            The number of alerts to generate.
        nprvdiasources : int | tuple[int, int]
            Number of alerts to include in `prvDiaSources`. If a tuple of two
            ints, number will be between the two supplied integers.
        schema_ver : tuple[int, int]
            Tuple of (major_version, minor_version). (default: (10, 0)).

    Returns:
    ---
        alerts : List[dict]
            A list of the generated alerts. Each alert is a dictionary with fields
            defined by the LSST schema used.
        alertsizes : List[float)]
            a list of the alert sizes in kilobytes (1kB = 1024 bits).

    """
    alerts = []
    schema = _get_alert_schema(schema_ver)
    print(f"Schema: {schema.definition['name']}")

    arrayCount = {}
    if type(nprvdiasources) == int:
        if nprvdiasources > 0:
            arrayCount = {
                'prvDiaSources': nprvdiasources
                # 'prvDiaForcedSources': 100//12, # unimplemented
            }
    elif type(nprvdiasources) != int and len(nprvdiasources) == 2:
        nprvdiasources = tuple(sorted(nprvdiasources))
    else:
        raise ValueError("nprvdiasources must be an integer or 2-tuple of ints.")

    alertsizes = []
    for i in tqdm(range(0, numalerts), desc="Generating alerts"):
        if type(nprvdiasources) != int and len(nprvdiasources) == 2:
            # draw a random number of prvDiaSources to generate based on range given
            arrayCount['prvDiaSources'] = random.choice(
                range(nprvdiasources[0], nprvdiasources[1]+1)
            )

        alert = lap.simulate_alert(
            schema.definition,
            keepNull=['ssObject', 'ssSource', 'mpc_orbits'],
            arrayCount=arrayCount
        )

        alert = alertwriter(alert)
        # pprint_alert(alert, save=True); exit()

        alerts.append(alert)
        alertsizes.append(len(pickle.dumps(alert))/1024)

    return alerts, alertsizes

def send(
    alerts: list[dict[str, Any]],
    server: str = "localhost:19092",
    topic: str = "lass-topic",
    avro: bool = False,
    schema_ver: tuple[int, int] = (10, 0),
):
    """Send alerts to Kafka, serialized as pickle or schemaless Avro."""
    if len(alerts) == 0:
        return

    # This assumes the schema_ver matches the schema version used to generate `alerts`
    serialize = _alert_serializer(avro=avro, schema_ver=schema_ver)
    p = Producer({
        "bootstrap.servers": server
    })

    pbar = tqdm(alerts)
    # this is a hack for when p.produce doesn't update the pbar description sometimes ¯\_(ツ)_/¯
    pbar.set_description(f"Message delivered to {topic} [0]")
    for alert in pbar:
        # Trigger any available delivery report callbacks from previous produce() calls
        p.poll(0)

        # Asynchronously produce a message. The delivery report callback will
        # be triggered from the call to poll() above, or flush() below, when the
        # message has been successfully delivered or failed permanently.
        p.produce(
            topic,
            serialize(alert),
            callback=lambda e, m: pbar.set_description(_delivery_report(e, m)),
        )

    # Wait for any outstanding messages to be delivered and delivery report
    # callbacks to be triggered.
    p.flush()

def startbroker():
    ref = _get_ref('scripts', 'start-broker.sh')
    subprocess.run(ref)

def removebroker():
    ref = _get_ref('scripts', 'remove-broker.sh')
    subprocess.run(ref)

class RawAndDefaultsHelpFormatter( # ty https://stackoverflow.com/questions/61324536/
    argparse.RawDescriptionHelpFormatter, argparse.ArgumentDefaultsHelpFormatter
):
    pass

def main(): # entry point for lass cli tool
    parser = argparse.ArgumentParser(
        "lass",
        description="""
▄▄     ▄▄▄   ▄▄▄▄  ▄▄▄▄
██    ██▀██ ███▄▄ ███▄▄
██▄▄▄ ██▀██ ▄▄██▀ ▄▄██▀
LSST Alert Stream Simulator
Simulate a Rubin LSST Alert Broker with fake alerts and a kafka stream.""", # font is ANSI compact
        formatter_class=RawAndDefaultsHelpFormatter
    )
    parser.add_argument(
        "-n",
        "--numalerts",
        type=int,
        default=10000, # a few seconds worth of alerts (10000)
        help="Number of alerts to generate."
    )
    parser.add_argument(
        "-p",
        "--prvdiasources",
        type=int,
        default=0,
        help="Maximum number of prvDiaSources to generate in each alert. Actual number is random."
    )
    parser.add_argument(
        "-f",
        "--fixed-prvdiasources",
        action="store_true",
        help="If set, all alerts will have the same number of prvDiaSources supplied."
    )
    parser.add_argument(
        "-s",
        "--schemaversion",
        type=int,
        default=10,
        help="The major version of the LSST alert schema to use. Defaults to v10 and the earliest minor version (i.e. v10-0)."
    )
    parser.add_argument(
        '-b',
        '--batch',
        type=int,
        default=0,
        help="Send batches of alerts as they are generated instead of waiting for all alerts to be generated."
    )
    parser.add_argument('-v', '--version', action='version', version=importlib.metadata.version('lass'))
    parser.add_argument(
        '--quit',
        action='store_true',
        help="Remove lass's kafka broker (along with the simulated alerts)",
    )
    parser.add_argument('--skipbroker', action='store_true', help="If set, do not start a kafka broker.")
    parser.add_argument(
        '--avro',
        action='store_true',
        help="Serialize Kafka message payloads as schemaless Avro instead of pickle.",
    )
    args = parser.parse_args()

    if args.quit:
        removebroker()
        exit()

    ###
    numalerts = args.numalerts
    schema_ver = (args.schemaversion, 0) # (Major, Minor)
    batch = args.batch

    batches = [numalerts] # no batches
    if batch > 0 and batch < numalerts: # Send alerts in batches
        # regular sized batches + leftover
        batches = [batch]*(numalerts//batch) + [numalerts % batch]
        assert sum(batches) == numalerts
    elif batch < 0 or batch > numalerts:
        print("Ignoring batch argument. Batch must be greater than 0 and less than numalerts.")

    cmd_dps = subprocess.run("docker ps | grep lassbroker | wc -l", shell=True, capture_output=True)
    is_lassbroker_running = (
        int(cmd_dps.stdout) > 0
    )
    if not is_lassbroker_running and not args.skipbroker:
        startbroker()
        is_lassbroker_running = True

    totalerts = 0
    for batch in batches:
        alerts, _ = generate(
            numalerts=batch,
            schema_ver=schema_ver,
            nprvdiasources=(0, args.prvdiasources) if args.fixed_prvdiasources else args.prvdiasources
        )
        if not args.skipbroker and is_lassbroker_running:
            send(alerts, avro=args.avro, schema_ver=schema_ver)
        totalerts = totalerts + batch
        if len(batches) > 1:
            print(f"Handled {totalerts} of {numalerts} alerts")
