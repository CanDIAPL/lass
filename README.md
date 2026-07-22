# LSST Alert Stream Simulator (lass)

`lass` simulates alerts sent by the Rubin Observatory's [Legacy Survey of Space and Time (LSST)](https://rubinobservatory.org/explore/how-rubin-works/lsst) during its real-time difference imaging pipeline. It creates a Kafka broker in a container using docker, generates LSST schema-compliant alerts, and sends the alerts to the Kafka container. Scripts, other containers, or external tools can then be used to ingest the alerts from the `lass` broker.

The Kafka broker emulates the community alert broker architecture, which generally (but not always) send most of their alerts via kafka streams. More information on the community alert brokers is available [here](https://rubinobservatory.org/for-scientists/data-products/alerts-and-brokers). 

`lass` was primarily written to serve as a tool for stress testing databases that store LSST alerts. One such database is [FASTDB](https://fastdb.readthedocs.io/en/latest/), developed by the [LSST-DESC Science Collaboration](https://lsstdesc.org/). 

In general, `lass`'s generated alerts can be modified to simulate physical scenarios, and can be used to prototype and test real-time analysis pipelines that process the LSST alert stream. 

## Features:
* Generate millions of alerts with random data, aiming for realistic alert sizes (real alerts are on the order of 82 kb each). Note that by default simulated alerts are filled with unphysical data.
* Easy switching of alert schema versions and ongoing compliance with version updates. LSST alerts are simulated using the [CanDIAPL fork](https://github.com/CanDIAPL/alert_packet) of LSST's [alert_packet repo](https://github.com/lsst/alert_packet). 
* Multiple kafka advertised listeners for container and host networking
* Command line and Python interfaces
* Custom and configurable alert generation allows for simulating realistic alert cadences and physical data.
  * Configurable `prvDiaSources` field. This field is important for generating long running lightcurves.
  * Pass a custom `alertwriter` function to write any data to any field within the alert packet.

## Installation

`lass` can be installed via `pip`:

```bash
pip install lass
```

To use the Kafka broker you must have [Docker](https://docs.docker.com/engine/install/) installed.

*Advanced*: if you would like to modify the configuration of the Kafka container, installing from the repo and with `--editable` is currently the best way:

```bash
git clone https://github.com/CanDIAPL/lass.git
cd lass && pip install --editable . 
```

The Kafka broker/container configuration lives in [src/lass/scripts/start-broker.sh](src/lass/scripts/start-broker.sh)

## Usage:

Installing `lass` installs a command line tool as well as the `lass` python module.

### **Command line-interface**
To create a kafka container and send 10,000 alerts to it, run `lass` on the command line:

```bash
$ lass
Schema: lsst.v10_0.alert
Generating alerts: 100%|██████████| 10000/10000 [00:01<00:00, 5084.26it/s]
Created container with ID:     4aa8c38b1d06e7936e56c17dcbeac43ccf526705793168f8ad94351001f59faa
Created network with ID:       6784c250bc246a14fc7a17be0dcc46ed014f4f447c982f2f7647ef3aaf204f00
Created topic lass-topic.
Message delivered to lass-topic 100%|█████████| 10000/10000 [00:00<00:00, 14487.84it/s]
```

This will start a kafka container named `lassbroker`, then create and connect it to a docker user-defined bridge network called `lassnet`. It will also create the kafka topic `'lass-topic'`. 

To send a million alerts to the kafka broker instead, or to send a million alerts to the already running kafka container created in the previous step, do

```bash
$ lass -n 1000000
```

To send 100 alerts each linking to up to 100 'previous' alerts (i.e. with a populated `prvDiaSources` field), use `-p <int>`. 
This will randomly fill each alert with between 0 and 100 `diaSources`. To generate alerts that all have the same length of `prvDiaSources`, include the `-f` flag.
```bash
$ lass -n 100 -p 100 -f # populate each alert's 'prvDiaSources' field with a list of 100 diaSources
```

To change the LSST Alert schema used, use `-s <major>`:
```bash
$ lass -s 11 -n 100 # use the v11.0 schema and send 100 more alerts
```

To cleanup, delete the kafka container and network, and release resources, do

```bash
$ lass --quit
Removing container: lassbroker
Removing network: lassnet
```

For full usage info do `lass --help`: <!-- update with :r!lass --help -->
```bash
$ lass --help
usage: lass [-h] [-n NUMALERTS] [-p PRVDIASOURCES] [-f] [-s SCHEMAVERSION]
            [-v] [--quit] [--skipbroker]

▄▄     ▄▄▄   ▄▄▄▄  ▄▄▄▄
██    ██▀██ ███▄▄ ███▄▄
██▄▄▄ ██▀██ ▄▄██▀ ▄▄██▀
LSST Alert Stream Simulator
Simulate a Rubin LSST Alert Broker with fake alerts and a kafka stream.

options:
  -h, --help            show this help message and exit
  -n, --numalerts NUMALERTS
                        Number of alerts to generate. (default: 10)
  -p, --prvdiasources PRVDIASOURCES
                        Maximum number of prvDiaSources to generate in each alert. Actual number is random. (default: 0)
  -f, --fixed-prvdiasources
                        If set, all alerts will have the same number of prvDiaSources supplied. (default: False)
  -s, --schemaversion SCHEMAVERSION
                        The major version of the LSST alert schema to use.
                        Defaults to v10 and the earliest minor version (i.e. v10-0). (default: 10)
  -v, --version         show program's version number and exit
  --quit                Remove lass's kafka broker (along with the simulated alerts) (default: False)
  --skipbroker          If set, do not start a kafka broker. (default: False)
```

### **Python Interface**

The following is a minimal example of using the `lass` module to generate alerts, start 
the broker containter, and send alerts to the broker:

```python
import lass

alerts, alertsizes = lass.generate()
lass.startbroker()
lass.send(alerts)
lass.plotAlertSizeDistibution(alertsizes)
lass.pprint_alert(alerts[0], save=False)
lass.removebroker()
```

#### **Custom Alert Generation**

By default alerts are filled with nonsense data that is simply there to fill up bytes in a way that reflects real alerts sizes. This is useful for basic database benchmarking and bandwidth measurements, and not much else. 

If we want a set of test alerts for a database that enforces relations between alert fields, or test alerts for emulating real astrophysical transients, we must modify the alert so that it satifies our use case. These modifications can range from very minor to extensive changes. 

To allow for these use cases, the `lass.generate()` function accepts an `alertwriter` argument. The `alertwriter` is any function
that accepts an alert packet in the form of a Python dictionary and returns an alert packet. 
That is, for any given `alertwriter` function, `lass.generate()` calls 

```python
alert = alertwriter(alert)
```

after the schema compliant alert is generated. This means any `alertwriter` function can access and 
overwrite any of the alert fields, and even add new (non-schema compliant) fields to the packet.

The following is an example of an alert writer that makes sure a diaSource's diaObjectId matches its parent diaObjectId, i.e., an alert writer that enforces limited data consistency:

```python
def default_fastdb_writer(alert: dict[str, Any]):
    # ...
    # Match IDs
    alert['diaObject']['diaObjectId'] = secrets.randbits(63)
    alert['diaSource']['diaObjectId'] = alert['diaObject']['diaObjectId']
    alert['diaSourceId'] = secrets.randbits(63)
    alert['diaSource']['diaSourceId'] = alert['diaSourceId']
    return alert
```

The alert writer can then be used with

```python
alerts, alertsizes = lass.generate(
    alertwriter=default_fastdb_writer
)
```

This alert writer is in fact used as the default alert writer by `lass`, as it enforces this and other basic
data consistency modifications that are useful in all cases.

Because `alertwriter`s are just functions, they can be composed together to have cumulative effects.
For example, suppose we want the basic consistency edits that come from the default `lass.default_fastdb_writer` writer, but also want all of our alerts to appear to have come from the Small Magellanic Cloud. 

We could achieve this by creating our own alert writer that writes to the relevant `ra` and `dec` fields in the alert packet and compose it with `lass`'s built in writer:

```python
def smc_radec_writer(alert):
    alert = lass.default_fastdb_writer(alert)
    alert['diaObject']['dec'] = -72.6977
    alert['diaObject']['ra'] = 13.1867
    alert['diaSource']['dec'] = -72.6977
    alert['diaSource']['ra'] = 13.1867
    return alert

alerts, alertsizes = lass.generate(
    alertwriter=smc_radec_writer
)
print(alerts[0]['diaObject']['ra'], alerts[0]['diaObject']['dec']) #  13.1867 -72.6977
```

Alert cutouts, the postage stamp size images that come with alert packets, can also be written to in this way:

```python
def cutout_writer(alert):
    alert = lass.default_fastdb_writer(alert)
    alert["cutoutDifference"] = # ...
    alert["cutoutScience"]    = # ...
    alert["cutoutTemplate"]   = # ...
    return alert
```

### **Consuming alerts**
Consuming the alerts can be done from the host machine or from another container. 

To consume from the host machine, connect to `localhost:19092`. For example,

```python
import pickle
from confluent_kafka import Consumer
from tqdm import tqdm

c = Consumer(
    {
        "bootstrap.servers": "localhost:19092",
        "group.id": "mygroup",
        "auto.offset.reset": "earliest",
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
        # do whatever you want with the alert
        pbar.update()
        pbar.set_description("Received message: {}".format(alert["diaSourceId"]))
    c.close()
```

If consuming alerts from another container, you must instead connect your container to the `lassnet` network and connect to the kafka listener at `lassbroker:9092`. Note the differing port number to support both host and container connections. 

For example, if you have a container named `cool_lsst_database` running your bespoke database, connect your container to the `lassbroker` container with 

```bash
$ docker network connect lassnet cool_lsst_database
```

Your container code can now use `lassbroker:9092` as the kafka bootstrap server.

To change the configuration of the Kafka broker please modify `start-broker.sh`.

## Example Alerts

Examples of generated alerts can be found in the `docs/` folder of this repo. See for example [example-alert-v10.json](docs/example-alert-v10.json)
