# LSST Alert Stream Simulator (lass)

`lass` simulates Legacy Survey of Space and Time (LSST) alerts, creates a Kafka broker, and sends them to it. Scripts or external tools can then be used to ingest the alerts from the lass broker
This simulates the community broker architecture. 

The goal of `lass` is to serve as a tool for stress testing databases that store LSST alerts. One such database is [LSST-DESC's FASTDB](https://github.com/LSSTDESC/FASTDB).

## Features:
* Generate millions of alerts with random (unphysical) data, aiming for realistic alert sizes on the order of 82 kb each.
* LSST alerts are currently simulated using the [CanDIAPL fork](https://github.com/CanDIAPL/alert_packet) of LSST's [alert_packet repo](https://github.com/lsst/alert_packet). This enables easy switching of alert schemas and ensuring compliance with the schema as the format changes.

## Installation

```bash
git clone https://github.com/CanDIAPL/lass
cd lass && pip install --editable . 
```

At the moment, the code is the interface so installing with `--editable` is a good idea.

To use the Kafka broker you must have [Docker](https://docs.docker.com/engine/install/) installed.
## Usage:
The interface is still in flux but you can start a kafka broker using docker with 

```bash
./start-broker.sh
``` 

and then generate and post messages to the broker using 

```bash
python src/lass/lassproducer.py
``` 

or you can use the `lass` command line tool that is installed.

To change the number of alerts generated, modify `src/lassproducer.py:main`. The default is 10,000 alerts, or about a few seconds worth of LSST alerts.

To change the configuration of the Kafka broker please modify `start-broker.sh`.
