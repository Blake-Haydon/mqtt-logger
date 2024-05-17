# MQTT to SQLite Logger

[![PyPI version](https://badge.fury.io/py/mqtt-logger.svg)](https://badge.fury.io/py/mqtt-logger)
[![Unit Tests](https://github.com/Blake-Haydon/mqtt-logger/actions/workflows/python-test.yml/badge.svg)](https://github.com/Blake-Haydon/mqtt-logger/actions/workflows/python-test.yml)
[![Upload Python Package](https://github.com/Blake-Haydon/mqtt-logger/actions/workflows/python-publish.yml/badge.svg)](https://github.com/Blake-Haydon/mqtt-logger/actions/workflows/python-publish.yml)
[![codecov](https://codecov.io/github/Blake-Haydon/mqtt-logger/graph/badge.svg?token=8PA3F5RWXA)](https://codecov.io/github/Blake-Haydon/mqtt-logger)

## Table of Contents

- [MQTT to SQLite Logger](#mqtt-to-sqlite-logger)
  - [Table of Contents](#table-of-contents)
  - [Description](#description)
  - [Installation](#installation)
  - [Example Usage](#example-usage)
    - [Recording MQTT Messages](#recording-mqtt-messages)
    - [Playback Recorded MQTT Messages](#playback-recorded-mqtt-messages)
  - [Database](#database)
    - [`LOG` Table](#log-table)
    - [`RUN` Table](#run-table)

## Description

`mqtt-logger` allows for asynchronous data logging of MQTT messages to a SQLite database. It also allows for the playback of previously recorded MQTT messages.

## Installation

To install `mqtt-logger` you can simply use `pip`.

```bash
pip install mqtt-logger
```

## Example Usage

### Recording MQTT Messages

This example records messages to the `test/#` topic using a public MQTT broker. It will record for 10 seconds. If you are using a private broker, you may need to set the `username` and `password` parameters.

<!-- poetry run python examples/10s_recording.py -->

_Example recorder taken from [examples/10s_recording.py](examples/10s_recording.py)_

```python
import mqtt_logger
import os
import time

# Initalise mqtt recorder object
rec = mqtt_logger.Recorder(
    sqlite_database_path=os.path.join(os.path.dirname(__file__), "MQTT_log.db"),
    topics=["test/#"],
    broker_address="broker.hivemq.com",
    verbose=True,

    ## Uncomment for TLS connection
    # port=8883,
	# use_tls=True,
	# tls_insecure=False,

    ## Uncomment for username and password
    # username="username",
    # password="password",
)

# Start the logger, wait 10 seconds and stop the logger
rec.start()
time.sleep(10)
rec.stop()
```

### Playback Recorded MQTT Messages

This example plays back previously recorded MQTT messages from `mqtt_logger.Recorder`. If you are using a private
broker, you may need to set the `username` and `password` parameters.

<!-- poetry run python examples/10s_playback.py -->

_Example recorder taken from [examples/10s_playback.py](examples/10s_playback.py)_

```python
import mqtt_logger
import os

# Initalise playback object
playback = mqtt_logger.Playback(
    sqlite_database_path=os.path.join(os.path.dirname(__file__), "MQTT_log.db"),
    broker_address="broker.hivemq.com",
    verbose=True,
)

# Start playback at 2x speed (twice as fast)
playback.play(speed=2)
```

## Database

The SQLite database has two tables called `LOG` and `RUN`. The `LOG` table contains the messages that are being logged. The `RUN` table contains the information about the current run of the program.

### `LOG` Table

| ROW NAME  | DESCRIPTION                                            |
| --------- | ------------------------------------------------------ |
| ID        | Unique number assigned to each message (ascending int) |
| RUN_ID    | ID of the current run (ascending int)                  |
| UNIX_TIME | Time when the message was received                     |
| TOPIC     | MQTT topic                                             |
| MESSAGE   | MQTT message received                                  |

---

### `RUN` Table

| ROW NAME        | DESCRIPTION                                   |
| --------------- | --------------------------------------------- |
| ID              | Unique number assigned to run (ascending int) |
| START_UNIX_TIME | Time when logger was started                  |
| END_UNIX_TIME   | Time when logger was stopped                  |

---
