# MQTT to SQLite Logger

[![PyPI version](https://badge.fury.io/py/mqtt-logger.svg)](https://badge.fury.io/py/mqtt-logger)
[![Python package](https://github.com/Blake-Haydon/mqtt-logger/actions/workflows/python-package.yml/badge.svg)](https://github.com/Blake-Haydon/mqtt-logger/actions/workflows/python-package.yml)
[![Upload Python Package](https://github.com/Blake-Haydon/mqtt-logger/actions/workflows/python-publish.yml/badge.svg)](https://github.com/Blake-Haydon/mqtt-logger/actions/workflows/python-publish.yml)

## Table of Contents
- [MQTT to SQLite Logger](#mqtt-to-sqlite-logger)
  - [Table of Contents](#table-of-contents)
  - [Description](#description)
    - [`LOG` Table](#log-table)
    - [`RUN` Table](#run-table)
  - [Installation](#installation)
  - [Example Usage](#example-usage)
    - [Recording MQTT Messages](#recording-mqtt-messages)
    - [Playback Recorded MQTT Messages](#playback-recorded-mqtt-messages)
  - [Unit Tests](#unit-tests)

## Description

`mqtt-logger` allows for asynchronous data logging of MQTT messages to a SQLite database. The SQLite database has two 
tables called `LOG` and `RUN`. The `LOG` table contains the messages that are being logged. The `RUN` table contains 
the information about the current run of the program.

### `LOG` Table

| ROW NAME  | DESCRIPTION                                            |
| --------- | ------------------------------------------------------ |
| ID        | Unique number assigned to each message (ascending int) |
| RUN_ID    | ID of the current run (ascending int)                  |
| UNIX_TIME | Time when the message was received                     |
| TOPIC     | MQTT topic                                             |
| MESSAGE   | MQTT message received                                  |

### `RUN` Table

| ROW NAME        | DESCRIPTION                                   |
| --------------- | --------------------------------------------- |
| ID              | Unique number assigned to run (ascending int) |
| START_UNIX_TIME | Time when logger was started                  |
| END_UNIX_TIME   | Time when logger was stopped                  |


--- 


## Installation

If you are using `mqtt-logger` as a python package, you can install it using pip.

```bash
# To use as a package
pip install mqtt-logger
```

If you are looking to develop `mqtt-logger`, clone and run the following commands (poetry must be installed). 

```bash
# For development work
git clone git@github.com:Blake-Haydon/mqtt-logger.git
git config --local core.hooksPath .githooks/
poetry install
```

---


## Example Usage

### Recording MQTT Messages

This example records messages to the `test/#` topic using a public MQTT broker. It will record for 10 seconds. If you 
are using a private broker, you may need to set the `username` and `password` parameters.

```bash
# Run example in terminal
poetry run python examples/10s_recording.py
```

Example recorder taken from [examples/10s_recording.py](examples/10s_recording.py)
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
)

# Start the logger, wait 10 seconds and stop the logger
rec.start()
time.sleep(10)
rec.stop()
```

### Playback Recorded MQTT Messages

This example plays back previously recorded MQTT messages from `mqtt_logger.Recorder`. If you are using a private 
broker, you may need to set the `username` and `password` parameters.

```bash
# Run example in terminal after running the recorder example
poetry run python examples/10s_playback.py
```

Example recorder taken from [examples/10s_playback.py](examples/10s_playback.py)
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


---


## Unit Tests

```bash
# Run tests in poetry virtual environment
poetry run pytest
```