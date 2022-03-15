# MQTT to SQLite Logger

## Table of Contents
- [MQTT to SQLite Logger](#mqtt-to-sqlite-logger)
  - [Table of Contents](#table-of-contents)
  - [Installation](#installation)
  - [Example Usage](#example-usage)
    - [Recording MQTT Messages](#recording-mqtt-messages)
    - [Playback Recorded MQTT Messages](#playback-recorded-mqtt-messages)
  - [Unit Tests](#unit-tests)

## Installation

```bash
pip install mqtt-logger
```

## Example Usage

### Recording MQTT Messages

This example records messages to the `test/#` topic using a public MQTT broker. It will record for 10 seconds. If you are using a private broker, you may need to set the `username` and `password` parameters.

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
    # username="username",
    # password="password",
)

# Start the logger, wait 10 seconds and stop the logger
rec.start()
time.sleep(10)
rec.stop()
```

### Playback Recorded MQTT Messages

This example plays back previously recorded MQTT messages from `mqtt_logger.Recorder`. If you are using a private broker, you may need to set the `username` and `password` parameters.

```python
import mqtt_logger
import os

# Initalise playback object
playback = mqtt_logger.Playback(
    sqlite_database_path=os.path.join(os.path.dirname(__file__), "MQTT_log.db"),
    broker_address="broker.hivemq.com",
    verbose=True,
    # username="username",
    # password="password",
)

# Start playback at 2x speed (twice as fast)
playback.play(speed=2)
```

## Unit Tests

```bash
# Run tests in poetry virtual environment
poetry run pytest
```