import os
import pytest

import mqtt_logger

# FIXME: Use fake broker to test, not a public one
TEST_BROKER_ADDRESS = "broker.hivemq.com"


def test_version():
    assert mqtt_logger.__version__ == "0.3.4"


def test_basic_instantiation(tmp_path):
    mqtt_logger.Recorder(
        sqlite_database_path=os.path.join(tmp_path, "MQTT_log.db"),
        topics=["test/#"],
        broker_address=TEST_BROKER_ADDRESS,
    )

    mqtt_logger.Playback(
        sqlite_database_path=os.path.join(tmp_path, "MQTT_log.db"),
        broker_address=TEST_BROKER_ADDRESS,
    )


def test_playback_without_database(tmp_path):
    with pytest.raises(Exception):
        mqtt_logger.Playback(sqlite_database_path=os.path.join(tmp_path, "MQTT_log.db"))
