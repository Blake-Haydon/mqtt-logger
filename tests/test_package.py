import pytest

import mqtt_logger


def test_version():
    assert mqtt_logger.__version__ == "0.3.5"
