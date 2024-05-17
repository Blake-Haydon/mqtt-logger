import mqtt_logger
import os
import time

# Initalise mqtt recorder object
rec = mqtt_logger.Recorder(
    sqlite_database_path=os.path.join(os.path.dirname(__file__), "MQTT_log.db"),
    topics=["test/#"],
    broker_address="broker.hivemq.com",
    verbose=True,
    # port=1883,
    # use_tls=True,
    # tls_insecure=False,
    # username="username",
    # password="password",
)

# Start the logger, wait 10 seconds and stop the logger
rec.start()
time.sleep(10)
rec.stop()
