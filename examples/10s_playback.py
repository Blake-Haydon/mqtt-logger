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
