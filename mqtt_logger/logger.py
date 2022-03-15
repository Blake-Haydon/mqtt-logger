import logging
import sqlite3
import asyncio
import time

import paho.mqtt.client as mqtt
from rich.logging import RichHandler

# Define Constants
LOGGER_TABLE_NAME = "LOG"

# Set logging to output all info by default (with a space for clarity)
logging.basicConfig(
    format="%(message)s",
    level=logging.WARNING,
    handlers=[RichHandler()],
)


class Recorder:
    """Record incoming MQTT messages on selected topics.

    Parameters
    ----------
    sqlite_database_path : str
        Filepath to the *.db file that sqlite uses to save data
    topics : List(str)
        A list containing the topic strings that are to be subscribed to
    broker_address : str
        The IP address that the MQTT broker lives on
    verbose : bool
        Set logging output to INFO level if True
    username : str
        Username for mqtt broker
    password : str
        Password for mqtt broker

    Attributes
    ----------
    topics : List(str)
        List of topic names
    _recording : bool
        Whether the Recorder object is currently recording or not
    _last_recorded_time : `float`
        The last recorded time so that new time deltas will always count forward and not start from 0 again
    _start_time : `float`
        The time that the Recorder object was created
    _con : `sqlite3.Connection`
        Connection object that updates the database file located at sqlite_database_path
    _cur : `sqlite3.Cursor`
        Database cursor object that updates the database file located at sqlite_database_path
    _client : `paho.mqtt.client`
        MQTT client that connects to the broker and recives the messages
    """

    def __init__(
        self,
        sqlite_database_path: str = "MQTT_log.db",
        topics: list = ["#"],
        broker_address: str = "localhost",
        verbose: bool = False,
        username: str = None,
        password: str = None,
    ) -> None:
        # If set to verbose print info messages
        if verbose:
            logging.getLogger().setLevel(logging.INFO)

        # Connect to sqlite database
        # check_same_thread needs to be false as the MQTT callbacks run on a different thread
        self._con = sqlite3.connect(sqlite_database_path, check_same_thread=False)
        self._cur = self._con.cursor()

        # Check to see if the logging database has been initiated
        contains_log_table = self._cur.execute(
            f"""
            SELECT name FROM sqlite_master WHERE type='table' AND name='{LOGGER_TABLE_NAME}'
            """
        ).fetchone()

        # Make a LOG table if none currently exists
        if contains_log_table is None:
            self._cur.execute(
                f"""
                CREATE TABLE {LOGGER_TABLE_NAME}
                (TIME_DELTA DECIMAL(15,6)   PRIMARY KEY         NOT NULL,
                TOPIC                       VARCHAR             NOT NULL,
                MESSAGE                     VARCHAR             NOT NULL);
                """
            )
            logging.info(f"Created table {LOGGER_TABLE_NAME} in {sqlite_database_path}")

        else:
            logging.warning(
                f"Table already exists, mqtt_logger will append to {sqlite_database_path}"
            )

        # The logger object can subscribe to many topics (if none are selected then it will subscribe to all)
        self.topics = topics

        # Save last recorded time so that time always moves forward. This is so logging can still function on a pi
        # where the real time clock may be reset
        self._start_time = time.monotonic()
        self._last_recorded_time = self._cur.execute(
            f"""
            SELECT MAX(TIME_DELTA) FROM {LOGGER_TABLE_NAME}
            """
        ).fetchone()[0]

        # This occurs when there are no records but the database has been setup
        if self._last_recorded_time is None:
            self._last_recorded_time = 0

        # Do not start logging when object is created (wait for start method)
        self._recording = False

        # Connect to MQTT broker
        self._client = mqtt.Client()
        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message
        if username is not None and password is not None:
            self._client.username_pw_set(username, password)
        self._client.connect(broker_address)
        self._client.loop_start()  # Threaded execution loop

    def _on_connect(self, client, userdata, flags, rc) -> None:
        """Callback function for MQTT broker on connection."""

        if rc == 0:
            logging.info("Connection Successful")
        else:
            raise ConnectionError(
                "Connection was unsuccessful, check that the broker IP is corrrect"
            )

        # Subscribe to all of the topics
        try:
            for topic in self.topics:
                self._client.subscribe(topic)
                logging.info(f"Subscribed to: {topic}")

        except Exception as e:
            logging.error(f"{type(e)}: {e}")

    def _on_message(self, client, userdata, msg) -> None:
        """Callback function for MQTT broker on message that logs the incoming MQTT message."""
        if self._recording:
            try:
                self.log(msg.topic, msg.payload.decode("utf-8"))

            except Exception as e:
                logging.error(f"{type(e)}: {e}")

    def log(self, mqtt_topic: str, message: str) -> None:
        """Logs the time delta and message data to the local sqlite database.

        Parameters
        ----------
        mqtt_topic : str
            Incoming topic to be recorded
        message : str
            Corresponding message to be recorded
        """
        time_delta = time.monotonic() - self._start_time + self._last_recorded_time

        # Write data to csv file
        try:
            self._cur.execute(
                f"INSERT INTO {LOGGER_TABLE_NAME} VALUES (?, ?, ?)",
                (time_delta, mqtt_topic, message),
            )
            self._con.commit()
            logging.info(f"{mqtt_topic}: {message}")

        except Exception as e:
            logging.error(f"{type(e)}: {e} \nFailed to log {mqtt_topic}: {message}")

    def start(self) -> None:
        """Starts the MQTT logging."""
        self._recording = True
        logging.info("Logging started")

    def stop(self) -> None:
        """Graceful exit for closing the database connection and stopping the MQTT client."""
        self._recording = False
        logging.info("Logging stopped")
        self._client.loop_stop()
        logging.info("MQTT broker disconnected")
        self._con.close()
        logging.info("Database connection closed")


class Playback:
    """Playback MQTT messages from log files in realtime or faster.

    Parameters
    ----------
    sqlite_database_path : str
        Filepath to the *.db file that sqlite uses to save data
    broker_address : str
        The IP address of the MQTT broker
    verbose : bool
        Set logging output to INFO level if True
    username : str
        Username for mqtt broker
    password : str
        Password for mqtt broker

    Attributes
    ----------
    _client : `paho.mqtt.client`
        MQTT client that connects to the broker and recives the messages
    _log_data: list(dict)
        A list of all of the rows in the log file stored in dict format
    """

    def __init__(
        self,
        sqlite_database_path: str = "MQTT_log.db",
        broker_address: str = "localhost",
        verbose: bool = False,
        username: str = None,
        password: str = None,
    ) -> None:

        # If set to verbose print info messages
        if verbose:
            logging.getLogger().setLevel(logging.INFO)

        # Connect to sqlite database and get all records from the LOG table
        conn = sqlite3.connect(sqlite_database_path)
        all_log_records = conn.execute("""SELECT * FROM LOG""").fetchall()

        # Convert list of tuples in list of dicts
        self._log_data = []
        for record in all_log_records:
            self._log_data.append(
                {"time_delta": record[0], "mqtt_topic": record[1], "message": record[2]}
            )

        # Connect to MQTT broker
        self._client = mqtt.Client()
        if username is not None and password is not None:
            self._client.username_pw_set(username, password)
        self._client.on_connect = self._on_connect
        self._client.connect(broker_address)
        self._client.loop_start()  # Threaded execution loop

    def _on_connect(self, client, userdata, flags, rc) -> None:
        """Callback function for MQTT broker on connection."""

        if rc == 0:
            logging.info("Connection Successful")
        else:
            raise ConnectionError(
                "Connection was unsuccessful, check that the broker IP is corrrect"
            )

    def play(self, speed: float = 1) -> None:
        """Play the logged data at a certain speed using an async function.

        Parameters
        ----------
        speed : float
            Speed multiplier to determine how fast to send out the data. A higher value means faster.
        """

        logging.info(f"Playback initiated at {speed}x speed")

        # Run the event loop to issue out all of the MQTT publishes
        asyncio.run(self._publish(speed))

    async def _publish(self, speed: float) -> None:
        """Async function that collects all the necessary publish functions and gathers them to then be run by the
        event loop.

        Parameters
        ----------
        speed : float
            Speed multiplier to determine how fast to send out the data. A higher value means faster.
        """

        async def _publish_aux(row):
            scaled_sleep = row["time_delta"] / speed
            await asyncio.sleep(scaled_sleep)

            try:
                self._client.publish(row["mqtt_topic"], row["message"])
                logging.info(f"{row['mqtt_topic']}: {row['message']}")

            except Exception as e:
                logging.error(
                    f"{type(e)}: {e} \nFailed to publish {row['mqtt_topic']}: {row['message']}"
                )

        publish_queue = []
        for row in self._log_data:
            publish_queue.append(_publish_aux(row))

        # Load all async operation at the start using gather
        await asyncio.gather(*publish_queue, return_exceptions=True)
