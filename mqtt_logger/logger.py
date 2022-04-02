import logging
import sqlite3
import asyncio

import paho.mqtt.client as mqtt
from rich.logging import RichHandler

from mqtt_logger.database import (
    LOGGER_TABLE_NAME,
    create_log_table,
    insert_log_entry,
    log_table_exists,
    retrieve_log_entries,
    start_time,
)

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
    topics : list(str)
        List of topic names
    _recording : bool
        Whether the Recorder object is currently recording or not
    _con : sqlite3.Connection
        Connection object that updates the database file located at sqlite_database_path
    _cur : sqlite3.Cursor
        Database cursor object that updates the database file located at sqlite_database_path
    _client : paho.mqtt.client
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
    ):
        # If set to verbose print info messages
        if verbose:
            logging.getLogger().setLevel(logging.INFO)

        # Connect to sqlite database
        # check_same_thread needs to be false as the MQTT callbacks run on a different thread
        self._con = sqlite3.connect(sqlite_database_path, check_same_thread=False)
        self._cur = self._con.cursor()

        # Make a LOG table if none currently exists
        if log_table_exists(self._cur):
            logging.warning(
                f"Table already exists, mqtt_logger will append to {sqlite_database_path}"
            )
        else:
            create_log_table(self._cur)
            logging.info(f"Created table {LOGGER_TABLE_NAME} in {sqlite_database_path}")

        # The logger object can subscribe to many topics (if none are selected then it will subscribe to all)
        self.topics = topics

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

    def _on_connect(self, client, userdata, flags, rc):
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

    def _on_message(self, client, userdata, msg):
        """Callback function for MQTT broker on message that logs the incoming MQTT message."""
        if self._recording:
            try:
                insert_log_entry(self._con, msg.topic, msg.payload)
                logging.info(f"{len(msg.payload):>4} bytes <- {msg.topic}")

            except Exception as e:
                logging.error(f"{type(e)}: {e}")

    def start(self):
        """Starts the MQTT logging."""
        # TODO: ADD RECORDING NUMBER

        if self._recording:
            raise RuntimeError("Already recording")

        self._recording = True
        logging.info("Logging started")

    def stop(self):
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
    _con : sqlite3.Connection
        Connection object that updates the database file located at sqlite_database_path
    _cur : sqlite3.Cursor
        Database cursor object that updates the database file located at sqlite_database_path
    _client : paho.mqtt.client
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
    ):

        # If set to verbose print info messages
        if verbose:
            logging.getLogger().setLevel(logging.INFO)

        # Connect to sqlite database
        # check_same_thread needs to be false as the MQTT callbacks run on a different thread
        self._con = sqlite3.connect(sqlite_database_path, check_same_thread=False)
        self._cur = self._con.cursor()

        # Retrieve all of the log entries from the database
        self._log_data = retrieve_log_entries(self._cur)

        # Connect to MQTT broker
        self._client = mqtt.Client()
        if username is not None and password is not None:
            self._client.username_pw_set(username, password)
        self._client.on_connect = self._on_connect
        self._client.connect(broker_address)
        self._client.loop_start()  # Threaded execution loop

    def _on_connect(self, client, userdata, flags, rc):
        """Callback function for MQTT broker on connection."""

        if rc == 0:
            logging.info("Connection Successful")
        else:
            raise ConnectionError(
                "Connection was unsuccessful, check that the broker IP is corrrect"
            )

    def play(self, speed: float = 1):
        """Play the logged data at a certain speed using an async function.

        Parameters
        ----------
        speed : float
            Speed multiplier to determine how fast to send out the data. A higher value means faster.
        """

        logging.info(f"Playback initiated at {speed}x speed")

        # Run the event loop to issue out all of the MQTT publishes
        asyncio.run(self._publish(speed))

    async def _publish(self, speed: float):
        """Async function that collects all the necessary publish functions and gathers them to then be run by the
        event loop.

        Parameters
        ----------
        speed : float
            Speed multiplier to determine how fast to send out the data. A higher value means faster.
        """
        start_unix_time = start_time(self._cur)

        async def _publish_aux(log):
            scaled_sleep = (log["unix_time"] - start_unix_time) / speed
            await asyncio.sleep(scaled_sleep)

            try:
                self._client.publish(log["topic"], log["message"])
                logging.info(f"{len(log['message']):>4} bytes -> {log['topic']}")

            except Exception as e:
                logging.error(f"{type(e)}: {e}")

        # Load all async operation at the start using gather
        publish_queue = [_publish_aux(log) for log in self._log_data]
        await asyncio.gather(*publish_queue, return_exceptions=True)
