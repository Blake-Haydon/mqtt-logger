import sqlite3
import time


LOGGER_TABLE_NAME = "LOG"


def create_log_table(cursor: sqlite3.Cursor):
    """Creates log table in the database."""

    query = f"""
        CREATE TABLE {LOGGER_TABLE_NAME}
        (ID                         INTEGER             PRIMARY KEY,
        UNIX_TIME                   DECIMAL(15,6)       NOT NULL,
        TOPIC                       VARCHAR             NOT NULL,
        MESSAGE                     BLOB                NOT NULL);
        """

    cursor.execute(query)


def log_table_exists(cursor: sqlite3.Cursor) -> bool:
    """Checks if the LOG table exists in the database."""

    query = f"""
        SELECT name FROM sqlite_master WHERE type='table' AND name='{LOGGER_TABLE_NAME}'
        """

    if cursor.execute(query).fetchone() is None:
        return False

    return True


def insert_log_entry(cursor: sqlite3.Cursor, topic: str, message: bytes):
    """Inserts a log entry into the database."""

    query = f"""
        INSERT INTO {LOGGER_TABLE_NAME}
        (UNIX_TIME, TOPIC, MESSAGE)
        VALUES (?, ?, ?)
        """

    # NOTE: time.time() will not be the correct time if the system clock is reset (i.e on a raspberry pi)
    cursor.execute(query, (time.time(), topic, message))
    cursor.commit()


def retrieve_log_entries(cursor: sqlite3.Cursor) -> list[dict]:
    """Retrieves all log entries from the database."""

    query = f"""
        SELECT UNIX_TIME, TOPIC, MESSAGE FROM {LOGGER_TABLE_NAME}
        """

    # Convert list of tuples into a list of dicts
    log_data = []
    for record in cursor.execute(query).fetchall():
        log_data.append(
            {
                "unix_time": record[0],
                "topic": record[1],
                "message": record[2],
            }
        )

    return log_data


def start_time(cursor: sqlite3.Cursor) -> float:
    """Retrieves the logging start time"""

    query = f"""
        SELECT MIN(UNIX_TIME) FROM {LOGGER_TABLE_NAME}
        """

    return cursor.execute(query).fetchone()[0]
