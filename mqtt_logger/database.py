import sqlite3
import time
import logging
from typing import Optional, List, Set


LOGGER_TABLE_NAME = "LOG"
RUNS_TABLE_NAME = "RUN"


def create_tables(con: sqlite3.Connection):
    """Initialise the database by creating the necessary tables for logging."""
    cur = con.cursor()

    log_query = f"""
        CREATE TABLE {LOGGER_TABLE_NAME}
        (ID                         INTEGER             PRIMARY KEY,
        RUN_ID                      INTEGER             NOT NULL,
        UNIX_TIME                   DECIMAL(15,6)       NOT NULL,
        TOPIC                       VARCHAR             NOT NULL,
        MESSAGE                     BLOB                NOT NULL);
    """

    run_query = f"""
        CREATE TABLE {RUNS_TABLE_NAME}
        (ID                         INTEGER             PRIMARY KEY,
        START_UNIX_TIME             DECIMAL(15,6)       NOT NULL,
        END_UNIX_TIME               DECIMAL(15,6));
        """

    cur.execute(log_query)
    cur.execute(run_query)

    # Commit database tables to the database
    con.commit()


def tables_exist(con: sqlite3.Connection) -> bool:
    """Checks if the `LOG` and `RUN` tables exists in the database."""
    cur = con.cursor()

    log_query = f"""
        SELECT name FROM sqlite_master WHERE type='table' AND name='{LOGGER_TABLE_NAME}'
        """

    run_query = f"""
        SELECT name FROM sqlite_master WHERE type='table' AND name='{RUNS_TABLE_NAME}'
        """

    log_exists = cur.execute(log_query).fetchone() is not None
    run_exists = cur.execute(run_query).fetchone() is not None

    if log_exists != run_exists:
        raise RuntimeError(
            "Tables must exist together. Check that the database is either empty or contains both tables."
        )

    return log_exists  # or run_exists could be used as they are equivalent


def start_run_entry(con: sqlite3.Connection) -> Optional[int]:
    """Inserts a run entry into the database. Returns the run id."""
    cur = con.cursor()

    query = f"""
        INSERT INTO {RUNS_TABLE_NAME}
        (START_UNIX_TIME)
        VALUES ({time.time()})
        """

    cur.execute(query)
    con.commit()
    return cur.lastrowid


def stop_run_entry(con: sqlite3.Connection, run_id: Optional[int]):
    """Inserts a run entry into the database. Returns the run id."""
    cur = con.cursor()

    query = f"""
        UPDATE {RUNS_TABLE_NAME}
        SET END_UNIX_TIME = {time.time()}
        WHERE ROWID = {run_id}
        """

    if run_id is None:
        raise ValueError("run_id must be provided.")
    cur.execute(query)
    con.commit()


def insert_log_entry(con: sqlite3.Connection, topic: str, message: bytes, run_id: Optional[int]):
    """Inserts a log entry into the database."""
    cur = con.cursor()

    query = f"""
        INSERT INTO {LOGGER_TABLE_NAME}
        (UNIX_TIME, TOPIC, MESSAGE, RUN_ID)
        VALUES ({time.time()}, ?, ?, ?)
        """

    # NOTE: time.time() will not be the correct time if the system clock is reset (i.e on a raspberry pi)
    if run_id is None:
        raise ValueError("run_id must be provided.")

    if run_id not in run_ids(con):
        logging.warning(
            f"Run ID {run_id} does not exist in the database, please call start_run_entry() first."
        )

    cur.execute(query, (topic, message, run_id))
    con.commit()


def retrieve_log_entries(con: sqlite3.Connection, patterns: Optional[List[str]] = None) -> list:
    """Retrieves all log entries from the database."""
    cur = con.cursor()

    query = f"""
        SELECT UNIX_TIME, TOPIC, MESSAGE FROM {LOGGER_TABLE_NAME}
        """

    if patterns is not None:
        query += " WHERE " + " OR ".join(
            [f"TOPIC LIKE '{pattern}' " for pattern in patterns]
        )

    # Convert list of tuples into a list of dicts
    return [
        {
            "unix_time": record[0],
            "topic": record[1],
            "message": record[2],
        }
        for record in cur.execute(query).fetchall()
    ]


def start_time(con: sqlite3.Connection) -> float:
    """Retrieves the logging start time."""
    cur = con.cursor()

    # TODO: Implement this with run numbers
    query = f"""
        SELECT MIN(UNIX_TIME) FROM {LOGGER_TABLE_NAME}
        """

    return cur.execute(query).fetchone()[0]


def run_ids(con: sqlite3.Connection) -> Set[int]:
    """Retrieves all run ids from the database."""
    cur = con.cursor()

    query = f"""SELECT ID FROM {RUNS_TABLE_NAME}"""

    all_run_ids = cur.execute(query).fetchall()
    return {record[0] for record in all_run_ids}
