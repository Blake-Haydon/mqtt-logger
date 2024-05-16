import os
import pytest
import sqlite3

import mqtt_logger


def connect_to_database(tmp_path):
    """Helper function to create and connect to a fresh database."""
    sqlite_database_path = os.path.join(tmp_path, "MQTT_log.db")
    con = sqlite3.connect(sqlite_database_path)
    cur = con.cursor()
    return con, cur


def test_create_tables(tmp_path):
    con, cur = connect_to_database(tmp_path)

    num_tables_query = f"""
        SELECT COUNT(*) AS table_count
        FROM sqlite_master
        WHERE type = 'table';
        """

    log_query = f"""
        SELECT name
        FROM sqlite_master
        WHERE type='table'
        AND name='{mqtt_logger.database.LOGGER_TABLE_NAME}'
        """

    run_query = f"""
        SELECT name
        FROM sqlite_master
        WHERE type='table'
        AND name='{mqtt_logger.database.RUNS_TABLE_NAME}'
        """

    # Check that the tables do not exist yet
    table_count = cur.execute(num_tables_query).fetchone()[0]
    assert table_count == 0

    # Create the logging tables
    mqtt_logger.database.create_tables(con)

    # Check that the tables were created
    table_count = cur.execute(num_tables_query).fetchone()[0]
    log_exists = cur.execute(log_query).fetchone() is not None
    run_exists = cur.execute(run_query).fetchone() is not None

    assert table_count == 2
    assert log_exists
    assert run_exists


def test_tables_exist(tmp_path):
    con, cur = connect_to_database(tmp_path)

    run_query = f"""CREATE TABLE {mqtt_logger.database.RUNS_TABLE_NAME} (ID INTEGER PRIMARY KEY);"""
    log_query = f"""CREATE TABLE {mqtt_logger.database.LOGGER_TABLE_NAME} (ID INTEGER PRIMARY KEY);"""
    rand_query = f"""CREATE TABLE rand (ID INTEGER PRIMARY KEY);"""

    # No Tables are present
    assert not mqtt_logger.database.tables_exist(con)

    # Add the random table
    cur.execute(rand_query)

    # Only the random table exists
    assert not mqtt_logger.database.tables_exist(con)

    # Add only the run table
    cur.execute(run_query)

    # Tables do not exist together (raise exception)
    # This should never really happen, but it is a good check
    with pytest.raises(Exception):
        mqtt_logger.database.tables_exist(con)

    # Add the log table (both tables exist now)
    cur.execute(log_query)

    # The tables now both exist and should be detected
    assert mqtt_logger.database.tables_exist(con)


def test_start_run_entry(tmp_path):
    con, cur = connect_to_database(tmp_path)

    # Create the logging tables
    mqtt_logger.database.create_tables(con)

    # Run id starts at 1 and increments by 1
    for i in range(1, 10):
        run_id = mqtt_logger.database.start_run_entry(con)
        assert run_id == i


def test_stop_run_entry(tmp_path):
    con, cur = connect_to_database(tmp_path)

    # Create the logging tables
    mqtt_logger.database.create_tables(con)

    # Run id starts at 1 and increments by 1
    for i in range(1, 10):
        run_id = mqtt_logger.database.start_run_entry(con)

    # Stop the runs in reverse order
    for i in range(9, 0, -1):
        query = f"""
            SELECT *
            FROM {mqtt_logger.database.RUNS_TABLE_NAME}
            WHERE ID = {i}
            """

        # Check that the run was not stopped
        final_time = cur.execute(query).fetchone()[2]
        assert final_time is None, f"Run {i} was not stopped"

        # Stop the run
        mqtt_logger.database.stop_run_entry(con, i)

        # Check that the run was stopped
        final_time = cur.execute(query).fetchone()[2]
        assert final_time is not None, f"Run {i} was not stopped"


def test_insert_log_entry(tmp_path):
    con, cur = connect_to_database(tmp_path)

    # Inserting a log without a database setup should raise an error
    with pytest.raises(sqlite3.OperationalError):
        mqtt_logger.database.insert_log_entry(con, "test_topic", b"test_message", -1)

    # Create the logging tables
    mqtt_logger.database.create_tables(con)

    # Inserting a log for a run that does not exist should log a warning, but not raise an error
    mqtt_logger.database.insert_log_entry(con, "test_topic", b"test_message", -1)

    # Start a run
    run_id = mqtt_logger.database.start_run_entry(con)

    # Insert a log entry
    mqtt_logger.database.insert_log_entry(con, "test_topic", b"test_message", run_id)

    # Check that the log entry was inserted
    query = f"""
        SELECT RUN_ID, TOPIC, MESSAGE
        FROM {mqtt_logger.database.LOGGER_TABLE_NAME}
        WHERE RUN_ID = {run_id}
        """

    log_entry = cur.execute(query).fetchone()

    assert log_entry[0] == run_id
    assert log_entry[1] == "test_topic"
    assert log_entry[2] == b"test_message"

    # Stop the run
    mqtt_logger.database.stop_run_entry(con, run_id)

    # Check that the log entry is still present
    query = f"""
        SELECT RUN_ID, TOPIC, MESSAGE
        FROM {mqtt_logger.database.LOGGER_TABLE_NAME}
        WHERE RUN_ID = {run_id}
        """

    log_entry = cur.execute(query).fetchone()

    assert log_entry[0] == run_id
    assert log_entry[1] == "test_topic"
    assert log_entry[2] == b"test_message"


def test_retrieve_log_entries(tmp_path):
    pytest.skip("need to implement test")


def test_start_time(tmp_path):
    pytest.skip("need to implement test")


def test_run_ids(tmp_path):
    con, cur = connect_to_database(tmp_path)

    # Create the logging tables
    mqtt_logger.database.create_tables(con)

    # Check that the correct number of runs are returned
    for num_runs in range(1, 10):
        mqtt_logger.database.start_run_entry(con)
        assert len(mqtt_logger.database.run_ids(con)) == num_runs
