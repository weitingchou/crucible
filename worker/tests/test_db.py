"""Tests for worker.db — shared PostgreSQL helpers."""

from unittest.mock import patch, MagicMock, call

import pytest

from worker.db import (
    _get_conn,
    acquire_sut_lock,
    get_ready_count,
    get_run_status,
    get_start_signal,
    increment_completed_and_check,
    increment_ready_worker,
    init_waiting_room,
    release_sut_lock,
    set_start_signal,
    update_run_status,
)


@pytest.fixture(autouse=True)
def reset_conn():
    """Reset the module-level connection before each test."""
    import worker.db as db_mod
    db_mod._conn = None
    yield
    db_mod._conn = None


def _mock_conn():
    conn = MagicMock()
    conn.closed = False
    cursor = MagicMock()
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return conn, cursor


# ---------------------------------------------------------------------------
# _get_conn — lazy creation and reuse
# ---------------------------------------------------------------------------

def test_get_conn_creates_connection():
    with patch("worker.db.psycopg2.connect") as mock_connect:
        mock_connect.return_value = MagicMock(closed=False)
        conn = _get_conn()
        mock_connect.assert_called_once()
        assert conn is not None


def test_get_conn_reuses_existing_connection():
    with patch("worker.db.psycopg2.connect") as mock_connect:
        mock_conn = MagicMock(closed=False)
        mock_connect.return_value = mock_conn
        conn1 = _get_conn()
        conn2 = _get_conn()
        assert conn1 is conn2
        mock_connect.assert_called_once()


def test_get_conn_reconnects_when_closed():
    with patch("worker.db.psycopg2.connect") as mock_connect:
        # First connection
        mock_conn1 = MagicMock(closed=False)
        mock_conn2 = MagicMock(closed=False)
        mock_connect.side_effect = [mock_conn1, mock_conn2]
        conn1 = _get_conn()
        # Simulate close
        mock_conn1.closed = True
        conn2 = _get_conn()
        assert conn2 is mock_conn2
        assert mock_connect.call_count == 2


# ---------------------------------------------------------------------------
# update_run_status
# ---------------------------------------------------------------------------

def test_update_run_status_basic():
    conn, cursor = _mock_conn()
    with patch("worker.db._get_conn", return_value=conn):
        update_run_status("r1", "EXECUTING")
    cursor.execute.assert_called_once()
    sql = cursor.execute.call_args[0][0]
    assert "status = %s" in sql
    assert cursor.execute.call_args[0][1] == ["EXECUTING", "r1"]
    conn.commit.assert_called_once()


def test_update_run_status_with_error_detail():
    conn, cursor = _mock_conn()
    with patch("worker.db._get_conn", return_value=conn):
        update_run_status("r1", "FAILED", error_detail="timeout")
    sql = cursor.execute.call_args[0][0]
    assert "error_detail = %s" in sql
    assert cursor.execute.call_args[0][1] == ["FAILED", "timeout", "r1"]


def test_update_run_status_with_started_at():
    conn, cursor = _mock_conn()
    with patch("worker.db._get_conn", return_value=conn):
        update_run_status("r1", "EXECUTING", set_started_at=True)
    sql = cursor.execute.call_args[0][0]
    assert "started_at = now()" in sql


def test_update_run_status_with_completed_at():
    conn, cursor = _mock_conn()
    with patch("worker.db._get_conn", return_value=conn):
        update_run_status("r1", "COMPLETED", set_completed_at=True)
    sql = cursor.execute.call_args[0][0]
    assert "completed_at = now()" in sql


# ---------------------------------------------------------------------------
# init_waiting_room
# ---------------------------------------------------------------------------

def test_init_waiting_room():
    conn, cursor = _mock_conn()
    with patch("worker.db._get_conn", return_value=conn):
        init_waiting_room("r1", 4)
    sql = cursor.execute.call_args[0][0]
    assert "INSERT INTO waiting_room" in sql
    assert "completed_count" in sql
    assert cursor.execute.call_args[0][1] == ("r1", 4)
    conn.commit.assert_called_once()


# ---------------------------------------------------------------------------
# get_ready_count
# ---------------------------------------------------------------------------

def test_get_ready_count_returns_value():
    conn, cursor = _mock_conn()
    cursor.fetchone.return_value = (3,)
    with patch("worker.db._get_conn", return_value=conn):
        count = get_ready_count("r1")
    assert count == 3


def test_get_ready_count_returns_zero_when_missing():
    conn, cursor = _mock_conn()
    cursor.fetchone.return_value = None
    with patch("worker.db._get_conn", return_value=conn):
        count = get_ready_count("r1")
    assert count == 0


# ---------------------------------------------------------------------------
# set_start_signal / get_start_signal
# ---------------------------------------------------------------------------

def test_set_start_signal():
    conn, cursor = _mock_conn()
    with patch("worker.db._get_conn", return_value=conn):
        set_start_signal("r1", "START")
    sql = cursor.execute.call_args[0][0]
    assert "UPDATE waiting_room SET signal" in sql
    conn.commit.assert_called_once()


def test_get_start_signal_returns_value():
    conn, cursor = _mock_conn()
    cursor.fetchone.return_value = ("START",)
    with patch("worker.db._get_conn", return_value=conn):
        result = get_start_signal("r1")
    assert result == "START"


def test_get_start_signal_returns_wait_when_missing():
    conn, cursor = _mock_conn()
    cursor.fetchone.return_value = None
    with patch("worker.db._get_conn", return_value=conn):
        result = get_start_signal("r1")
    assert result == "WAIT"


# ---------------------------------------------------------------------------
# increment_ready_worker
# ---------------------------------------------------------------------------

def test_increment_ready_worker():
    conn, cursor = _mock_conn()
    with patch("worker.db._get_conn", return_value=conn):
        increment_ready_worker("r1")
    sql = cursor.execute.call_args[0][0]
    assert "ready_count = ready_count + 1" in sql
    conn.commit.assert_called_once()


# ---------------------------------------------------------------------------
# increment_completed_and_check
# ---------------------------------------------------------------------------

def test_increment_completed_and_check_returns_true_when_all_done():
    conn, cursor = _mock_conn()
    cursor.fetchone.return_value = (4, 4)  # completed_count == target_count
    with patch("worker.db._get_conn", return_value=conn):
        result = increment_completed_and_check("r1")
    assert result is True
    sql = cursor.execute.call_args[0][0]
    assert "completed_count = completed_count + 1" in sql
    assert "RETURNING" in sql


def test_increment_completed_and_check_returns_false_when_not_done():
    conn, cursor = _mock_conn()
    cursor.fetchone.return_value = (2, 4)  # 2 of 4
    with patch("worker.db._get_conn", return_value=conn):
        result = increment_completed_and_check("r1")
    assert result is False


def test_increment_completed_and_check_returns_true_when_no_row():
    conn, cursor = _mock_conn()
    cursor.fetchone.return_value = None  # no waiting room row (intra_node)
    with patch("worker.db._get_conn", return_value=conn):
        result = increment_completed_and_check("r1")
    assert result is True


# ---------------------------------------------------------------------------
# get_run_status
# ---------------------------------------------------------------------------

def test_get_run_status_returns_value():
    conn, cursor = _mock_conn()
    cursor.fetchone.return_value = ("EXECUTING",)
    with patch("worker.db._get_conn", return_value=conn):
        result = get_run_status("r1")
    assert result == "EXECUTING"


def test_get_run_status_returns_none_when_missing():
    conn, cursor = _mock_conn()
    cursor.fetchone.return_value = None
    with patch("worker.db._get_conn", return_value=conn):
        result = get_run_status("r1")
    assert result is None


# ---------------------------------------------------------------------------
# acquire_sut_lock / release_sut_lock
# ---------------------------------------------------------------------------

def test_acquire_sut_lock_calls_pg_advisory_lock():
    conn, cursor = _mock_conn()
    with patch("worker.db._get_conn", return_value=conn):
        acquire_sut_lock(12345)
    sql = cursor.execute.call_args[0][0]
    assert "pg_advisory_lock" in sql


def test_release_sut_lock_calls_pg_advisory_unlock():
    conn, cursor = _mock_conn()
    with patch("worker.db._get_conn", return_value=conn):
        release_sut_lock(12345)
    sql = cursor.execute.call_args[0][0]
    assert "pg_advisory_unlock" in sql
    conn.commit.assert_called_once()
