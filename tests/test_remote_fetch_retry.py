#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
"""Tests for the retry/backoff behavior of fetch_file_from_remote_setup.

A transient SSH/SFTP connection failure (dropped packet, momentary network
blip) on an otherwise-healthy remote host used to be fatal on the first
attempt. These tests cover the retry-then-succeed and give-up-after-max-
retries paths.
"""
from unittest.mock import MagicMock, patch

import paramiko
import pytest

from redisbench_admin.utils.remote import fetch_file_from_remote_setup, view_bar_simple


def _mock_connection():
    conn = MagicMock()
    conn.get.return_value = None
    return conn


def test_fetch_succeeds_on_first_attempt_without_retry():
    conn = _mock_connection()
    with patch(
        "redisbench_admin.utils.remote.pysftp.Connection", return_value=conn
    ) as mocked_connection, patch(
        "redisbench_admin.utils.remote.time.sleep"
    ) as mocked_sleep:
        fetch_file_from_remote_setup(
            "1.2.3.4", "ubuntu", "/tmp/key.pem", "local.out", "/tmp/remote.out"
        )

    mocked_connection.assert_called_once()
    conn.get.assert_called_once_with(
        "/tmp/remote.out", "local.out", callback=view_bar_simple
    )
    mocked_sleep.assert_not_called()


def test_fetch_retries_on_transient_error_then_succeeds():
    conn = _mock_connection()
    with patch(
        "redisbench_admin.utils.remote.pysftp.Connection",
        side_effect=[
            paramiko.SSHException("Error reading SSH protocol banner"),
            conn,
        ],
    ) as mocked_connection, patch(
        "redisbench_admin.utils.remote.time.sleep"
    ) as mocked_sleep:
        fetch_file_from_remote_setup(
            "1.2.3.4",
            "ubuntu",
            "/tmp/key.pem",
            "local.out",
            "/tmp/remote.out",
            max_retries=3,
        )

    assert mocked_connection.call_count == 2
    conn.get.assert_called_once()
    mocked_sleep.assert_called_once_with(1)  # 2**0 backoff on first retry


def test_fetch_gives_up_after_max_retries_and_reraises():
    with patch(
        "redisbench_admin.utils.remote.pysftp.Connection",
        side_effect=EOFError("connection dropped"),
    ) as mocked_connection, patch(
        "redisbench_admin.utils.remote.time.sleep"
    ) as mocked_sleep:
        with pytest.raises(EOFError):
            fetch_file_from_remote_setup(
                "1.2.3.4",
                "ubuntu",
                "/tmp/key.pem",
                "local.out",
                "/tmp/remote.out",
                max_retries=2,
            )

    # 1 initial attempt + 2 retries = 3 total attempts.
    assert mocked_connection.call_count == 3
    assert mocked_sleep.call_count == 2


def test_fetch_does_not_retry_on_non_transient_error():
    with patch(
        "redisbench_admin.utils.remote.pysftp.Connection",
        side_effect=ValueError("not a transient/network error"),
    ) as mocked_connection, patch(
        "redisbench_admin.utils.remote.time.sleep"
    ) as mocked_sleep:
        with pytest.raises(ValueError):
            fetch_file_from_remote_setup(
                "1.2.3.4",
                "ubuntu",
                "/tmp/key.pem",
                "local.out",
                "/tmp/remote.out",
                max_retries=3,
            )

    mocked_connection.assert_called_once()
    mocked_sleep.assert_not_called()
