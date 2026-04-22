import os

import pytest
import redis

from redisbench_admin.utils.remote import (
    ARCH_ARM,
    ARCH_X86,
    detect_target_arch,
)


class _StubConn:
    def __init__(self, info_reply=None, info_raises=None):
        self._info_reply = info_reply
        self._info_raises = info_raises

    def info(self, section=None):
        if self._info_raises is not None:
            raise self._info_raises
        return self._info_reply


def test_detect_target_arch_x86_from_linux_info():
    conn = _StubConn(info_reply={"os": "Linux 6.8.0-1015-aws x86_64"})
    assert detect_target_arch(conn) == ARCH_X86


def test_detect_target_arch_arm_from_linux_info():
    conn = _StubConn(info_reply={"os": "Linux 6.8.0-1015-aws aarch64"})
    assert detect_target_arch(conn) == ARCH_ARM


def test_detect_target_arch_amd64_alias():
    conn = _StubConn(info_reply={"os": "Linux 5.15.0 amd64"})
    assert detect_target_arch(conn) == ARCH_X86


def test_detect_target_arch_arm64_alias():
    conn = _StubConn(info_reply={"os": "Darwin 23.0 arm64"})
    assert detect_target_arch(conn) == ARCH_ARM


def test_detect_target_arch_unknown_returns_none():
    conn = _StubConn(info_reply={"os": "Linux 5.4 armv7l"})
    assert detect_target_arch(conn) is None


def test_detect_target_arch_missing_os_field_returns_none():
    conn = _StubConn(info_reply={"redis_version": "7.4.0"})
    assert detect_target_arch(conn) is None


def test_detect_target_arch_empty_os_field_returns_none():
    conn = _StubConn(info_reply={"os": ""})
    assert detect_target_arch(conn) is None


def test_detect_target_arch_info_connection_error_returns_none():
    conn = _StubConn(info_raises=redis.ConnectionError("down"))
    assert detect_target_arch(conn) is None


def test_detect_target_arch_info_generic_error_returns_none():
    """Must swallow any exception — the fallback path should be safe."""
    conn = _StubConn(info_raises=RuntimeError("unexpected"))
    assert detect_target_arch(conn) is None


def test_detect_target_arch_non_dict_reply_returns_none():
    conn = _StubConn(info_reply="oops")
    assert detect_target_arch(conn) is None


def test_detect_target_arch_case_insensitive():
    conn = _StubConn(info_reply={"os": "Linux 6.8.0 AARCH64"})
    assert detect_target_arch(conn) == ARCH_ARM


def test_detect_target_arch_against_live_redis_stack():
    """End-to-end against the tox-managed redis-stack sidecar. Runs only
    when RTS_PORT is set (i.e. under `tox -e integration-tests`)."""
    if "RTS_PORT" not in os.environ:
        pytest.skip("RTS_PORT environment variable not set")
    rts_port = os.environ["RTS_PORT"]
    try:
        conn = redis.Redis(port=rts_port)
        conn.ping()
    except redis.ConnectionError:
        pytest.skip("Could not connect to redis-stack sidecar on RTS_PORT")

    arch = detect_target_arch(conn)
    assert arch in (ARCH_X86, ARCH_ARM), (
        "expected a valid arch from live INFO server, got {}".format(arch)
    )
