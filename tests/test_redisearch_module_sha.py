import os

import pytest
import redis

from redisbench_admin.utils.redisearch import extract_module_git_sha


class _FakeConn:
    def __init__(self, module_list_reply, debug_replies=None, debug_raises=None):
        self._module_list = module_list_reply
        self._debug_replies = debug_replies or {}
        self._debug_raises = debug_raises or {}
        self.calls = []

    def execute_command(self, *args):
        self.calls.append(args)
        cmd = args[0].upper()
        if cmd == "MODULE" and len(args) >= 2 and args[1].upper() == "LIST":
            return self._module_list
        if len(args) >= 2 and args[1].upper() == "GIT_SHA":
            if cmd in self._debug_raises:
                raise self._debug_raises[cmd]
            if cmd in self._debug_replies:
                return self._debug_replies[cmd]
        raise redis.ResponseError("unknown command {}".format(args))


def _entry(name, version=99999):
    return [b"name", name.encode(), b"ver", version]


def test_extract_module_git_sha_from_search():
    conn = _FakeConn(
        module_list_reply=[_entry("search")],
        debug_replies={"FT.DEBUG": b"abc1234"},
    )
    assert extract_module_git_sha(conn) == "abc1234"


def test_extract_module_git_sha_strips_whitespace():
    conn = _FakeConn(
        module_list_reply=[_entry("search")],
        debug_replies={"FT.DEBUG": b"  abc1234\n"},
    )
    assert extract_module_git_sha(conn) == "abc1234"


def test_extract_module_git_sha_ignores_modules_without_debug_git_sha():
    """JSON/timeseries/bloom/graph are NOT in the map today (they don't
    expose DEBUG GIT_SHA). When they're the only thing loaded, return None."""
    conn = _FakeConn(
        module_list_reply=[_entry("ReJSON"), _entry("timeseries"), _entry("bf")],
    )
    assert extract_module_git_sha(conn) is None


def test_extract_module_git_sha_search_wins_alongside_unsupported_modules():
    """With search + unsupported modules all loaded, search's sha still
    comes through — unsupported modules are silently skipped."""
    conn = _FakeConn(
        module_list_reply=[_entry("ReJSON"), _entry("search"), _entry("timeseries")],
        debug_replies={"FT.DEBUG": b"search-sha"},
    )
    assert extract_module_git_sha(conn) == "search-sha"


def test_extract_module_git_sha_no_module_loaded():
    conn = _FakeConn(module_list_reply=[])
    assert extract_module_git_sha(conn) is None


def test_extract_module_git_sha_unknown_module_only():
    conn = _FakeConn(module_list_reply=[_entry("rejson-ish")])
    assert extract_module_git_sha(conn) is None


def test_extract_module_git_sha_module_list_fails():
    class BrokenConn:
        def execute_command(self, *args):
            raise redis.ConnectionError("down")

    assert extract_module_git_sha(BrokenConn()) is None


def test_extract_module_git_sha_debug_fails_returns_none():
    """FT.DEBUG raising is not fatal — helper returns None, caller falls
    through to other hash sources (e.g. the local-repo hash)."""
    conn = _FakeConn(
        module_list_reply=[_entry("search")],
        debug_raises={"FT.DEBUG": redis.ResponseError("nope")},
    )
    assert extract_module_git_sha(conn) is None


def test_extract_module_git_sha_malformed_entry_is_skipped():
    """MODULE LIST entries without an index [1] name field must not crash."""
    conn = _FakeConn(
        module_list_reply=[[b"name"], _entry("search")],
        debug_replies={"FT.DEBUG": b"abc1234"},
    )
    assert extract_module_git_sha(conn) == "abc1234"


def test_extract_module_git_sha_debug_returns_none_returns_none():
    """If FT.DEBUG GIT_SHA returns nil, helper returns None."""
    conn = _FakeConn(
        module_list_reply=[_entry("search")],
        debug_replies={"FT.DEBUG": None},
    )
    assert extract_module_git_sha(conn) is None


def test_extract_module_git_sha_debug_returns_empty_string_returns_none():
    conn = _FakeConn(
        module_list_reply=[_entry("search")],
        debug_replies={"FT.DEBUG": b"   "},
    )
    assert extract_module_git_sha(conn) is None


def test_extract_module_git_sha_against_live_redis_stack():
    """End-to-end against the tox-managed redis-stack sidecar (which loads
    the search module). Proves the helper works against a real FT.DEBUG
    GIT_SHA reply, not just mocked bytes."""
    if "RTS_PORT" not in os.environ:
        pytest.skip("RTS_PORT environment variable not set")
    rts_port = os.environ["RTS_PORT"]
    try:
        conn = redis.Redis(port=rts_port)
        conn.ping()
    except redis.ConnectionError:
        pytest.skip("Could not connect to redis-stack sidecar on RTS_PORT")

    sha = extract_module_git_sha(conn)
    assert (
        sha is not None
    ), "expected a non-None git_sha from redis-stack's search module"
    assert isinstance(sha, str) and sha != ""
    # defensive: looks like a git sha (hex-ish, non-trivial length)
    assert len(sha) >= 7
