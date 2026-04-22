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


def test_extract_module_git_sha_prefers_requested_module():
    conn = _FakeConn(
        module_list_reply=[_entry("search"), _entry("json")],
        debug_replies={"FT.DEBUG": b"search-sha", "JSON.DEBUG": b"json-sha"},
    )
    assert extract_module_git_sha(conn, module_name="json") == "json-sha"


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


def test_extract_module_git_sha_debug_fails_falls_through():
    conn = _FakeConn(
        module_list_reply=[_entry("search"), _entry("json")],
        debug_raises={"FT.DEBUG": redis.ResponseError("nope")},
        debug_replies={"JSON.DEBUG": b"json-sha"},
    )
    assert extract_module_git_sha(conn) == "json-sha"


def test_extract_module_git_sha_malformed_entry_is_skipped():
    """MODULE LIST entries without an index [1] name field must not crash."""
    conn = _FakeConn(
        module_list_reply=[[b"name"], _entry("search")],
        debug_replies={"FT.DEBUG": b"abc1234"},
    )
    assert extract_module_git_sha(conn) == "abc1234"


def test_extract_module_git_sha_debug_returns_none_falls_through():
    """If <mod>.DEBUG GIT_SHA returns None, loop over to the next candidate."""
    conn = _FakeConn(
        module_list_reply=[_entry("search"), _entry("json")],
        debug_replies={"FT.DEBUG": None, "JSON.DEBUG": b"json-sha"},
    )
    assert extract_module_git_sha(conn) == "json-sha"


def test_extract_module_git_sha_debug_returns_empty_string_falls_through():
    conn = _FakeConn(
        module_list_reply=[_entry("search"), _entry("json")],
        debug_replies={"FT.DEBUG": b"   ", "JSON.DEBUG": b"json-sha"},
    )
    assert extract_module_git_sha(conn) == "json-sha"
