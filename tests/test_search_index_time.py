import yaml

from redisbench_admin.run.common import (
    check_dbconfig_keyspacelen_requirement,
    check_dbconfig_tool_requirement,
    decode_if_bytes,
    flat_reply_to_dict,
    run_redis_pre_steps,
    search_specific_init,
)
from redisbench_admin.run.metrics import extract_results_table
from redisbench_admin.utils.results import merge_measurements_into_results


class FakeSearchRedis:
    """Redis conn stub replying a scripted FT.INFO sequence per index."""

    def __init__(self, ft_info_replies, ft_list_reply=None):
        # dict of index name -> list of replies, or a single list for "idx"
        if isinstance(ft_info_replies, list):
            ft_info_replies = {"idx": ft_info_replies}
        self.ft_info_replies = {k: list(v) for k, v in ft_info_replies.items()}
        self.ft_list_reply = (
            ft_list_reply
            if ft_list_reply is not None
            else list(self.ft_info_replies.keys())
        )
        self.commands = []

    def execute_command(self, *args, **kwargs):
        command = " ".join([str(a) for a in args])
        self.commands.append(command)
        if command.lower() == "info modules":
            return "# Modules\nmodule:name=search,ver=81201\n"
        if command.lower() == "ft._list":
            return list(self.ft_list_reply)
        if command.upper().startswith("FT.INFO"):
            index_name = command.split(" ")[1]
            replies = self.ft_info_replies[index_name]
            if len(replies) > 1:
                return replies.pop(0)
            return replies[0]
        return "OK"

    def info(self, section):
        return {"redis_version": "8.2.0"}

    def ft_info_calls(self):
        """index names FT.INFO was called on, in call order"""
        return [
            c.split(" ")[1] for c in self.commands if c.upper().startswith("FT.INFO")
        ]


def ft_info_reply(indexing, percent_indexed=1, num_docs=1000, as_bytes=False):
    reply = [
        "index_name",
        "idx",
        "num_docs",
        num_docs,
        "indexing",
        indexing,
        "percent_indexed",
        percent_indexed,
        "hash_indexing_failures",
        0,
    ]
    if as_bytes:
        reply = [x.encode() if isinstance(x, str) else str(x).encode() for x in reply]
    return reply


def test_decode_if_bytes():
    assert decode_if_bytes(b"idx") == "idx"
    assert decode_if_bytes("idx") == "idx"
    assert decode_if_bytes(0) == 0


def test_flat_reply_to_dict():
    assert flat_reply_to_dict(["a", 1, "b", 2]) == {"a": 1, "b": 2}
    assert flat_reply_to_dict([b"a", b"1"]) == {"a": b"1"}
    # resp3 map replies only need their keys decoded
    assert flat_reply_to_dict({b"a": 1}) == {"a": 1}
    # odd sized replies must not raise
    assert flat_reply_to_dict(["a", 1, "dangling"]) == {"a": 1}
    # nested values are kept as is
    reply = ["attributes", [["identifier", "title"]], "indexing", 0]
    assert flat_reply_to_dict(reply)["indexing"] == 0


def test_search_specific_init_times_the_index_build(monkeypatch):
    monkeypatch.setattr(
        "redisbench_admin.run.common.SEARCH_INDEXING_POLL_INTERVAL_SECS", 0.001
    )
    # 3 polls reporting indexing=1 and then a 4th reporting indexing=0
    conn = FakeSearchRedis(
        [
            ft_info_reply(1, percent_indexed=0.25),
            ft_info_reply(1, percent_indexed=0.5),
            ft_info_reply(1, percent_indexed=0.75),
            ft_info_reply(0, percent_indexed=1),
        ]
    )
    measurements = search_specific_init(conn, ["search"])
    assert len(conn.ft_info_calls()) == 4
    assert measurements["index_time_secs"] > 0
    assert measurements["index_time_ms"] == measurements["index_time_secs"] * 1000.0


def test_search_specific_init_records_nothing_when_no_scan_was_running():
    # an index created before the documents exist gets no background scan, so the
    # first poll already reports indexing=0. percent_indexed is 1 on a never
    # scanned index, so only the absence of a transition reveals it
    conn = FakeSearchRedis([ft_info_reply(0, percent_indexed=1, num_docs=0)])
    measurements = search_specific_init(conn, ["search"])
    assert measurements == {}
    assert len(conn.ft_info_calls()) == 1


def test_search_specific_init_handles_resp2_byte_replies(monkeypatch):
    monkeypatch.setattr(
        "redisbench_admin.run.common.SEARCH_INDEXING_POLL_INTERVAL_SECS", 0.001
    )
    conn = FakeSearchRedis(
        [ft_info_reply(1, as_bytes=True), ft_info_reply(0, as_bytes=True)],
        ft_list_reply=[b"idx"],
    )
    measurements = search_specific_init(conn, ["search"])
    assert "index_time_secs" in measurements


def test_search_specific_init_waits_for_every_index(monkeypatch):
    monkeypatch.setattr(
        "redisbench_admin.run.common.SEARCH_INDEXING_POLL_INTERVAL_SECS", 0.001
    )
    # idx_b needs one more round than idx_a. the previous implementation popped
    # from the list it was iterating, which skipped an entry per round
    conn = FakeSearchRedis(
        {
            "idx_a": [ft_info_reply(1), ft_info_reply(0)],
            "idx_b": [ft_info_reply(1), ft_info_reply(1), ft_info_reply(0)],
        }
    )
    measurements = search_specific_init(conn, ["search"])
    assert "index_time_secs" in measurements
    # idx_a stops being polled once it reports 0, idx_b keeps going
    calls = conn.ft_info_calls()
    assert calls.count("idx_a") == 2
    assert calls.count("idx_b") == 3


def test_search_specific_init_noop_without_the_search_module():
    conn = FakeSearchRedis([ft_info_reply(0)])
    assert search_specific_init(conn, ["timeseries"]) == {}
    assert conn.commands == []


def test_search_specific_init_no_indices():
    conn = FakeSearchRedis([ft_info_reply(0)], ft_list_reply=[])
    assert search_specific_init(conn, ["search"]) == {}


BENCHMARK_CONFIG = {
    "dbconfig": [
        {"init_commands": ['"FT.CREATE" "idx" "ON" "HASH" "SCHEMA" "title" "TEXT"']},
    ]
}


def test_run_redis_pre_steps_returns_the_index_build_time(monkeypatch):
    monkeypatch.setattr(
        "redisbench_admin.run.common.SEARCH_INDEXING_POLL_INTERVAL_SECS", 0.001
    )
    conn = FakeSearchRedis([ft_info_reply(1, 0.5), ft_info_reply(0, 1)])
    version, measurements = run_redis_pre_steps(BENCHMARK_CONFIG, conn, ["search"])
    assert version == "81201"
    assert measurements["index_time_secs"] > 0
    # the FT.CREATE must run before the index is polled, otherwise there is no
    # background scan to wait on
    assert conn.commands[0].startswith("FT.CREATE")
    assert conn.commands.index("ft._list") > 0


def test_run_redis_pre_steps_without_a_background_build():
    conn = FakeSearchRedis([ft_info_reply(0, 1)])
    version, measurements = run_redis_pre_steps(BENCHMARK_CONFIG, conn, ["search"])
    assert version == "81201"
    assert measurements == {}


def test_merge_measurements_into_results():
    results_dict = {"ALL STATS": {"Totals": {"Ops/sec": 1.0}}}
    merged = merge_measurements_into_results(results_dict, {"index_time_secs": 12.5})
    assert merged["Measurements"]["index_time_secs"] == 12.5
    assert merged["ALL STATS"]["Totals"]["Ops/sec"] == 1.0
    # merging keeps previously recorded measurements
    merged = merge_measurements_into_results(merged, {"other_secs": 1.0})
    assert merged["Measurements"]["index_time_secs"] == 12.5
    assert merged["Measurements"]["other_secs"] == 1.0
    # no measurements is a no-op
    assert merge_measurements_into_results(results_dict, {}) == results_dict
    assert merge_measurements_into_results(results_dict, None) == results_dict
    # a None results dict is tolerated
    assert merge_measurements_into_results(None, {"a_secs": 1.0}) == {
        "Measurements": {"a_secs": 1.0}
    }


def test_merge_measurements_into_results_overrides_non_dict_key():
    merged = merge_measurements_into_results(
        {"Measurements": "unexpected"}, {"index_time_secs": 1.0}
    )
    assert merged["Measurements"] == {"index_time_secs": 1.0}


def test_exporter_jsonpath_reaches_the_measurements():
    results_dict = merge_measurements_into_results({}, {"index_time_secs": 12.5})
    results_table = extract_results_table(
        ["$.Measurements.index_time_secs"], results_dict
    )
    assert len(results_table) == 1
    # the exported metric name is the jsonpath minus the leading "$."
    assert results_table[0][0] == "Measurements.index_time_secs"
    assert 12.5 in results_table[0]


def test_example_benchmark_definition_produces_the_exported_metric():
    with open("./tests/test_data/search-background-indexing.yml", "r") as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
    # the documents must be loaded by the dbconfig preload tool, which runs
    # before the init_commands, so the FT.CREATE sees a full keyspace
    assert check_dbconfig_tool_requirement(benchmark_config) is True
    required, keyspacelen, _ = check_dbconfig_keyspacelen_requirement(benchmark_config)
    assert required is True and keyspacelen > 0
    # measurement only: the definition must not gate the run on pass/fail
    assert "kpis" not in benchmark_config
    # the exporter and comparison jsonpaths must match what the pre steps record
    results_dict = merge_measurements_into_results(
        {}, {"index_time_secs": 12.5, "index_time_ms": 12500.0}
    )
    exporter = benchmark_config["exporter"]
    for jsonpath in exporter["redistimeseries"]["metrics"]:
        assert len(extract_results_table([jsonpath], results_dict)) == 1
    for jsonpath in exporter["comparison"]["metrics"]:
        assert len(extract_results_table([jsonpath], results_dict)) == 1
