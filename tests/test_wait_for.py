import pytest
import yaml

from redisbench_admin.run.metrics import extract_results_table
from redisbench_admin.utils.results import merge_measurements_into_results
from redisbench_admin.run.common import (
    dbconfig_wait_for_conditions,
    extract_dbconfig_wait_for,
    extract_wait_for_comparison,
    flat_reply_to_dict,
    wait_for_compare,
    wait_for_condition,
    wait_for_covers_field,
)


class FakeRedis:
    """Minimal redis conn stub replying a scripted sequence of flat k/v arrays."""

    def __init__(self, replies):
        self.replies = list(replies)
        self.commands = []

    def execute_command(self, command):
        self.commands.append(command)
        if len(self.replies) > 1:
            return self.replies.pop(0)
        return self.replies[0]


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


def test_extract_dbconfig_wait_for_dict_form():
    benchmark_config = yaml.safe_load(
        """
dbconfig:
  dataset_name: "some-dataset"
  wait_for:
    - name: "index_time"
      command: "FT.INFO idx"
      field: "indexing"
      eq: 0
"""
    )
    specs = extract_dbconfig_wait_for(benchmark_config)
    assert len(specs) == 1
    assert specs[0]["name"] == "index_time"
    assert specs[0]["eq"] == 0


def test_extract_dbconfig_wait_for_legacy_list_form():
    benchmark_config = yaml.safe_load(
        """
dbconfig:
  - dataset_name: "some-dataset"
  - wait_for:
      name: "index_time"
      command: "FT.INFO idx"
      field: "indexing"
      eq: 0
"""
    )
    specs = extract_dbconfig_wait_for(benchmark_config)
    assert len(specs) == 1
    assert specs[0]["command"] == "FT.INFO idx"


def test_extract_dbconfig_wait_for_absent():
    assert extract_dbconfig_wait_for({}) == []
    assert extract_dbconfig_wait_for({"dbconfig": {"dataset_name": "x"}}) == []


def test_wait_for_covers_field():
    specs = [{"field": "indexing"}, {"field": "cleaning"}]
    assert wait_for_covers_field(specs, "indexing") is True
    assert wait_for_covers_field(specs, "num_docs") is False
    assert wait_for_covers_field([], "indexing") is False


def test_flat_reply_to_dict():
    assert flat_reply_to_dict(["a", 1, "b", 2]) == {"a": 1, "b": 2}
    assert flat_reply_to_dict([b"a", b"1"]) == {"a": b"1"}
    assert flat_reply_to_dict({b"a": 1}) == {"a": 1}
    # odd sized replies must not raise
    assert flat_reply_to_dict(["a", 1, "dangling"]) == {"a": 1}
    # nested values are kept as is
    reply = ["attributes", [["identifier", "title"]], "indexing", 0]
    assert flat_reply_to_dict(reply)["indexing"] == 0


def test_wait_for_compare_numeric_and_string():
    assert wait_for_compare(b"0", "eq", 0) is True
    assert wait_for_compare("1", "eq", 0) is False
    assert wait_for_compare(0.5, "lt", 1) is True
    assert wait_for_compare(1, "ge", 1) is True
    assert wait_for_compare(b"idx", "eq", "idx") is True
    assert wait_for_compare("idx", "ne", "other") is True
    with pytest.raises(Exception):
        wait_for_compare("idx", "le", "other")
    with pytest.raises(Exception):
        wait_for_compare(1, "approximately", 1)


def test_extract_wait_for_comparison():
    assert extract_wait_for_comparison({"eq": 0}) == ("eq", 0)
    with pytest.raises(Exception):
        extract_wait_for_comparison({"name": "x"})
    with pytest.raises(Exception):
        extract_wait_for_comparison({"eq": 0, "le": 1})


def test_wait_for_condition_measures_the_transition():
    # 3 polls reporting indexing=1 and then a 4th reporting indexing=0
    conn = FakeRedis(
        [
            ft_info_reply(1, percent_indexed=0.25),
            ft_info_reply(1, percent_indexed=0.5),
            ft_info_reply(1, percent_indexed=0.75),
            ft_info_reply(0, percent_indexed=1),
        ]
    )
    measurements = wait_for_condition(
        {
            "name": "index_time",
            "command": "FT.INFO idx",
            "field": "indexing",
            "eq": 0,
            "poll_interval_ms": 1,
            "timeout_secs": 30,
            "require": {"percent_indexed": 1},
            "record_fields": ["num_docs", "percent_indexed"],
        },
        conn,
    )
    assert len(conn.commands) == 4
    assert conn.commands[0] == "FT.INFO idx"
    assert measurements["index_time_secs"] > 0
    assert measurements["index_time_ms"] == pytest.approx(
        measurements["index_time_secs"] * 1000.0
    )
    assert measurements["index_time_num_docs"] == 1000.0
    assert measurements["index_time_percent_indexed"] == 1.0


def test_wait_for_condition_handles_resp2_byte_replies():
    conn = FakeRedis([ft_info_reply(1, as_bytes=True), ft_info_reply(0, as_bytes=True)])
    measurements = wait_for_condition(
        {
            "name": "index_time",
            "command": "FT.INFO idx",
            "field": "indexing",
            "eq": 0,
            "poll_interval_ms": 1,
        },
        conn,
    )
    assert "index_time_secs" in measurements


def test_wait_for_condition_timeout_raises():
    conn = FakeRedis([ft_info_reply(1, percent_indexed=0.1)])
    with pytest.raises(Exception) as excinfo:
        wait_for_condition(
            {
                "name": "index_time",
                "command": "FT.INFO idx",
                "field": "indexing",
                "eq": 0,
                "poll_interval_ms": 1,
                "timeout_secs": 0.05,
            },
            conn,
        )
    assert "timed out" in str(excinfo.value)


def test_wait_for_condition_require_mismatch_raises():
    # an OOM aborted background scan flips indexing back to 0 without having
    # indexed everything. the require guard must reject it
    conn = FakeRedis([ft_info_reply(0, percent_indexed=0.42)])
    with pytest.raises(Exception) as excinfo:
        wait_for_condition(
            {
                "name": "index_time",
                "command": "FT.INFO idx",
                "field": "indexing",
                "eq": 0,
                "poll_interval_ms": 1,
                "require": {"percent_indexed": 1},
            },
            conn,
        )
    assert "Refusing to record a partial result" in str(excinfo.value)


def test_wait_for_condition_missing_field_raises():
    conn = FakeRedis([["index_name", "idx"]])
    with pytest.raises(Exception) as excinfo:
        wait_for_condition(
            {
                "name": "index_time",
                "command": "FT.INFO idx",
                "field": "indexing",
                "eq": 0,
                "poll_interval_ms": 1,
            },
            conn,
        )
    assert "is not present on the reply" in str(excinfo.value)


def test_wait_for_condition_incomplete_spec_raises():
    conn = FakeRedis([ft_info_reply(0)])
    with pytest.raises(Exception):
        wait_for_condition({"name": "index_time", "eq": 0}, conn)


def test_dbconfig_wait_for_conditions_multiple_specs():
    conn = FakeRedis([ft_info_reply(0)])
    measurements = dbconfig_wait_for_conditions(
        [
            {
                "name": "index_time",
                "command": "FT.INFO idx",
                "field": "indexing",
                "eq": 0,
                "poll_interval_ms": 1,
            },
            {
                "name": "docs_present",
                "command": "FT.INFO idx",
                "field": "num_docs",
                "ge": 1000,
                "poll_interval_ms": 1,
            },
        ],
        conn,
    )
    assert "index_time_secs" in measurements
    assert "docs_present_secs" in measurements


def test_dbconfig_wait_for_conditions_no_specs():
    assert dbconfig_wait_for_conditions([], FakeRedis([[]])) == {}


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


def test_wait_for_exporter_jsonpaths_reach_the_measurements():
    benchmark_config = yaml.safe_load(
        """
exporter:
  redistimeseries:
    metrics:
      - "$.Measurements.index_time_secs"
"""
    )
    results_dict = merge_measurements_into_results({}, {"index_time_secs": 12.5})
    results_table = extract_results_table(
        benchmark_config["exporter"]["redistimeseries"]["metrics"], results_dict
    )
    assert len(results_table) == 1
    # the exported metric name is the jsonpath minus the leading "$."
    assert results_table[0][0] == "Measurements.index_time_secs"
    assert 12.5 in results_table[0]


def test_example_benchmark_definition_is_parseable():
    with open(
        "./tests/test_data/search-background-indexing-wait-for.yml", "r"
    ) as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
    specs = extract_dbconfig_wait_for(benchmark_config)
    assert len(specs) == 1
    spec = specs[0]
    assert extract_wait_for_comparison(spec) == ("eq", 0)
    assert spec["field"] == "indexing"
    assert spec["require"] == {"percent_indexed": 1}
    # measurement only: the definition must not gate the test on pass/fail
    assert "kpis" not in benchmark_config
    # the exporter and comparison jsonpaths must match the measurement names the
    # wait_for entry produces
    conn = FakeRedis([ft_info_reply(0)])
    measurements = wait_for_condition(dict(spec, poll_interval_ms=1), conn)
    results_dict = merge_measurements_into_results({}, measurements)
    exporter = benchmark_config["exporter"]
    for jsonpath in exporter["redistimeseries"]["metrics"]:
        assert len(extract_results_table([jsonpath], results_dict)) == 1
    for jsonpath in exporter["comparison"]["metrics"]:
        assert len(extract_results_table([jsonpath], results_dict)) == 1


class FakeSearchRedis:
    """Redis conn stub good enough to drive run_redis_pre_steps end to end."""

    def __init__(self, ft_info_replies, ft_list_reply=None):
        self.ft_info_replies = list(ft_info_replies)
        self.ft_list_reply = ft_list_reply if ft_list_reply is not None else ["idx"]
        self.commands = []

    def execute_command(self, *args, **kwargs):
        command = " ".join([str(a) for a in args])
        self.commands.append(command)
        if command.lower() == "info modules":
            return "# Modules\nmodule:name=search,ver=81201\n"
        if command.lower() == "ft._list":
            return list(self.ft_list_reply)
        if command.upper().startswith("FT.INFO"):
            if len(self.ft_info_replies) > 1:
                return self.ft_info_replies.pop(0)
            return self.ft_info_replies[0]
        return "OK"

    def info(self, section):
        return {"redis_version": "8.2.0"}


WAIT_FOR_BENCHMARK_CONFIG = {
    "dbconfig": [
        {"init_commands": ['"FT.CREATE" "idx" "ON" "HASH" "SCHEMA" "title" "TEXT"']},
        {
            "wait_for": [
                {
                    "name": "index_time",
                    "command": "FT.INFO idx",
                    "field": "indexing",
                    "eq": 0,
                    "poll_interval_ms": 1,
                    "require": {"percent_indexed": 1},
                }
            ]
        },
    ]
}


def test_run_redis_pre_steps_measures_and_replaces_the_indexing_barrier():
    from redisbench_admin.run.common import run_redis_pre_steps

    conn = FakeSearchRedis([ft_info_reply(1, 0.5), ft_info_reply(0, 1)])
    version, measurements = run_redis_pre_steps(
        WAIT_FOR_BENCHMARK_CONFIG, conn, ["search"]
    )
    assert version == "81201"
    assert measurements["index_time_secs"] > 0
    # the FT.CREATE ran before the wait, and the unconditional indexing barrier
    # ( which polls ft._list ) was replaced by the wait_for entry
    assert conn.commands[0].startswith("FT.CREATE")
    assert "ft._list" not in [c.lower() for c in conn.commands]


def test_run_redis_pre_steps_keeps_the_barrier_without_wait_for(monkeypatch):
    from redisbench_admin.run.common import run_redis_pre_steps

    # search_specific_init polls every 5 secs, no need to wait for it here
    monkeypatch.setattr("time.sleep", lambda _: None)
    conn = FakeSearchRedis([ft_info_reply(0, 1)], ft_list_reply=["idx"])
    version, measurements = run_redis_pre_steps(
        {"dbconfig": [{"init_commands": ['"FT.CREATE" "idx" "ON" "HASH"']}]},
        conn,
        ["search"],
    )
    assert version == "81201"
    assert measurements == {}
    assert "ft._list" in [c.lower() for c in conn.commands]


def test_run_redis_pre_steps_skips_wait_for_when_disabled(monkeypatch):
    from redisbench_admin.run.common import run_redis_pre_steps

    monkeypatch.setattr("time.sleep", lambda _: None)
    conn = FakeSearchRedis([ft_info_reply(0, 1)])
    _, measurements = run_redis_pre_steps(
        WAIT_FOR_BENCHMARK_CONFIG, conn, ["search"], run_wait_for=False
    )
    assert measurements == {}
    # with the wait_for entry not evaluated the barrier must stay in place
    assert "ft._list" in [c.lower() for c in conn.commands]
