from unittest import TestCase

from redisbench_admin.utils.utils import (
    retrieve_local_or_remote_input_json,
    get_ts_metric_name,
    get_remote_input_file_from_url,
    env_flag,
    search_create_before_load,
)


class Test(TestCase):
    def test_benchmark_config_parseExporterMetricsDefinition(self):
        benchmark_config = {}
        pass

    def test_benchmark_config_parseExporterTimeMetricDefinition(self):
        pass

    def test_benchmark_config_parseExporterTimeMetric(self):
        pass

    def test_retrieve_local_or_remote_input_json(self):
        benchmark_config = retrieve_local_or_remote_input_json(
            "./tests/test_data/redis-benchmark.6.2.results.csv",
            ".",
            "opt",
            "csv",
            csv_header=False,
        )
        assert (
            benchmark_config["./tests/test_data/redis-benchmark.6.2.results.csv"][
                "col_0"
            ][0]
            == "test"
        )
        benchmark_config = retrieve_local_or_remote_input_json(
            "./tests/test_data/redis-benchmark.6.0.results.csv",
            ".",
            "opt",
            "csv",
            csv_header=False,
        )
        assert (
            benchmark_config["./tests/test_data/redis-benchmark.6.0.results.csv"][
                "col_0"
            ][0]
            == "PING_INLINE"
        )


def test_get_ts_metric_name():
    by = "by.branch"
    by_value = "unstable"
    tf_github_org = "redis"
    tf_github_repo = "redis"
    deployment_type = "oss-standalone"
    deployment_name = "oss-standalone"
    test_name = "test-1"
    tf_triggering_env = "ci"
    metric_name = "rps"
    metric_context_path = None
    use_metric_context_path = False
    build_variant_name = None

    assert (
        get_ts_metric_name(
            by,
            by_value,
            tf_github_org,
            tf_github_repo,
            deployment_name,
            deployment_type,
            test_name,
            tf_triggering_env,
            metric_name,
            metric_context_path,
            use_metric_context_path,
            build_variant_name,
        )
        == "ci.benchmarks.redislabs/by.branch/ci/redis/redis/test-1/oss-standalone/unstable/rps"
    )

    metric_context_path = "PING"
    use_metric_context_path = True
    assert (
        get_ts_metric_name(
            by,
            by_value,
            tf_github_org,
            tf_github_repo,
            deployment_type,
            deployment_name,
            test_name,
            tf_triggering_env,
            metric_name,
            metric_context_path,
            use_metric_context_path,
            build_variant_name,
        )
        == "ci.benchmarks.redislabs/by.branch/ci/redis/redis/test-1/oss-standalone/unstable/rps/PING"
    )

    build_variant_name = "icc-2021.3.0-amd64-ubuntu18.04-default"
    assert get_ts_metric_name(
        by,
        by_value,
        tf_github_org,
        tf_github_repo,
        deployment_name,
        deployment_type,
        test_name,
        tf_triggering_env,
        metric_name,
        metric_context_path,
        use_metric_context_path,
        build_variant_name,
    ) == "ci.benchmarks.redislabs/by.branch/ci/redis/redis/test-1/{}/oss-standalone/unstable/rps/PING".format(
        build_variant_name
    )


def test_get_remote_input_file_from_url():
    """Test that get_remote_input_file_from_url generates unique file paths based on file name."""
    # Test with a typical S3 URL
    url = "https://s3.amazonaws.com/benchmarks.redislabs/redistimeseries/tsbs/queries/SETUP.CSV"
    assert get_remote_input_file_from_url(url) == "/tmp/input-SETUP.data"

    # Test with a different file name (simulates the AND_QUERY case)
    url = "https://s3.amazonaws.com/benchmarks.redislabs/redisearch/data/AND_QUERY.CSV"
    assert get_remote_input_file_from_url(url) == "/tmp/input-AND_QUERY.data"

    # Test with URL-encoded characters
    url = "https://s3.amazonaws.com/benchmarks.redislabs/tsbs/queries/queries_cpu-only_redistimeseries_100_cpu-max-all-1_10000_123_2016-01-01T00%3A00%3A00Z_2016-01-04T00%3A00%3A00Z.dat"
    result = get_remote_input_file_from_url(url)
    assert result.startswith("/tmp/input-")
    assert result.endswith(".data")
    # URL-encoded characters like %3A should be replaced with _
    assert "%" not in result

    # Test with s3:// URI
    url = "s3://benchmarks.redislabs/data/my_dataset.csv"
    assert get_remote_input_file_from_url(url) == "/tmp/input-my_dataset.data"

    # Test with None - should fallback to default
    assert get_remote_input_file_from_url(None) == "/tmp/input.data"

    # Test with empty string - should fallback to default
    assert get_remote_input_file_from_url("") == "/tmp/input.data"

    # Test with file without extension
    url = "https://s3.amazonaws.com/data/myfile"
    assert get_remote_input_file_from_url(url) == "/tmp/input-myfile.data"

    # Test with .dat extension
    url = "https://s3.amazonaws.com/data/benchmark_data.dat"
    assert get_remote_input_file_from_url(url) == "/tmp/input-benchmark_data.data"


def test_env_flag(monkeypatch):
    # unset is a distinct third state, not False
    monkeypatch.delenv("SOME_FLAG", raising=False)
    assert env_flag("SOME_FLAG") is None

    for enabled in ["1", "true", "TRUE", "yes", "y", "on", "  True  ", "whatever"]:
        monkeypatch.setenv("SOME_FLAG", enabled)
        assert env_flag("SOME_FLAG") is True, enabled

    for disabled in ["0", "false", "FALSE", "no", "n", "off", "", "  0  "]:
        monkeypatch.setenv("SOME_FLAG", disabled)
        assert env_flag("SOME_FLAG") is False, disabled


def test_search_create_before_load_defaults_off(monkeypatch):
    monkeypatch.delenv("SEARCH_CREATE_BEFORE_LOAD", raising=False)
    monkeypatch.delenv("SEARCH_CLUSTERSET", raising=False)
    assert search_create_before_load() is False


def test_search_create_before_load_inherits_search_clusterset(monkeypatch):
    # backwards compatibility: SEARCH_CLUSTERSET used to be the only thing
    # driving the create-index-then-load ordering, and it is presence-based
    monkeypatch.delenv("SEARCH_CREATE_BEFORE_LOAD", raising=False)
    for clusterset in ["1", "yes", ""]:
        monkeypatch.setenv("SEARCH_CLUSTERSET", clusterset)
        assert search_create_before_load() is True, clusterset


def test_search_create_before_load_standalone(monkeypatch):
    # the whole point of the new var: enable the ordering without SEARCH_CLUSTERSET
    monkeypatch.delenv("SEARCH_CLUSTERSET", raising=False)
    monkeypatch.setenv("SEARCH_CREATE_BEFORE_LOAD", "1")
    assert search_create_before_load() is True


def test_search_create_before_load_explicit_opt_out(monkeypatch):
    # an explicit disable wins over the SEARCH_CLUSTERSET fallback
    monkeypatch.setenv("SEARCH_CLUSTERSET", "1")
    for disabled in ["0", "false", "no", "off"]:
        monkeypatch.setenv("SEARCH_CREATE_BEFORE_LOAD", disabled)
        assert search_create_before_load() is False, disabled
