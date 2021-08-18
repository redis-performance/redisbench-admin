from unittest import TestCase

from redisbench_admin.utils.utils import (
    retrieve_local_or_remote_input_json,
    get_ts_metric_name,
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
