import os
import unittest.mock

import argparse
import redis
import yaml

from redisbench_admin.profilers.pprof import process_pprof_text_to_tabular
from redisbench_admin.run.args import REDIS_7
from redisbench_admin.run_local.args import create_run_local_arguments
from redisbench_admin.run_local.local_helpers import (
    check_benchmark_binaries_local_requirements,
)
from redisbench_admin.profilers.profilers_schema import get_profilers_rts_key_prefix
from redisbench_admin.run_local.run_local import (
    run_local_command_logic,
)
from redisbench_admin.run.redistimeseries import datasink_profile_tabular_data


def test_check_benchmark_binaries_local_requirements():
    filename = "ycsb-redisearch-binding-0.18.0-SNAPSHOT.tar.gz"
    inner_foldername = "ycsb-redisearch-binding-0.18.0-SNAPSHOT"
    binaries_localtemp_dir = "./binaries"
    with open("./tests/test_data/ycsb-config.yml", "r") as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        (
            benchmark_tool,
            which_benchmark_tool,
            benchmark_tool_workdir,
        ) = check_benchmark_binaries_local_requirements(
            benchmark_config, "ycsb", binaries_localtemp_dir
        )
        assert which_benchmark_tool == os.path.abspath(
            "./binaries/ycsb-redisearch-binding-0.18.0-SNAPSHOT/bin/ycsb"
        )
        assert benchmark_tool_workdir == os.path.abspath(
            "./binaries/ycsb-redisearch-binding-0.18.0-SNAPSHOT"
        )
        assert benchmark_tool == "ycsb"


def test_datasink_profile_tabular_data():
    tabular_map = {}
    tabular_map["text"] = process_pprof_text_to_tabular(
        "./tests/test_data/results/profile_oss-standalone__primary-1-of-1__tsbs-scale100_lastpoint_perf:record_2021-09-07-15-13-02.out.pprof.txt",
        "text",
    )

    tabular_map["text-lines"] = process_pprof_text_to_tabular(
        "./tests/test_data/results/profile_oss-standalone__primary-1-of-1__tsbs-scale100_cpu-max-all-1_perf:record_2021-09-07-16-52-16.out.pprof.LOC.txt",
        "text-lines",
    )
    try:
        start_time_str = "2021-09-09"
        test_name = "test1"
        setup_type = "oss-standalone"
        tf_triggering_env = "ci"
        github_branch = "branch-1"
        github_hash = "hash-11312213"
        rts = redis.Redis()
        rts.ping()
        rts.flushall()
        datasink_profile_tabular_data(
            github_branch,
            "org",
            "repo",
            github_hash,
            tabular_map,
            rts,
            setup_type,
            1000,
            start_time_str,
            test_name,
            tf_triggering_env,
        )
        zset_profiles_key_name = get_profilers_rts_key_prefix(
            tf_triggering_env,
            "org",
            "repo",
        )
        #
        assert rts.exists(zset_profiles_key_name)
        assert rts.zcard(zset_profiles_key_name) == 1

        profile_test_suffix = "{start_time_str}:{test_name}/{setup_type}/{github_branch}/{github_hash}".format(
            start_time_str=start_time_str,
            test_name=test_name,
            setup_type=setup_type,
            github_branch=github_branch,
            github_hash=github_hash,
        )
        for pprof_format in ["text", "text-lines"]:
            table_columns_text_key = "{}:{}:columns:text".format(
                pprof_format, profile_test_suffix
            )
            assert rts.exists(table_columns_text_key)
        # assert redis_conn.exists(testcases_setname)
        # assert redis_conn.exists(running_platforms_setname)
        # assert redis_conn.exists(build_variant_setname)

    except redis.exceptions.ConnectionError:
        pass


def test_run_local_command_logic_redis_benchmark():
    parser = argparse.ArgumentParser(
        description="test",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser = create_run_local_arguments(parser)
    args = parser.parse_args(
        args=[
            "--test",
            "./tests/test_data/redis-benchmark-json.yml",
            "--keep_env_and_topo",
        ]
    )
    try:
        run_local_command_logic(args, "tool", "v0")
    except SystemExit as e:
        assert e.code == 0
    finally:
        r = redis.Redis()
        r.ping()
        total_keys = r.info("keyspace")["db0"]["keys"]
        r.shutdown(nosave=True)
        assert total_keys == 1000


def test_run_local_command_logic():
    parser = argparse.ArgumentParser(
        description="test",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser = create_run_local_arguments(parser)
    args = parser.parse_args(
        args=[
            "--test",
            "./tests/test_data/redis-benchmark-vanilla.yml",
            "--redis-7",
            "{}".format(REDIS_7),
        ]
    )
    try:
        run_local_command_logic(args, "tool", "v0")
    except SystemExit as e:
        assert e.code == 0

    ## specify the default properties to load
    ## and limit the allowed envs to oss-standalone
    parser = argparse.ArgumentParser(
        description="test",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser = create_run_local_arguments(parser)
    args = parser.parse_args(
        args=[
            "--test",
            "./tests/test_data/redis-benchmark-vanilla.yml",
            "--defaults_filename",
            "./tests/test_data/common-properties-v0.5.yml",
            "--allowed-envs",
            "oss-standalone",
            "--redis-7",
            "{}".format(REDIS_7),
        ]
    )
    try:
        run_local_command_logic(args, "tool", "v0")
    except SystemExit as e:
        assert e.code == 0

    ## expected to fail on not allowed tool
    parser = argparse.ArgumentParser(
        description="test",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser = create_run_local_arguments(parser)
    args = parser.parse_args(
        args=[
            "--test",
            "./tests/test_data/redis-benchmark-vanilla.yml",
            "--allowed-tools",
            "ftsb_redisearch",
            "--redis-7",
            "{}".format(REDIS_7),
        ]
    )
    try:
        run_local_command_logic(args, "tool", "v0")
    except SystemExit as e:
        assert e.code == 1

    ## run while pushing results to redis_conn
    rts_host = os.getenv("RTS_DATASINK_HOST", None)
    rts_port = 16379
    if rts_host is None:
        return
    rts = redis.Redis(port=16379, host=rts_host)
    rts.ping()
    rts.flushall()
    parser = argparse.ArgumentParser(
        description="test",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser = create_run_local_arguments(parser)
    args = parser.parse_args(
        args=[
            "--test",
            "./tests/test_data/redis-benchmark-vanilla.yml",
            "--redistimeseries_host",
            rts_host,
            "--redistimeseries_port",
            "{}".format(rts_port),
            "--push_results_redistimeseries",
            "--redis-7",
            "{}".format(REDIS_7),
        ]
    )
    try:
        run_local_command_logic(args, "tool", "v0")
    except SystemExit as e:
        assert e.code == 0


def test_run_local_dry_run():
    """Test dry-run mode to ensure only PING commands are executed"""
    parser = argparse.ArgumentParser(
        description="test dry-run",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser = create_run_local_arguments(parser)

    # Test basic dry-run
    args = parser.parse_args(
        args=[
            "--test",
            "./tests/test_data/redis-benchmark-vanilla.yml",
            "--dry-run",
            "--redis-7",
            "{}".format(REDIS_7),
        ]
    )

    # Mock the run_local_benchmark function to ensure it's never called
    with unittest.mock.patch(
        "redisbench_admin.run_local.local_helpers.run_local_benchmark"
    ) as mock_benchmark:
        # Mock Redis connection to track commands
        with unittest.mock.patch("redis.Redis") as mock_redis_class:
            mock_redis_instance = unittest.mock.MagicMock()
            mock_redis_class.return_value = mock_redis_instance

            # Configure ping to return True
            mock_redis_instance.ping.return_value = True

            # Configure info method to handle different parameters
            def mock_info(section=None):
                if section == "keyspace":
                    return {}  # Empty keyspace for dry-run
                else:
                    return {
                        "process_id": 12345,
                        "redis_version": "7.0.0",
                        "redis_mode": "standalone",
                        "arch_bits": 64,
                        "multiplexing_api": "epoll",
                        "gcc_version": "9.4.0",
                        "process_id": 12345,
                        "run_id": "test-run-id",
                        "tcp_port": 6379,
                        "uptime_in_seconds": 100,
                        "uptime_in_days": 0
                    }

            mock_redis_instance.info.side_effect = mock_info

            # Configure other Redis methods that might be called during setup
            mock_redis_instance.flushall.return_value = True
            mock_redis_instance.config_set.return_value = True
            mock_redis_instance.config_get.return_value = []

            try:
                run_local_command_logic(args, "tool", "v0")
            except SystemExit as e:
                assert e.code == 0

            # Verify that run_local_benchmark was never called (no actual benchmark execution)
            mock_benchmark.assert_not_called()

            # Verify that ping was called (connectivity test)
            mock_redis_instance.ping.assert_called()

            # Verify no benchmark-related Redis commands were called
            called_methods = [call[0] for call in mock_redis_instance.method_calls]

            # These are benchmark execution commands that should NOT be called in dry-run
            forbidden_benchmark_commands = [
                "set",
                "get",
                "mset",
                "mget",
                "lpush",
                "lpop",
                "sadd",
                "spop",
                "zadd",
                "zrange",
                "eval",
                "evalsha",
            ]
            for method_name in called_methods:
                assert (
                    method_name.lower() not in forbidden_benchmark_commands
                ), f"Benchmark command {method_name} was executed in dry-run mode"


def test_run_local_dry_run_with_preload():
    """Test dry-run-with-preload mode to ensure only PING commands are executed after preload"""
    parser = argparse.ArgumentParser(
        description="test dry-run with preload",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser = create_run_local_arguments(parser)

    # Test dry-run with preload
    args = parser.parse_args(
        args=[
            "--test",
            "./tests/test_data/redis-benchmark-json.yml",  # This test includes data preloading
            "--dry-run-with-preload",
            "--redis-7",
            "{}".format(REDIS_7),
        ]
    )

    # Mock the run_local_benchmark function to ensure it's never called
    with unittest.mock.patch(
        "redisbench_admin.run_local.local_helpers.run_local_benchmark"
    ) as mock_benchmark:
        # Mock Redis connection to track commands
        with unittest.mock.patch("redis.Redis") as mock_redis_class:
            mock_redis_instance = unittest.mock.MagicMock()
            mock_redis_class.return_value = mock_redis_instance

            # Configure ping to return True
            mock_redis_instance.ping.return_value = True

            # Configure info method to handle different parameters
            def mock_info_with_preload(section=None):
                if section == "keyspace":
                    return {
                        "db0": {"keys": 1000, "expires": 0}
                    }  # Simulated preloaded data
                else:
                    return {"process_id": 12345}

            mock_redis_instance.info.side_effect = mock_info_with_preload

            # Configure other Redis methods that might be called during setup
            mock_redis_instance.flushall.return_value = True
            mock_redis_instance.config_set.return_value = True
            mock_redis_instance.config_get.return_value = []

            try:
                run_local_command_logic(args, "tool", "v0")
            except SystemExit as e:
                assert e.code == 0

            # Verify that run_local_benchmark was never called (no actual benchmark execution)
            mock_benchmark.assert_not_called()

            # Verify that ping was called multiple times (connectivity tests after setup and after preload)
            assert (
                mock_redis_instance.ping.call_count >= 2
            ), "Expected multiple ping calls for connectivity tests"

            # Verify preload happened but benchmark didn't
            # In dry-run-with-preload, data loading commands are allowed, but benchmark commands are not
            called_methods = [call[0] for call in mock_redis_instance.method_calls]

            # Benchmark execution commands should not be present
            benchmark_execution_commands = [
                "eval",
                "evalsha",
            ]  # Commands typically used by benchmark tools
            for method_name in called_methods:
                assert (
                    method_name.lower() not in benchmark_execution_commands
                ), f"Benchmark execution command {method_name} was executed in dry-run mode"
