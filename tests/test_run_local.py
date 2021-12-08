import os

import argparse
import redis
import yaml
from redistimeseries.client import Client

from redisbench_admin.profilers.pprof import process_pprof_text_to_tabular
from redisbench_admin.run_local.args import create_run_local_arguments
from redisbench_admin.run_local.local_helpers import (
    check_benchmark_binaries_local_requirements,
)
from redisbench_admin.run_local.profile_local import get_profilers_rts_key_prefix
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
        rts = Client()
        rts.redis.ping()
        rts.redis.flushall()
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
        assert rts.redis.exists(zset_profiles_key_name)
        assert rts.redis.zcard(zset_profiles_key_name) == 1

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
            assert rts.redis.exists(table_columns_text_key)
        # assert rts.redis.exists(testcases_setname)
        # assert rts.redis.exists(running_platforms_setname)
        # assert rts.redis.exists(build_variant_setname)

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

    r = redis.StrictRedis()
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
        ]
    )
    try:
        run_local_command_logic(args, "tool", "v0")
    except SystemExit as e:
        assert e.code == 1

    ## run while pushing results to rts
    rts_host = os.getenv("RTS_DATASINK_HOST", None)
    rts_port = 16379
    if rts_host is None:
        assert False
    rts = Client(port=16379, host=rts_host)
    rts.redis.ping()
    rts.redis.flushall()
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
        ]
    )
    try:
        run_local_command_logic(args, "tool", "v0")
    except SystemExit as e:
        assert e.code == 0
