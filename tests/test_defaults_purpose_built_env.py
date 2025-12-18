import argparse
import unittest.mock
import yaml
import subprocess
import time
import redis
import tempfile
import os

from redisbench_admin.run_local.args import create_run_local_arguments
from redisbench_admin.run_local.run_local import run_local_command_logic
from redisbench_admin.utils.benchmark_config import (
    get_defaults,
    process_default_yaml_properties_file,
)
from redisbench_admin.run.common import extract_test_feasible_setups
from redisbench_admin.environments.oss_cluster import (
    spin_up_local_redis_cluster,
    setup_redis_cluster_from_conns,
    generate_cluster_redis_server_args,
)


def test_defaults_purpose_built_env_parsing():
    """Test that the new defaults-with-purpose-built-env.yml file is properly parsed"""
    defaults_filename = "./tests/test_data/defaults-with-purpose-built-env.yml"

    # Test that the file can be loaded and parsed
    (
        default_kpis,
        default_remote,
        default_metrics,
        exporter_timemetric_path,
        default_specs,
        cluster_config,
    ) = get_defaults(defaults_filename)

    # Verify basic structure
    assert default_specs is not None
    assert "setups" in default_specs

    # Verify the specific environment we're testing
    setups = default_specs["setups"]
    setup_names = [setup["name"] for setup in setups]

    # Check that our target environment exists
    assert "oss-cluster-02-primaries_joan-uv-threads_w20_st20_sio20" in setup_names

    # Find and validate the specific setup
    target_setup = None
    for setup in setups:
        if setup["name"] == "oss-cluster-02-primaries_joan-uv-threads_w20_st20_sio20":
            target_setup = setup
            break

    assert target_setup is not None

    # Validate the setup structure
    assert target_setup["type"] == "oss-cluster"
    assert "redis_topology" in target_setup
    assert "resources" in target_setup
    assert "dbconfig" in target_setup

    # Validate topology
    topology = target_setup["redis_topology"]
    assert topology["primaries"] == 2  # Note: YAML "02" becomes int 2
    assert topology["replicas"] == 0
    assert topology["placement"] == "sparse"

    # Validate resources
    resources = target_setup["resources"]
    assert "requests" in resources
    assert resources["requests"]["cpus"] == "4"
    assert resources["requests"]["memory"] == "180g"

    # Validate dbconfig
    dbconfig = target_setup["dbconfig"]
    assert "module-configuration-parameters" in dbconfig
    module_params = dbconfig["module-configuration-parameters"]
    assert "redisearch" in module_params
    assert "module-oss" in module_params
    assert module_params["redisearch"]["WORKERS"] == 6
    assert module_params["redisearch"]["MIN_OPERATION_WORKERS"] == 6


def test_extract_feasible_setups_with_purpose_built_env():
    """Test that extract_test_feasible_setups works with the new defaults file"""
    defaults_filename = "./tests/test_data/defaults-with-purpose-built-env.yml"

    (
        default_kpis,
        default_remote,
        default_metrics,
        exporter_timemetric_path,
        default_specs,
        cluster_config,
    ) = get_defaults(defaults_filename)

    # Test with a benchmark config that specifies our target environment
    benchmark_config = {
        "setups": ["oss-cluster-02-primaries_joan-uv-threads_w20_st20_sio20"]
    }

    feasible_setups = extract_test_feasible_setups(
        benchmark_config, "setups", default_specs, backwards_compatible=False
    )

    # Verify the setup was found and extracted correctly
    assert len(feasible_setups) == 1
    assert "oss-cluster-02-primaries_joan-uv-threads_w20_st20_sio20" in feasible_setups

    setup = feasible_setups["oss-cluster-02-primaries_joan-uv-threads_w20_st20_sio20"]
    assert setup["type"] == "oss-cluster"
    assert setup["redis_topology"]["primaries"] == 2
    assert setup["redis_topology"]["replicas"] == 0


def test_dry_run_with_simple_standalone_env():
    """Test dry-run with real Redis using a simple oss-standalone environment (no modules)"""
    parser = argparse.ArgumentParser(
        description="test simple env dry-run",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser = create_run_local_arguments(parser)

    # Create a simple test config that uses a standalone environment (no modules)
    test_config = {
        "name": "test-simple-env",
        "setups": ["oss-standalone"],
        "clientconfig": [
            {"tool": "redis-benchmark"},
            {"parameters": [{"clients": 5}, {"requests": 100}, {"test": "set"}]},
        ],
    }

    # Ensure we have the test DB to store results
    assert "RTS_PORT" in os.environ
    rts_port = os.environ.get("RTS_PORT", None)
    rts = redis.Redis(port=rts_port, decode_responses=True)
    rts.ping()
    rts.flushall()

    # Save the test config temporarily
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        yaml.dump(test_config, f)
        test_config_path = f.name

    try:
        args = parser.parse_args(
            args=[
                "--test",
                test_config_path,
                "--defaults_filename",
                "./tests/test_data/defaults-with-dbconfig.yml",  # Use simpler defaults
                "--dry-run",
                "--allowed-envs",
                "oss-standalone",
                "--port",
                "12000",
                "--push_results_redistimeseries",
                "--redistimeseries_port",
                f"{rts_port}",
            ]
        )

        try:
            run_local_command_logic(args, "tool", "v0")
            success = True
        except SystemExit as e:
            success = e.code == 0

        assert success, "Dry-run with real Redis standalone should succeed"
        print("✅ Dry-run completed successfully with real Redis standalone")

        assert rts.info("keyspace")["db0"]["keys"] >= 0

    finally:
        # Clean up temporary file
        os.unlink(test_config_path)


def test_dry_run_with_purpose_built_env():
    """Test dry-run with real Redis using oss-cluster-02-primaries_joan-uv-threads_w20_st20_sio20 environment"""
    parser = argparse.ArgumentParser(
        description="test purpose-built env dry-run",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser = create_run_local_arguments(parser)

    # Env name
    setup_name = "oss-cluster-02-primaries_joan-uv-threads_w20_st20_sio20"

    # Ensure we have the test DB to store results
    assert "RTS_PORT" in os.environ
    rts_port = os.environ.get("RTS_PORT", None)
    rts = redis.Redis(port=rts_port, decode_responses=True)
    rts.ping()
    rts.flushall()

    # Create a simple test config that uses our target environment
    test_config = {
        "name": "test-purpose-built-env",
        "setups": [setup_name],
        "clientconfig": [
            {"tool": "redis-benchmark"},
            {"parameters": [{"clients": 5}, {"requests": 100}, {"test": "set"}]},
        ],
    }

    # Save the test config temporarily
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        yaml.dump(test_config, f)
        test_config_path = f.name

    triggering_env_name = "test-triggering-env"
    try:
        args = parser.parse_args(
            args=[
                "--test",
                test_config_path,
                "--defaults_filename",
                "./tests/test_data/defaults-with-purpose-built-env.yml",
                "--dry-run",
                "--allowed-envs",
                setup_name,
                "--port",
                "12100",
                "--push_results_redistimeseries",
                "--redistimeseries_port",
                f"{rts_port}",
                "--triggering_env",
                triggering_env_name,
            ]
        )

        try:
            run_local_command_logic(args, "tool", "v0")
            success = True
        except SystemExit as e:
            success = e.code == 0

        # ensure we have data
        assert rts.info("keyspace")["db0"]["keys"] >= 0

        # ensure that the environemt name is present on timeseries

        keys_from_deployment = rts.ts().queryindex([f"deployment_name={setup_name}"])
        assert len(keys_from_deployment) > 0

        assert setup_name in rts.zrange(
            f"ci.benchmarks.redislabs/{triggering_env_name}/redis-performance/redisbench-admin:deployment_names",
            0,
            -1,
        )

        assert success, "Dry-run with real Redis should succeed"
        print("✅ Dry-run completed successfully with real Redis cluster")

    finally:
        # Clean up temporary file
        os.unlink(test_config_path)
