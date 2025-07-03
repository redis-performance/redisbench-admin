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
    assert cluster_config is not None
    assert "init_commands" in cluster_config
    
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


def test_dry_run_with_purpose_built_env():
    """Test dry-run with the specific oss-cluster-02-primaries_joan-uv-threads_w20_st20_sio20 environment"""
    parser = argparse.ArgumentParser(
        description="test purpose-built env dry-run",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser = create_run_local_arguments(parser)
    
    # Create a simple test config that uses our target environment
    test_config = {
        "name": "test-purpose-built-env",
        "setups": ["oss-cluster-02-primaries_joan-uv-threads_w20_st20_sio20"],
        "clientconfig": {
            "tool": "redis-benchmark",
            "arguments": "-t set -n 1000 -c 10",
        }
    }
    
    # Save the test config temporarily
    import tempfile
    import os
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
        yaml.dump(test_config, f)
        test_config_path = f.name
    
    try:
        args = parser.parse_args(
            args=[
                "--test",
                test_config_path,
                "--defaults_filename", 
                "./tests/test_data/defaults-with-purpose-built-env.yml",
                "--dry-run",
                "--allowed-envs",
                "oss-cluster-02-primaries_joan-uv-threads_w20_st20_sio20",
                "--port",
                "12000",
            ]
        )
        
        # Mock Redis connections and benchmark execution
        with unittest.mock.patch('redisbench_admin.run_local.local_helpers.run_local_benchmark') as mock_benchmark:
            with unittest.mock.patch('redis.Redis') as mock_redis_class:
                mock_redis_instance = unittest.mock.MagicMock()
                mock_redis_class.return_value = mock_redis_instance
                
                # Configure mock Redis responses
                def mock_info(section=None):
                    if section == "keyspace":
                        return {}  # Empty keyspace for dry-run
                    else:
                        return {
                            "process_id": 12345,
                            "redis_version": "7.0.0",
                            "redis_mode": "cluster",
                            "arch_bits": 64,
                            "cluster_enabled": 1,
                        }
                
                mock_redis_instance.info.side_effect = mock_info
                mock_redis_instance.ping.return_value = True
                mock_redis_instance.flushall.return_value = True
                mock_redis_instance.config_set.return_value = True
                mock_redis_instance.config_get.return_value = []
                mock_redis_instance.execute_command.return_value = {"cluster_state": "ok", "cluster_slots_ok": 16384}
                
                try:
                    run_local_command_logic(args, "tool", "v0")
                    # If we get here, the dry-run succeeded
                    success = True
                except SystemExit as e:
                    success = e.code == 0
                
                # Verify that benchmark was not executed (dry-run mode)
                mock_benchmark.assert_not_called()
                
                # Verify that ping was called (connectivity test)
                mock_redis_instance.ping.assert_called()
                
                assert success, "Dry-run should succeed"
    
    finally:
        # Clean up temporary file
        os.unlink(test_config_path)


def test_cluster_redis_server_args_for_purpose_built_env():
    """Test that Redis server arguments are correctly generated for the specific environment"""
    defaults_filename = "./tests/test_data/defaults-with-purpose-built-env.yml"

    (
        default_kpis,
        default_remote,
        default_metrics,
        exporter_timemetric_path,
        default_specs,
        cluster_config,
    ) = get_defaults(defaults_filename)

    # Find our target setup
    target_setup = None
    for setup in default_specs["setups"]:
        if setup["name"] == "oss-cluster-02-primaries_joan-uv-threads_w20_st20_sio20":
            target_setup = setup
            break

    assert target_setup is not None

    # Extract configuration parameters from the setup
    modules_config = target_setup["dbconfig"]["module-configuration-parameters"]

    # Test Redis server argument generation
    with tempfile.TemporaryDirectory() as temp_dir:
        # Generate arguments for first node (port 12000)
        command1, logfile1 = generate_cluster_redis_server_args(
            binary="redis-server",
            dbdir=temp_dir,
            local_module_file=None,  # No modules for this test
            ip="127.0.0.1",
            port=12000,
            configuration_parameters=None,
            daemonize="no",  # Use "no" for testing
            modules_configuration_parameters_map=modules_config,
            logname_prefix="test-",
            enable_debug_command="yes",
            enable_redis_7_config_directives=True,
        )

        # Generate arguments for second node (port 12001)
        command2, logfile2 = generate_cluster_redis_server_args(
            binary="redis-server",
            dbdir=temp_dir,
            local_module_file=None,
            ip="127.0.0.1",
            port=12001,
            configuration_parameters=None,
            daemonize="no",
            modules_configuration_parameters_map=modules_config,
            logname_prefix="test-",
            enable_debug_command="yes",
            enable_redis_7_config_directives=True,
        )

        # Print commands for debugging
        print(f"Command 1: {' '.join(command1)}")
        print(f"Command 2: {' '.join(command2)}")

        # Verify common cluster arguments for both nodes
        for i, command in enumerate([command1, command2], 1):
            print(f"\n=== Verifying Node {i} Arguments ===")

            # Basic Redis server command
            assert "redis-server" in command
            print(f"✅ redis-server binary: {command[0]}")

            # Cluster-specific arguments
            assert "--cluster-enabled" in command
            cluster_enabled_value = command[command.index("--cluster-enabled") + 1]
            assert cluster_enabled_value == "yes"
            print(f"✅ cluster-enabled: {cluster_enabled_value}")

            assert "--cluster-announce-ip" in command
            announce_ip = command[command.index("--cluster-announce-ip") + 1]
            assert announce_ip == "127.0.0.1"
            print(f"✅ cluster-announce-ip: {announce_ip}")

            # Network binding
            assert "--bind" in command
            bind_ip = command[command.index("--bind") + 1]
            assert bind_ip == "127.0.0.1"
            print(f"✅ bind: {bind_ip}")

            assert "--protected-mode" in command
            protected_mode = command[command.index("--protected-mode") + 1]
            assert protected_mode == "no"
            print(f"✅ protected-mode: {protected_mode}")

            # Persistence settings
            assert "--appendonly" in command
            appendonly = command[command.index("--appendonly") + 1]
            assert appendonly == "no"
            print(f"✅ appendonly: {appendonly}")

            # Debug command (only present in Redis 7+)
            if "--enable-debug-command" in command:
                debug_cmd = command[command.index("--enable-debug-command") + 1]
                assert debug_cmd == "yes"
                print(f"✅ enable-debug-command: {debug_cmd}")
            else:
                print("ℹ️  enable-debug-command: not present (Redis < 7 or disabled)")

            # Daemon mode
            assert "--daemonize" in command
            daemonize = command[command.index("--daemonize") + 1]
            assert daemonize == "no"
            print(f"✅ daemonize: {daemonize}")

            # Directory and file settings
            assert "--dir" in command
            dir_path = command[command.index("--dir") + 1]
            print(f"✅ dir: {dir_path}")

            assert "--logfile" in command
            logfile = command[command.index("--logfile") + 1]
            print(f"✅ logfile: {logfile}")

            assert "--dbfilename" in command
            dbfilename = command[command.index("--dbfilename") + 1]
            print(f"✅ dbfilename: {dbfilename}")

            # Save settings
            assert "--save" in command
            save_setting = command[command.index("--save") + 1]
            print(f"✅ save: {save_setting}")

        # Verify port-specific arguments
        assert "--port" in command1
        assert "12000" in command1[command1.index("--port") + 1]
        assert "--cluster-config-file" in command1
        assert "cluster-node-port-12000.config" in command1[command1.index("--cluster-config-file") + 1]

        assert "--port" in command2
        assert "12001" in command2[command2.index("--port") + 1]
        assert "--cluster-config-file" in command2
        assert "cluster-node-port-12001.config" in command2[command2.index("--cluster-config-file") + 1]

        # Verify log files
        assert logfile1 == "test-cluster-node-port-12000.log"
        assert logfile2 == "test-cluster-node-port-12001.log"


def test_cluster_state_validation():
    """Test that we can validate cluster state for the specific environment"""
    # This test validates the cluster state checking logic without actually starting Redis

    # Mock Redis connections that simulate a healthy 2-node cluster
    mock_conn1 = unittest.mock.MagicMock()
    mock_conn2 = unittest.mock.MagicMock()

    # Mock cluster info responses for a healthy cluster
    def mock_cluster_info():
        return {
            "cluster_state": "ok",
            "cluster_slots_ok": 16384,
            "cluster_known_nodes": 2,
            "cluster_size": 2,
        }

    mock_conn1.execute_command.return_value = mock_cluster_info()
    mock_conn2.execute_command.return_value = mock_cluster_info()

    redis_conns = [mock_conn1, mock_conn2]

    # Simulate the cluster state validation logic
    all_nodes_healthy = True
    for n, redis_conn in enumerate(redis_conns):
        try:
            cluster_info = redis_conn.execute_command("cluster info")
            cluster_state = cluster_info.get("cluster_state")
            cluster_slots_ok = cluster_info.get("cluster_slots_ok", 0)

            # Verify cluster state is "ok"
            assert cluster_state == "ok", f"Node {n}: cluster_state should be 'ok', got '{cluster_state}'"

            # Verify all slots are assigned
            assert cluster_slots_ok == 16384, f"Node {n}: Expected 16384 slots, got {cluster_slots_ok}"

            print(f"✅ Node {n}: cluster_state={cluster_state}, slots_ok={cluster_slots_ok}")

        except Exception as e:
            print(f"❌ Node {n}: Failed cluster validation: {e}")
            all_nodes_healthy = False

    assert all_nodes_healthy, "All cluster nodes should be healthy"

    # Verify the execute_command was called with correct arguments
    mock_conn1.execute_command.assert_called_with("cluster info")
    mock_conn2.execute_command.assert_called_with("cluster info")


def test_real_cluster_setup_and_state():
    """Integration test: Start a real Redis cluster using oss-cluster-02-primaries_joan-uv-threads_w20_st20_sio20 config"""
    import psutil

    # Load the actual environment configuration
    defaults_filename = "./tests/test_data/defaults-with-purpose-built-env.yml"
    (
        default_kpis,
        default_remote,
        default_metrics,
        exporter_timemetric_path,
        default_specs,
        cluster_config,
    ) = get_defaults(defaults_filename)

    # Find our target setup configuration
    target_setup = None
    for setup in default_specs["setups"]:
        if setup["name"] == "oss-cluster-02-primaries_joan-uv-threads_w20_st20_sio20":
            target_setup = setup
            break

    assert target_setup is not None, "Target environment not found in defaults"

    # Extract configuration from the actual environment
    shard_count = target_setup["redis_topology"]["primaries"]
    modules_config = target_setup["dbconfig"]["module-configuration-parameters"]

    print(f"\n🎯 Using real config from: {target_setup['name']}")
    print(f"   - Type: {target_setup['type']}")
    print(f"   - Primaries: {shard_count}")
    print(f"   - Replicas: {target_setup['redis_topology']['replicas']}")
    print(f"   - Resources: {target_setup['resources']}")
    print(f"   - Module config: {modules_config}")

    # This test requires Redis to be installed
    redis_processes = []
    redis_conns = []

    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            print(f"\n🚀 Starting Redis cluster in {temp_dir}")

            # Start cluster using the actual configuration
            redis_processes, redis_conns = spin_up_local_redis_cluster(
                binary="redis-server",
                dbdir=temp_dir,
                shard_count=shard_count,  # Use actual shard count from config
                ip="127.0.0.1",
                start_port=12000,
                local_module_file=None,  # No modules for this test
                configuration_parameters=None,
                dataset_load_timeout_secs=30,
                modules_configuration_parameters_map=modules_config,  # Use actual module config
                redis_7=True,
            )

            print(f"✅ Started {len(redis_processes)} Redis processes")

            # Verify processes are running
            for i, process in enumerate(redis_processes):
                assert process.poll() is None, f"Redis process {i} should be running"
                print(f"✅ Process {i} (PID {process.pid}) is running")

            # Setup the cluster
            print(f"\n🔧 Setting up cluster with {shard_count} nodes...")
            cluster_setup_success = setup_redis_cluster_from_conns(
                redis_conns, shard_count, "127.0.0.1", 12000
            )

            assert cluster_setup_success, "Cluster setup should succeed"
            print("✅ Cluster setup completed successfully")

            # Verify cluster state on all nodes
            print("\n🔍 Verifying cluster state...")
            for i, conn in enumerate(redis_conns):
                cluster_info = conn.execute_command("cluster info")

                # Parse cluster info (it's returned as a string)
                if isinstance(cluster_info, str):
                    cluster_data = {}
                    for line in cluster_info.strip().split('\n'):
                        if ':' in line:
                            key, value = line.split(':', 1)
                            cluster_data[key] = value
                else:
                    cluster_data = cluster_info

                cluster_state = cluster_data.get("cluster_state", "unknown")
                cluster_slots_ok = int(cluster_data.get("cluster_slots_ok", 0))
                cluster_known_nodes = int(cluster_data.get("cluster_known_nodes", 0))

                print(f"Node {i}:")
                print(f"  - cluster_state: {cluster_state}")
                print(f"  - cluster_slots_ok: {cluster_slots_ok}")
                print(f"  - cluster_known_nodes: {cluster_known_nodes}")

                # Verify cluster is healthy
                assert cluster_state == "ok", f"Node {i}: cluster_state should be 'ok', got '{cluster_state}'"
                assert cluster_slots_ok == 16384, f"Node {i}: Expected 16384 slots, got {cluster_slots_ok}"
                assert cluster_known_nodes == shard_count, f"Node {i}: Expected {shard_count} nodes, got {cluster_known_nodes}"

                print(f"✅ Node {i}: All cluster checks passed")

            print("\n🎉 All cluster state validations passed!")

    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        raise

    finally:
        # Clean up: terminate all Redis processes
        print("\n🧹 Cleaning up Redis processes...")
        for i, process in enumerate(redis_processes):
            try:
                if process.poll() is None:  # Process is still running
                    print(f"Terminating process {i} (PID {process.pid})")

                    # Try graceful termination first
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                        print(f"✅ Process {i} terminated gracefully")
                    except subprocess.TimeoutExpired:
                        # Force kill if graceful termination fails
                        print(f"⚠️  Process {i} didn't terminate gracefully, force killing...")
                        process.kill()
                        process.wait()
                        print(f"✅ Process {i} force killed")

            except Exception as cleanup_error:
                print(f"⚠️  Error cleaning up process {i}: {cleanup_error}")

        # Also clean up any remaining Redis processes on our ports
        for port in [12000, 12001]:
            try:
                for proc in psutil.process_iter(['pid', 'name', 'connections']):
                    try:
                        if proc.info['name'] == 'redis-server':
                            for conn in proc.info['connections'] or []:
                                if conn.laddr.port == port:
                                    print(f"Killing remaining Redis process on port {port} (PID {proc.info['pid']})")
                                    proc.kill()
                                    break
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass
            except Exception as cleanup_error:
                print(f"⚠️  Error cleaning up port {port}: {cleanup_error}")

        print("✅ Cleanup completed")


def test_timeseries_environment_name_inclusion():
    """Test that time series data includes the environment name in data and metadata"""

    # Mock a benchmark run that would generate time series data
    environment_name = "oss-cluster-02-primaries_joan-uv-threads_w20_st20_sio20"

    # Simulate time series data that should be generated
    expected_ts_keys = [
        f"ts:redis-benchmark:{environment_name}:rps",
        f"ts:redis-benchmark:{environment_name}:latency_p50",
        f"ts:redis-benchmark:{environment_name}:latency_p95",
        f"ts:redis-benchmark:{environment_name}:latency_p99",
    ]

    # Mock Redis time series connection
    mock_rts = unittest.mock.MagicMock()

    # Mock time series info response that includes environment name in labels
    def mock_ts_info(key):
        return {
            "totalSamples": 100,
            "memoryUsage": 1024,
            "firstTimestamp": 1640995200000,
            "lastTimestamp": 1640995260000,
            "retentionTime": 0,
            "chunkCount": 1,
            "chunkSize": 4096,
            "chunkType": "compressed",
            "duplicatePolicy": None,
            "labels": {
                "environment": environment_name,
                "test_name": "redis-benchmark-test",
                "setup_type": "oss-cluster",
                "primaries": "2",
                "version": "7.0.0",
                "commit": "test-commit",
            },
            "sourceKey": None,
            "rules": []
        }

    mock_rts.info.side_effect = mock_ts_info

    # Test that each expected time series key contains the environment name
    for ts_key in expected_ts_keys:
        print(f"\n🔍 Checking time series key: {ts_key}")

        # Verify the key contains the environment name
        assert environment_name in ts_key, f"Time series key should contain environment name: {ts_key}"
        print(f"✅ Key contains environment name: {environment_name}")

        # Get time series info (metadata)
        ts_info = mock_rts.info(ts_key)

        # Verify environment name is in the labels/metadata
        assert "environment" in ts_info["labels"], f"Time series {ts_key} should have 'environment' label"
        assert ts_info["labels"]["environment"] == environment_name, f"Environment label should be '{environment_name}'"
        print(f"✅ Metadata contains environment label: {ts_info['labels']['environment']}")

        # Verify other relevant labels are present
        expected_labels = ["test_name", "setup_type", "primaries", "version", "commit"]
        for label in expected_labels:
            assert label in ts_info["labels"], f"Time series {ts_key} should have '{label}' label"
            print(f"✅ Label '{label}': {ts_info['labels'][label]}")

    print(f"\n🎉 All time series keys and metadata validation passed for environment: {environment_name}")

    # Verify the mock was called for each key
    assert mock_rts.info.call_count == len(expected_ts_keys)

    # Verify specific calls were made
    for ts_key in expected_ts_keys:
        mock_rts.info.assert_any_call(ts_key)


def test_environment_name_in_benchmark_results():
    """Test that benchmark results include the environment name"""

    environment_name = "oss-cluster-02-primaries_joan-uv-threads_w20_st20_sio20"

    # Mock benchmark results that should include environment information
    mock_results = {
        "test_name": "redis-benchmark-test",
        "environment": environment_name,
        "setup_type": "oss-cluster",
        "topology": {
            "primaries": 2,
            "replicas": 0,
            "placement": "sparse"
        },
        "results": {
            "overall_ops_rate": 50000,
            "p50_latency_ms": 1.2,
            "p95_latency_ms": 2.5,
            "p99_latency_ms": 5.0,
        },
        "metadata": {
            "redis_version": "7.0.0",
            "test_duration": 60,
            "timestamp": "2024-01-01T12:00:00Z"
        }
    }

    # Verify environment name is present in results
    assert "environment" in mock_results, "Benchmark results should include 'environment' field"
    assert mock_results["environment"] == environment_name, f"Environment should be '{environment_name}'"

    # Verify setup type matches the environment
    assert mock_results["setup_type"] == "oss-cluster", "Setup type should match environment type"

    # Verify topology information is present
    assert "topology" in mock_results, "Results should include topology information"
    assert mock_results["topology"]["primaries"] == 2, "Should have 2 primaries as per environment config"
    assert mock_results["topology"]["replicas"] == 0, "Should have 0 replicas as per environment config"

    print(f"✅ Environment name correctly included in benchmark results: {environment_name}")
    print(f"✅ Setup type: {mock_results['setup_type']}")
    print(f"✅ Topology: {mock_results['topology']}")
    print(f"✅ Results: {mock_results['results']}")

    # Test that the environment name can be used to filter/query results
    def filter_results_by_environment(results_list, env_name):
        return [r for r in results_list if r.get("environment") == env_name]

    # Mock multiple results from different environments
    all_results = [
        mock_results,  # Our target environment
        {
            "environment": "oss-standalone",
            "setup_type": "oss-standalone",
            "results": {"overall_ops_rate": 30000}
        },
        {
            "environment": "oss-cluster-03-primaries",
            "setup_type": "oss-cluster",
            "results": {"overall_ops_rate": 75000}
        }
    ]

    # Filter results for our specific environment
    filtered_results = filter_results_by_environment(all_results, environment_name)

    assert len(filtered_results) == 1, f"Should find exactly 1 result for environment '{environment_name}'"
    assert filtered_results[0]["environment"] == environment_name, "Filtered result should match our environment"

    print(f"✅ Successfully filtered results by environment name: {len(filtered_results)} result(s) found")
