#  BSD 3-Clause License
#
#  Copyright (c) 2022., Redis Labs Modules
#  All rights reserved.
#
import yaml

from redisbench_admin.run.memtier_benchmark.memtier_benchmark import (
    extract_monitor_input_from_arguments,
    replace_monitor_input_in_arguments,
    process_monitor_input_in_arguments,
    prepare_memtier_benchmark_command,
)


def test_extract_monitor_input_from_arguments():
    """Test extraction of --monitor-input value from arguments string."""
    # Test with space separator
    args = "--test-time 120 --monitor-input /path/to/file.txt --command test"
    assert extract_monitor_input_from_arguments(args) == "/path/to/file.txt"

    # Test with equals separator
    args = "--test-time 120 --monitor-input=/path/to/file.txt --command test"
    assert extract_monitor_input_from_arguments(args) == "/path/to/file.txt"

    # Test with HTTP URL
    args = "--monitor-input https://example.com/file.txt --command test"
    assert extract_monitor_input_from_arguments(args) == "https://example.com/file.txt"

    # Test with S3 URL
    args = "--monitor-input s3://bucket/path/file.txt --command test"
    assert extract_monitor_input_from_arguments(args) == "s3://bucket/path/file.txt"

    # Test with quoted value
    args = "--monitor-input '/path/with spaces/file.txt' --command test"
    assert extract_monitor_input_from_arguments(args) == "/path/with spaces/file.txt"

    # Test with no monitor-input
    args = "--test-time 120 --command test"
    assert extract_monitor_input_from_arguments(args) is None

    # Test with empty string
    assert extract_monitor_input_from_arguments("") is None
    assert extract_monitor_input_from_arguments(None) is None


def test_replace_monitor_input_in_arguments():
    """Test replacement of --monitor-input value in arguments string."""
    # Test replacing URL with local path
    args = "--test-time 120 --monitor-input https://example.com/file.txt --command test"
    result = replace_monitor_input_in_arguments(
        args, "https://example.com/file.txt", "/tmp/file.txt"
    )
    assert "--monitor-input /tmp/file.txt" in result
    assert "https://example.com/file.txt" not in result

    # Test with equals separator
    args = "--monitor-input=https://example.com/file.txt --command test"
    result = replace_monitor_input_in_arguments(
        args, "https://example.com/file.txt", "/tmp/file.txt"
    )
    assert (
        "--monitor-input=/tmp/file.txt" in result
        or "--monitor-input /tmp/file.txt" in result
    )

    # Test with S3 URL
    args = "--monitor-input s3://bucket/file.txt --command test"
    result = replace_monitor_input_in_arguments(
        args, "s3://bucket/file.txt", "/tmp/file.txt"
    )
    assert "/tmp/file.txt" in result
    assert "s3://bucket/file.txt" not in result


def test_process_monitor_input_in_arguments_local_file():
    """Test that local file paths are not modified."""
    args = "--monitor-input /local/path/file.txt --command test"
    result, url = process_monitor_input_in_arguments(args)
    assert result == args
    assert url is None


def test_process_monitor_input_in_arguments_remote():
    """Test that remote execution uses provided remote_monitor_file path."""
    args = "--monitor-input https://example.com/file.txt --command test"
    result, url = process_monitor_input_in_arguments(
        args,
        local_temp_dir="/tmp",
        is_remote=True,
        remote_monitor_file="/tmp/downloaded.txt",
    )
    assert "--monitor-input /tmp/downloaded.txt" in result
    assert url == "https://example.com/file.txt"


def test_prepare_memtier_benchmark_command():
    with open("./tests/test_data/vecsim-memtier.yml", "r") as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        command_arr, command_str = prepare_memtier_benchmark_command(
            "memtier_benchmark",
            "localhost",
            "6380",
            benchmark_config["clientconfig"],
            False,
            "result.json",
        )
        assert (
            command_str
            == "memtier_benchmark -s localhost -p 6380 --hide-histogram --json-out-file result.json --command \"FT.SEARCH idx 'text0=>[KNN $k @hnsw_vector $BLOB]' PARAMS 4 k 10 BLOB aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\" --test-time 180 -c 8 -t 2 --hide-histogram"
        )
        assert (
            command_arr[9]
            == "FT.SEARCH idx 'text0=>[KNN $k @hnsw_vector $BLOB]' PARAMS 4 k 10 BLOB aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        )
        assert len(command_arr) == 17


def test_prepare_memtier_benchmark_command_with_monitor_input_remote():
    """Test prepare_memtier_benchmark_command handles monitor-input URL for remote execution."""
    with open("./tests/test_data/vanilla-memtier-monitor-input.yml", "r") as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        command_arr, command_str = prepare_memtier_benchmark_command(
            "memtier_benchmark",
            "localhost",
            "6380",
            benchmark_config["clientconfig"],
            False,
            "result.json",
            is_remote=True,
            remote_monitor_file="/tmp/monitor.txt",
        )
        # Should have replaced the URL with the remote path
        assert "--monitor-input /tmp/monitor.txt" in command_str
        assert "https://s3.us-east-1.amazonaws.com" not in command_str
