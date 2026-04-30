#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#

import pytest
import yaml

from redisbench_admin.run.ftsb.ftsb import (
    prepare_ftsb_benchmark_command,
    extract_ftsb_extra_links,
)


def test_prepare_ftsb_benchmark_command_basic():
    """Test basic ftsb command generation without log file."""
    with open(
        "./tests/test_data/ftsb-1M-enwiki_abstract-hashes-fulltext-simple-1word-query.yml",
        "r",
    ) as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        for entry in benchmark_config["clientconfig"]:
            if "parameters" in entry:
                command_arr, command_str = prepare_ftsb_benchmark_command(
                    "ftsb_redisearch",
                    "localhost",
                    6379,
                    entry,
                    ".",
                    "/tmp/result.json",
                    "/tmp/input.data",
                    is_remote=False,
                )
                assert "ftsb_redisearch" in command_str
                assert "--host localhost:6379" in command_str
                assert "--json-out-file /tmp/result.json" in command_str
                assert "--log-file" not in command_str


def test_prepare_ftsb_benchmark_command_with_log_file():
    """Test ftsb command generation with log file."""
    with open(
        "./tests/test_data/ftsb-1M-enwiki_abstract-hashes-fulltext-simple-1word-query.yml",
        "r",
    ) as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        for entry in benchmark_config["clientconfig"]:
            if "parameters" in entry:
                command_arr, command_str = prepare_ftsb_benchmark_command(
                    "ftsb_redisearch",
                    "localhost",
                    6379,
                    entry,
                    ".",
                    "/tmp/result.json",
                    "/tmp/input.data",
                    is_remote=False,
                    log_out_file="/tmp/benchmark.log",
                )
                assert "ftsb_redisearch" in command_str
                assert "--host localhost:6379" in command_str
                assert "--json-out-file /tmp/result.json" in command_str
                assert "--log-file /tmp/benchmark.log" in command_str
                # Verify the order in command_arr
                assert "--log-file" in command_arr
                log_file_idx = command_arr.index("--log-file")
                assert command_arr[log_file_idx + 1] == "/tmp/benchmark.log"


def test_prepare_ftsb_benchmark_command_with_password():
    """Test ftsb command generation with password."""
    with open(
        "./tests/test_data/ftsb-1M-enwiki_abstract-hashes-fulltext-simple-1word-query.yml",
        "r",
    ) as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        for entry in benchmark_config["clientconfig"]:
            if "parameters" in entry:
                command_arr, command_str = prepare_ftsb_benchmark_command(
                    "ftsb_redisearch",
                    "localhost",
                    6379,
                    entry,
                    ".",
                    "/tmp/result.json",
                    "/tmp/input.data",
                    is_remote=False,
                    redis_password="secret123",
                    log_out_file="/tmp/benchmark.log",
                )
                assert "--a secret123" in command_str
                assert "--log-file /tmp/benchmark.log" in command_str


def test_prepare_ftsb_benchmark_command_cluster_mode():
    """Test ftsb command generation with cluster mode enabled."""
    with open(
        "./tests/test_data/ftsb-1M-enwiki_abstract-hashes-fulltext-simple-1word-query.yml",
        "r",
    ) as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        for entry in benchmark_config["clientconfig"]:
            if "parameters" in entry:
                command_arr, command_str = prepare_ftsb_benchmark_command(
                    "ftsb_redisearch",
                    "localhost",
                    6379,
                    entry,
                    ".",
                    "/tmp/result.json",
                    "/tmp/input.data",
                    is_remote=False,
                    cluster_api_enabled=True,
                    log_out_file="/tmp/benchmark.log",
                )
                assert "--cluster-mode" in command_str
                assert "--log-file /tmp/benchmark.log" in command_str


@pytest.mark.parametrize(
    "parameters,batch_value",
    [
        ([{"batch-size": 100}], "100"),
        ([{"batch_size": 250}], "250"),
        ({"batch-size": 500}, "500"),
        ({"batch_size": 750}, "750"),
    ],
    ids=["list-hyphen", "list-underscore", "dict-hyphen", "dict-underscore"],
)
def test_prepare_ftsb_benchmark_command_batch_size(parameters, batch_value):
    """Test batch-size is correctly passed for both config formats and key spellings."""
    entry = {"parameters": parameters}
    command_arr, command_str = prepare_ftsb_benchmark_command(
        "ftsb_redisearch",
        "localhost",
        6379,
        entry,
        ".",
        "/tmp/result.json",
        "/tmp/input.data",
        is_remote=False,
    )
    assert "--batch-size" in command_arr
    idx = command_arr.index("--batch-size")
    assert command_arr[idx + 1] == batch_value


@pytest.mark.parametrize(
    "parameters,expected_value",
    [
        ([{"max-latency-seconds": 60}], "60"),
        ([{"max_latency_seconds": 30}], "30"),
        ({"max-latency-seconds": 60}, "60"),
        ({"max_latency_seconds": 30}, "30"),
    ],
    ids=["list-hyphen", "list-underscore", "dict-hyphen", "dict-underscore"],
)
def test_prepare_ftsb_benchmark_command_max_latency_seconds(parameters, expected_value):
    """Test max-latency-seconds is correctly passed for both config formats and key spellings."""
    entry = {"parameters": parameters}
    command_arr, command_str = prepare_ftsb_benchmark_command(
        "ftsb_redisearch",
        "localhost",
        6379,
        entry,
        ".",
        "/tmp/result.json",
        "/tmp/input.data",
        is_remote=False,
    )
    assert "--max-latency-seconds" in command_arr
    idx = command_arr.index("--max-latency-seconds")
    assert command_arr[idx + 1] == expected_value


def test_extract_ftsb_extra_links():
    """Test extraction of ftsb extra links from config."""
    with open(
        "./tests/test_data/ftsb-1M-enwiki_abstract-hashes-fulltext-simple-1word-query.yml",
        "r",
    ) as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        queries_file_link, remote_tool_link, tool_link = extract_ftsb_extra_links(
            benchmark_config, "ftsb_redisearch", "clientconfig"
        )
        assert remote_tool_link == "/tmp/ftsb_redisearch"
        assert "ftsb_redisearch_linux_amd64" in tool_link
        assert queries_file_link is not None
        assert "s3.amazonaws.com" in queries_file_link
