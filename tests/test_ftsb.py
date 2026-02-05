#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#

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
