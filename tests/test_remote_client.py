#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import logging
import os
import shutil
import signal
import subprocess

import pytest
import yaml

import redisbench_admin.run_remote.remote_client as remote_client_module
from redisbench_admin.run.common import prepare_benchmark_parameters
from redisbench_admin.run_remote.remote_helpers import post_process_remote_run
from redisbench_admin.utils.benchmark_config import extract_benchmark_tool_settings


def test_run_remote_benchmark():
    port = 2222
    username = "ubuntu"
    private_key = "./tests/test_data/test-ssh/tox_rsa"
    client_public_ip = "localhost"
    with open("./tests/test_data/redis-benchmark-vanilla.yml", "r") as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        (
            benchmark_min_tool_version,
            benchmark_min_tool_version_major,
            benchmark_min_tool_version_minor,
            benchmark_min_tool_version_patch,
            benchmark_tool,
            benchmark_tool_source,
            benchmark_tool_source_bin_path,
            _,
        ) = extract_benchmark_tool_settings(benchmark_config)
        assert benchmark_tool is not None
        remote_results_file = "out.remote.txt"
        _, command_str = prepare_benchmark_parameters(
            benchmark_config,
            benchmark_tool,
            "9999",
            "localhost",
            remote_results_file,
            True,
        )
        # remote_run_result, stdout, stderr = run_remote_benchmark(
        #     client_public_ip,
        #     username,
        #     DEFAULT_PRIVATE_KEY,
        #     "/tmp/out.txt",
        #     "/tmp/out.txt",
        #     command_str,
        #     port,
        # )
        # assert remote_run_result is False


def test_concurrent_benchmark_command_succeeds_only_when_both_clients_succeed():
    success = remote_client_module.concurrent_benchmark_command("true", "true")
    assert "exec true > /dev/null 2>&1" in success
    assert subprocess.run(success, shell=True, check=False).returncode == 0

    primary_failure = remote_client_module.concurrent_benchmark_command(
        "exit 2", "true"
    )
    assert subprocess.run(primary_failure, shell=True, check=False).returncode != 0

    background_failure = remote_client_module.concurrent_benchmark_command(
        "true", "sh -c 'exit 3'"
    )
    assert subprocess.run(background_failure, shell=True, check=False).returncode != 0


@pytest.mark.parametrize("shell", ["sh", "bash"])
def test_concurrent_benchmark_command_kills_background_client_on_primary_failure(
    shell, tmp_path
):
    # bash (the login shell on the remote client VMs) forks a background
    # command group into a subshell, so killing $! must reach the client
    # itself and not only its parent subshell.
    if shutil.which(shell) is None:
        pytest.skip("{} not available".format(shell))
    pid_file = tmp_path / "background.pid"
    background = "sh -c 'echo $$ > {}; exec sleep 30'".format(pid_file)
    command = remote_client_module.concurrent_benchmark_command(
        "sleep 0.5; exit 2", background
    )
    result = subprocess.run([shell, "-c", command], check=False, timeout=10)
    assert result.returncode != 0

    background_pid = int(pid_file.read_text().strip())
    try:
        with pytest.raises(ProcessLookupError):
            os.kill(background_pid, 0)
    finally:
        try:
            os.kill(background_pid, signal.SIGKILL)
        except ProcessLookupError:
            pass


def _patch_remote_client_tool(monkeypatch, tools_by_config_key, captured):
    def fake_tool_settings(_benchmark_config, config_key):
        tool = tools_by_config_key[config_key]
        return None, None, None, None, tool, None, None, None

    def fake_prepare(*args, **kwargs):
        config_key = args[8]
        return [], "{}-command".format(config_key)

    def fake_run_remote_benchmark(*args, **kwargs):
        captured["commands"] = args[5]
        return True, [], []

    monkeypatch.setattr(
        remote_client_module, "extract_benchmark_tool_settings", fake_tool_settings
    )
    monkeypatch.setattr(
        remote_client_module, "benchmark_tools_sanity_check", lambda *args: None
    )
    monkeypatch.setattr(
        remote_client_module, "remote_tool_pre_bench_step", lambda *args: None
    )
    monkeypatch.setattr(
        remote_client_module, "prepare_benchmark_parameters", fake_prepare
    )
    monkeypatch.setattr(
        remote_client_module, "run_remote_benchmark", fake_run_remote_benchmark
    )
    monkeypatch.setattr(
        remote_client_module,
        "calculate_client_tool_duration_and_check",
        lambda *args: 1,
    )


def _run_remote_client_tool(benchmark_config):
    return remote_client_module.run_remote_client_tool(
        "memtier_benchmark,ftsb_redisearch",
        "version",
        benchmark_config,
        "client.example",
        False,
        "/tmp/primary.json",
        "/tmp/primary.out",
        0,
        "6379",
        "server.example",
        0,
        "start",
        "ubuntu",
        "clientconfig",
        "linux",
        "amd64",
        "Benchmark",
        0,
        22,
        "private-key",
        do_post_process=False,
    )


def test_run_remote_client_tool_wires_background_client(monkeypatch):
    benchmark_config = {
        "clientconfig": {"tool": "memtier_benchmark"},
        "background_clientconfig": {"tool": "memtier_benchmark"},
    }
    captured = {}
    _patch_remote_client_tool(
        monkeypatch,
        {
            "clientconfig": "memtier_benchmark",
            "background_clientconfig": "memtier_benchmark",
        },
        captured,
    )

    _run_remote_client_tool(benchmark_config)

    command = captured["commands"][0]
    assert "exec background_clientconfig-command > /dev/null 2>&1" in command
    assert "clientconfig-command" in command


def test_run_remote_client_tool_rejects_unsupported_background_tool(monkeypatch):
    benchmark_config = {
        "clientconfig": {"tool": "memtier_benchmark"},
        "background_clientconfig": {"tool": "ftsb_redisearch"},
    }
    captured = {}
    _patch_remote_client_tool(
        monkeypatch,
        {
            "clientconfig": "memtier_benchmark",
            "background_clientconfig": "ftsb_redisearch",
        },
        captured,
    )

    with pytest.raises(Exception, match="not supported as a background client"):
        _run_remote_client_tool(benchmark_config)
    assert "commands" not in captured


def test_print_commands_outputs_logs_redacted_commands(caplog):
    caplog.set_level(logging.INFO)
    remote_client_module.print_commands_outputs(
        ["memtier_benchmark -a sensitive-password-value"],
        True,
        [(1, ["out"], ["err"])],
    )
    assert "sensitive-password-value" not in caplog.text
    assert "Exit status for command memtier_benchmark -a ***: 1" in caplog.text


def test_ann_benchmark_remote_cmds():
    pkg_path = "/usr/local/lib/python3.6/dist-packages/redisbench_admin"
    local_bench_fname = "local-fname.log"
    (
        create_website_command,
        mkdir_command,
        results_outputdir_zip,
        results_outputdir_zip_local,
        website_outputdir_zip,
        website_outputdir_zip_local,
        zip_results_command,
        zip_website_command,
    ) = remote_client_module.ann_benchmark_remote_cmds(local_bench_fname, pkg_path)
    assert (
        create_website_command
        == "cd {}/run/ann/pkg/ && sudo python3 create_website.py --scatter --outputdir /tmp/website-local-fnam".format(
            pkg_path
        )
    )


def test_is_ycsb_java():
    commands = [
        "memtier_benchmark -s 10.3.0.117 -p 6379 --hide-histogram --json-out-file /tmp/benchmark-result-search-wildcard-nosorting_2022-07-25-14-01-44.out --test-time 120 -c 32 -t 1 --hide-histogram --command 'FT.SEARCH ycsb * LIMIT 0 0'"
    ]
    assert remote_client_module.is_ycsb_java(commands) == False
    commands = [
        "/tmp/go-ycsb -s 10.3.0.117 -p 6379 --hide-histogram --json-out-file /tmp/benchmark-result-search-wildcard-nosorting_2022-07-25-14-01-44.out --test-time 120 -c 32 -t 1 --hide-histogram --command 'FT.SEARCH ycsb * LIMIT 0 0'"
    ]
    assert remote_client_module.is_ycsb_java(commands) == False
    commands = [
        "/tmp/abacac/java/etc/adad/a/a/ycsb -s 10.3.0.117 -p 6379 --hide-histogram --json-out-file /tmp/benchmark-result-search-wildcard-nosorting_2022-07-25-14-01-44.out --test-time 120 -c 32 -t 1 --hide-histogram --command 'FT.SEARCH ycsb * LIMIT 0 0'"
    ]
    assert remote_client_module.is_ycsb_java(commands) == True


def test_remote_post_processing_persists_cpu_summary(tmp_path):
    results_file = tmp_path / "benchmark-result.json"
    results_file.write_text('{"ALL STATS": {"Totals": {"Ops/sec": 12.0}}}')
    cpu_summary = {
        "total_shards_used_cpu_pct": 50.0,
        "shards_count": 2,
        "shards_cpu_usage_min_pct": 10.0,
        "shards_cpu_usage_max_pct": 40.0,
        "shards_cpu_usage_avg_pct": 25.0,
        "shards_cpu_usage_median_pct": 25.0,
    }

    _, _, results_dict, return_code = post_process_remote_run(
        "81201",
        {},
        "memtier_benchmark",
        str(results_file),
        0,
        1756200000000,
        "2026-08-26-00-00-00",
        "",
        None,
        extra_results=cpu_summary,
    )

    assert return_code == 0
    assert results_dict["Measurements"] == cpu_summary
    assert "shards_cpu_usage_median_pct" in results_file.read_text()
