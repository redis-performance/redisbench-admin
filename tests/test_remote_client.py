#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import yaml

from redisbench_admin.run.common import prepare_benchmark_parameters
from redisbench_admin.run_remote.remote_client import ann_benchmark_remote_cmds
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
    ) = ann_benchmark_remote_cmds(local_bench_fname, pkg_path)
    assert (
        create_website_command
        == "cd {}/run/ann/pkg/ && sudo python3 create_website.py --scatter --outputdir /tmp/website-local-fnam".format(
            pkg_path
        )
    )
