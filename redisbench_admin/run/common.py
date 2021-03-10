import logging
import re

from redisbench_admin.run.redis_benchmark.redis_benchmark import prepareRedisBenchmarkCommand
from redisbench_admin.run.redisgraph_benchmark_go.redisgraph_benchmark_go import prepareRedisGraphBenchmarkGoCommand
from redisbench_admin.utils.remote import executeRemoteCommands, getFileFromRemoteSetup


def extract_benchmark_tool_settings(benchmark_config):
    benchmark_tool = None
    benchmark_min_tool_version = None
    benchmark_min_tool_version_major = None
    benchmark_min_tool_version_minor = None
    benchmark_min_tool_version_patch = None

    for entry in benchmark_config["clientconfig"]:
        if 'tool' in entry:
            benchmark_tool = entry['tool']
        if 'min-tool-version' in entry:
            benchmark_min_tool_version = entry['min-tool-version']
            p = re.compile("(\d+)\.(\d+)\.(\d+)")
            m = p.match(benchmark_min_tool_version)
            if m is None:
                logging.error(
                    "Unable to extract semversion from 'min-tool-version'. Will not enforce version")
                benchmark_min_tool_version = None
            else:
                benchmark_min_tool_version_major = m.group(1)
                benchmark_min_tool_version_minor = m.group(2)
                benchmark_min_tool_version_patch = m.group(3)
    return benchmark_min_tool_version, benchmark_min_tool_version_major, benchmark_min_tool_version_minor, benchmark_min_tool_version_patch, benchmark_tool


def prepare_benchmark_parameters(benchmark_config, benchmark_tool, server_plaintext_port, server_private_ip,
                                 remote_results_file, isremote=False):
    for entry in benchmark_config["clientconfig"]:
        if 'parameters' in entry:
            if benchmark_tool == 'redis-benchmark':
                command_arr, command_str = prepareRedisBenchmarkCommand(
                    "redis-benchmark",
                    server_private_ip,
                    server_plaintext_port,
                    entry
                )
                redirect_file = ">{}".format(remote_results_file)
                command_arr.append(redirect_file)
                command_str = command_str + " " + redirect_file

            if benchmark_tool == 'redisgraph-benchmark-go':
                command_arr = prepareRedisGraphBenchmarkGoCommand(
                    "/tmp/redisgraph-benchmark-go",
                    server_private_ip,
                    server_plaintext_port,
                    entry,
                    remote_results_file,
                )
                command_str = " ".join(command_arr)
    return command_arr, command_str


def runRemoteBenchmark(
        client_public_ip,
        username,
        private_key,
        remote_results_file,
        local_results_file,
        command
):
    remote_run_result = False
    res = executeRemoteCommands(client_public_ip, username, private_key, [command])
    recv_exit_status, stdout, stderr = res[0]

    if recv_exit_status != 0:
        logging.error("Exit status of remote command execution {}. Printing stdout and stderr".format(recv_exit_status))
        logging.error("remote process stdout: ".format(stdout))
        logging.error("remote process stderr: ".format(stderr))
    else:
        logging.info("Remote process exited normally. Exit code {}. Printing stdout.".format(recv_exit_status))
        logging.info("remote process stdout: ".format(stdout))
        logging.info("Extracting the benchmark results")
        remote_run_result = True
        getFileFromRemoteSetup(
            client_public_ip,
            username,
            private_key,
            local_results_file,
            remote_results_file,
        )
    return remote_run_result
