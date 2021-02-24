import logging
import re
import subprocess

from cpuinfo import cpuinfo


def run_command_logic(args):
    use_case_specific_arguments = dict(args.__dict__)
    # local_path = os.path.abspath(args.local_dir)
    is_remote_mode = args.remote
    tool = args.tool
    benchmark_machine_info = cpuinfo.get_cpu_info()
    total_cores = benchmark_machine_info["count"]
    benchmark_infra = {
        "total-benchmark-machines": 0,
        "benchmark-machines": {},
        "total-db-machines": 0,
        "db-machines": {},
    }
    benchmark_machine_1 = {"machine_info": benchmark_machine_info}
    benchmark_infra["benchmark-machines"]["benchmark-machine-1"] = benchmark_machine_1
    benchmark_infra["total-benchmark-machines"] += 1


def redis_benchmark_ensure_min_version(benchmark_tool, benchmark_min_tool_version, benchmark_min_tool_version_major,
                                       benchmark_min_tool_version_minor, benchmark_min_tool_version_patch):
    benchmark_client_process = subprocess.Popen(args=[benchmark_tool, "--version"],
                                                stdout=subprocess.PIPE,
                                                stderr=subprocess.STDOUT)
    (stdout, sterr) = benchmark_client_process.communicate()
    version_output = stdout.decode('ascii').split("\n")[0]
    logging.info(
        "Detected benchmark config tool {} with version {}".format(benchmark_tool, version_output))
    p = re.compile("redis-benchmark (\d+)\.(\d+)\.(\d+) ")
    m = p.match(version_output)
    if m is None:
        raise Exception(
            "Unable to detect benchmark tool version, and the benchmark requires a min version: {}".format(
                benchmark_min_tool_version))
    major = m.group(1)
    minor = m.group(2)
    patch = m.group(3)
    if major < benchmark_min_tool_version_major or (
            major == benchmark_min_tool_version_major and minor < benchmark_min_tool_version_minor) or (
            major == benchmark_min_tool_version_major and minor == benchmark_min_tool_version_minor and patch < benchmark_min_tool_version_patch):
        raise Exception(
            "Detected benchmark version that is inferior than the minimum required. {} < {}".format(
                version_output, benchmark_min_tool_version))
