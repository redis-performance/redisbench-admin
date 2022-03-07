import pkg_resources

ANN_MULTIRUN_PATH = pkg_resources.resource_filename(
    "redisbench_admin", "run/ann/pkg/multirun.py"
)


def prepare_ann_benchmark_command(
    server_private_ip: object,
    server_plaintext_port: object,
    cluster_mode: bool,
    benchmark_config: object,
    results_file: str,
    current_workdir: str,
):
    command_arr = ["python3", ANN_MULTIRUN_PATH]

    if "arguments" in benchmark_config:
        command_arr.extend(benchmark_config["arguments"].strip().split(" "))

    if "parameters" in benchmark_config:
        for k, v in benchmark_config["parameters"].items():
            command_arr.extend(["--{}".format(k), str(v)])

    if server_private_ip is not None:
        command_arr.extend(["--host", "{}".format(server_private_ip)])
    if server_plaintext_port is not None:
        command_arr.extend(["--port", str(server_plaintext_port)])
    if cluster_mode:
        command_arr.append("--cluster")

    command_arr.extend(["--json-output", "{}/{}".format(current_workdir, results_file)])

    command_str = " ".join([str(x) for x in command_arr])
    return command_arr, command_str
