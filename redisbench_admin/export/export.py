import os

from redisbench_admin.utils.utils import retrieve_local_or_remote_input_json


def export_command_logic(args):
    benchmark_files = args.benchmark_result_files
    local_path = os.path.abspath(args.local_dir)
    included_steps = args.steps.split(",")
    benchmark_results = retrieve_local_or_remote_input_json(benchmark_files, local_path, "--benchmark-result-files")
