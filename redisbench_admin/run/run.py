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
