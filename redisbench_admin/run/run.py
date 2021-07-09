#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import logging

from cpuinfo import cpuinfo

from redisbench_admin.run_remote.consts import min_recommended_benchmark_duration


def run_command_logic(args):
    dict(args.__dict__)
    # local_path = os.path.abspath(args.local_dir)
    benchmark_machine_info = cpuinfo.get_cpu_info()
    benchmark_infra = {
        "total-benchmark-machines": 0,
        "benchmark-machines": {},
        "total-db-machines": 0,
        "db-machines": {},
    }
    benchmark_machine_1 = {"machine_info": benchmark_machine_info}
    benchmark_infra["benchmark-machines"]["benchmark-machine-1"] = benchmark_machine_1
    benchmark_infra["total-benchmark-machines"] += 1


def calculate_benchmark_duration_and_check(benchmark_end_time, benchmark_start_time):
    benchmark_duration_seconds = (benchmark_end_time - benchmark_start_time).seconds
    logging.info("Benchmark duration {} secs.".format(benchmark_duration_seconds))
    if benchmark_duration_seconds < min_recommended_benchmark_duration:
        logging.warning(
            "Benchmark duration of {} secs is bellow the considered"
            " minimum duration for a stable run ({} secs).".format(
                benchmark_duration_seconds,
                min_recommended_benchmark_duration,
            )
        )
    return benchmark_duration_seconds
