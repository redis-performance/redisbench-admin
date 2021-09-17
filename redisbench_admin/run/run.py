#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import logging

from redisbench_admin.run_remote.consts import min_recommended_benchmark_duration


def calculate_client_tool_duration_and_check(
    benchmark_end_time, benchmark_start_time, step_name="Benchmark", warn_min=True
):
    benchmark_duration_seconds = (benchmark_end_time - benchmark_start_time).seconds
    logging.info("{} duration {} secs.".format(step_name, benchmark_duration_seconds))
    if benchmark_duration_seconds < min_recommended_benchmark_duration and warn_min:
        logging.warning(
            "{} duration of {} secs is bellow the considered"
            " minimum duration for a stable run ({} secs).".format(
                step_name,
                benchmark_duration_seconds,
                min_recommended_benchmark_duration,
            )
        )
    return benchmark_duration_seconds
