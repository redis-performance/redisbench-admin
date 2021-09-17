#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import datetime
import time

from redisbench_admin.run.run import calculate_client_tool_duration_and_check


def test_calculate_client_tool_duration_and_check():
    sleep_time = 1
    benchmark_start_time = datetime.datetime.now()
    time.sleep(sleep_time)
    benchmark_end_time = datetime.datetime.now()
    benchmark_duration_seconds = calculate_client_tool_duration_and_check(
        benchmark_end_time, benchmark_start_time, "benchmark", True
    )
    assert benchmark_duration_seconds >= sleep_time
