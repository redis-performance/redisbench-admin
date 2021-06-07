#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#

import os

from redisbench_admin.profilers.profilers import (
    PROFILERS_DEFAULT,
    ALLOWED_PROFILERS,
    PROFILE_FREQ_DEFAULT,
)

PROFILERS_ENABLED = os.getenv("PROFILE", 0)
PROFILERS = os.getenv("PROFILERS", PROFILERS_DEFAULT)
PROFILE_FREQ = os.getenv("PROFILE_FREQ", PROFILE_FREQ_DEFAULT)


def create_run_local_arguments(parser):
    parser.add_argument("--module_path", type=str, required=True)
    parser.add_argument(
        "--allowed-tools",
        type=str,
        default="redis-benchmark,redisgraph-benchmark-go,ycsb,tsbs_run_queries_redistimeseries,tsbs_load_redistimeseries",
        help="comma separated list of allowed tools for this module. By default all the supported are allowed.",
    )
    parser.add_argument(
        "--test",
        type=str,
        default="",
        help="specify a test to run. By default will run all of them.",
    )
    parser.add_argument(
        "--required-module",
        default=None,
        action="append",
        help="path to the module file. "
        "You can use `--required-module` more than once",
    )
    parser.add_argument("--profilers", type=str, default=PROFILERS)
    parser.add_argument(
        "--enable-profilers",
        default=PROFILERS_ENABLED,
        action="store_true",
        help="Enable Identifying On-CPU and Off-CPU Time using perf/ebpf/vtune tooling. "
        + "By default the chosen profilers are {}".format(PROFILERS_DEFAULT)
        + "Full list of profilers: {}".format(ALLOWED_PROFILERS)
        + "Only available on x86 Linux platform and kernel version >= 4.9",
    )
    parser.add_argument("--port", type=int, default=6379)
    return parser
