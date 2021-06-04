#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#

import os

PROFILERS_ENABLED = os.getenv("PROFILE", 0)
PROFILERS = os.getenv("PROFILERS", "ebpf-oncpu,ebpf-offcpu")


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
        help="Enable Identifying On- and Off-CPU Time using ebpf tooling. "
        "Only available on x86 Linux platform and kernel version >= 4.9",
    )
    parser.add_argument("--port", type=int, default=6379)
    return parser
