#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import os

from redisbench_admin.run.args import common_run_args
from redisbench_admin.run.common import REDIS_BINARY

FLUSHALL_AT_START = bool(int(os.getenv("FLUSHALL_AT_START", "0")))
IGNORE_KEYSPACE_ERRORS = bool(int(os.getenv("IGNORE_KEYSPACE_ERRORS", "0")))


def create_run_local_arguments(parser):
    parser = common_run_args(parser)
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--redis-binary", type=str, default=REDIS_BINARY)
    parser.add_argument(
        "--flushall_on_every_test_start",
        type=bool,
        default=FLUSHALL_AT_START,
        help="At the start of every test send a FLUSHALL",
    )
    parser.add_argument(
        "--ignore_keyspace_errors",
        type=bool,
        default=IGNORE_KEYSPACE_ERRORS,
        help="Ignore keyspace check errors. Will still log them as errors",
    )
    return parser
