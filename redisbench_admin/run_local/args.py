#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import os

from redisbench_admin.run.args import common_run_args
from redisbench_admin.run.common import REDIS_BINARY

FLUSHALL_AT_START = bool(int(os.getenv("FLUSHALL_AT_START", "1")))
IGNORE_KEYSPACE_ERRORS = bool(int(os.getenv("IGNORE_KEYSPACE_ERRORS", "0")))
SKIP_REDIS_SPIN = bool(int(os.getenv("SKIP_REDIS_SPIN", "0")))
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")


def create_run_local_arguments(parser):
    parser = common_run_args(parser)
    parser.add_argument("--port", type=int, default=REDIS_PORT)
    parser.add_argument("--host", type=str, default=REDIS_HOST)
    parser.add_argument("--redis-binary", type=str, default=REDIS_BINARY)
    parser.add_argument(
        "--flushall_on_every_test_start",
        type=bool,
        default=FLUSHALL_AT_START,
        help="At the start of every test send a FLUSHALL",
    )
    parser.add_argument(
        "--skip-redis-spin",
        type=bool,
        default=SKIP_REDIS_SPIN,
        help="Skip redis spin. consider redis alive at host:port",
    )
    parser.add_argument(
        "--ignore_keyspace_errors",
        type=bool,
        default=IGNORE_KEYSPACE_ERRORS,
        help="Ignore keyspace check errors. Will still log them as errors",
    )
    return parser
