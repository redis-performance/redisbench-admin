#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#

from redisbench_admin.run.args import common_run_args
from redisbench_admin.run.common import REDIS_BINARY


def create_run_local_arguments(parser):
    parser = common_run_args(parser)
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--redis-binary", type=str, default=REDIS_BINARY)
    return parser
