#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
from redisbench_admin.utils.remote import (
    PERFORMANCE_RTS_HOST,
    PERFORMANCE_RTS_PORT,
    PERFORMANCE_RTS_AUTH,
)


def create_watchdog_arguments(parser):
    parser.add_argument(
        "--steps",
        type=str,
        default="dangling,count-active",
        help="comma separated list of steps to be run",
    )
    parser.add_argument(
        "--exporter",
        type=str,
        default="redistimeseries",
        help="exporter to be used ( either csv or redistimeseries )",
    )
    parser.add_argument(
        "--update-interval",
        type=int,
        default=60,
        help="watchdog update interval in seconds",
    )
    parser.add_argument(
        "--redistimeseries_host", type=str, default=PERFORMANCE_RTS_HOST
    )
    parser.add_argument(
        "--redistimeseries_port", type=int, default=PERFORMANCE_RTS_PORT
    )
    parser.add_argument(
        "--redistimeseries_pass", type=str, default=PERFORMANCE_RTS_AUTH
    )
    return parser
