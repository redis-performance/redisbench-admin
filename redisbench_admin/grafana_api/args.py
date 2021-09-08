#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
from redisbench_admin.utils.remote import (
    PERFORMANCE_RTS_HOST,
    PERFORMANCE_RTS_PORT,
    PERFORMANCE_RTS_AUTH,
    PERFORMANCE_RTS_USER,
    REDIS_SOCKET_TIMEOUT,
    REDIS_HEALTH_CHECK_INTERVAL,
    REDIS_AUTH_SERVER_HOST,
    REDIS_AUTH_SERVER_PORT,
)


def create_grafana_api_arguments(parser):
    parser.add_argument("--port", type=str, default=5000)
    parser.add_argument("--redis_host", type=str, default=PERFORMANCE_RTS_HOST)
    parser.add_argument("--redis_port", type=int, default=PERFORMANCE_RTS_PORT)
    parser.add_argument("--redis_pass", type=str, default=PERFORMANCE_RTS_AUTH)
    parser.add_argument("--redis_user", type=str, default=PERFORMANCE_RTS_USER)
    parser.add_argument(
        "--redis_health_check_interval", type=int, default=REDIS_HEALTH_CHECK_INTERVAL
    )
    parser.add_argument(
        "--redis_socket_connect_timeout", type=int, default=REDIS_SOCKET_TIMEOUT
    )
    parser.add_argument("--auth_server_host", type=str, default=REDIS_AUTH_SERVER_HOST)
    parser.add_argument("--auth_server_port", type=int, default=REDIS_AUTH_SERVER_PORT)
    return parser
