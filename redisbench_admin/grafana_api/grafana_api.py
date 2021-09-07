#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#


import logging
import os

import redis

from redisbench_admin.grafana_api.app import create_app


LOG_LEVEL = logging.DEBUG
if os.getenv("VERBOSE", "0") == "0":
    LOG_LEVEL = logging.INFO
LOG_FORMAT = "%(asctime)s %(levelname)-4s %(message)s"
LOG_DATEFMT = "%Y-%m-%d %H:%M:%S"


def grafana_api_command_logic(args, project_name, project_version):
    logging.info(
        "Using: {project_name} {project_version}".format(
            project_name=project_name, project_version=project_version
        )
    )

    conn = redis.StrictRedis(
        host=args.redis_host,
        port=args.redis_port,
        decode_responses=True,
        password=args.redis_pass,
        health_check_interval=args.redis_health_check_interval,
        socket_connect_timeout=args.redis_socket_connect_timeout,
        socket_keepalive=True,
    )
    app = create_app(conn, args.auth_server_host, args.auth_server_port)
    if args.logname is not None:
        print("Writting log to {}".format(args.logname))
        handler = logging.handlers.RotatingFileHandler(
            args.logname, maxBytes=1024 * 1024
        )
        logging.getLogger("werkzeug").setLevel(logging.DEBUG)
        logging.getLogger("werkzeug").addHandler(handler)
        app.logger.setLevel(LOG_LEVEL)
        app.logger.addHandler(handler)
    else:
        # logging settings
        logging.basicConfig(
            format=LOG_FORMAT,
            level=LOG_LEVEL,
            datefmt=LOG_DATEFMT,
        )
    app.run(host="0.0.0.0", port=args.port)
