#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import logging
import os

import paramiko
import redis
from sshtunnel import SSHTunnelForwarder

from redisbench_admin.run_remote.consts import private_key
from redisbench_admin.utils.remote import check_and_fix_pem_str


def ssh_tunnel_redisconn(
    server_plaintext_port,
    server_private_ip,
    server_public_ip,
    username,
    ssh_port=22,
):
    ssh_pkey = paramiko.RSAKey.from_private_key_file(private_key)

    # Use sshtunnelforwarder tunnel to connect redis via springboard
    ssh_tunel = SSHTunnelForwarder(
        ssh_address_or_host=(server_public_ip, ssh_port),
        ssh_username=username,
        ssh_pkey=ssh_pkey,
        logger=logging.getLogger(),
        remote_bind_address=(
            server_private_ip,
            server_plaintext_port,
        ),  # remote redis server
        # Bind the socket to port 0. A random free port from 1024 to 65535 will be selected.
        local_bind_address=("0.0.0.0", 0),  # enable local forwarding port
    )
    ssh_tunel.start()  # start tunnel
    r = redis.StrictRedis(host="localhost", port=ssh_tunel.local_bind_port)
    return r, ssh_tunel


def ssh_pem_check(EC2_PRIVATE_PEM):
    if EC2_PRIVATE_PEM is None or EC2_PRIVATE_PEM == "":
        logging.error("missing required EC2_PRIVATE_PEM env variable")
        exit(1)
    with open(private_key, "w") as tmp_private_key_file:
        pem_str = check_and_fix_pem_str(EC2_PRIVATE_PEM)
        tmp_private_key_file.write(pem_str)
    if os.path.exists(private_key) is False:
        logging.error(
            "Specified private key path does not exist: {}".format(private_key)
        )
        exit(1)
    else:
        logging.info(
            "Confirmed that private key path artifact: '{}' exists!".format(private_key)
        )
