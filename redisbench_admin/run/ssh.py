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

from redisbench_admin.utils.remote import check_and_fix_pem_str, connect_remote_ssh


def ssh_tunnel_redisconn(
    server_plaintext_port,
    server_private_ip,
    server_public_ip,
    username,
    ssh_port,
    private_key,
):
    ssh_pkey = paramiko.RSAKey.from_private_key_file(private_key)
    logging.info("Checking we're able to connect to remote host")
    connection = connect_remote_ssh(ssh_port, private_key, server_public_ip, username)
    if check_connection(connection):
        logging.info("All good!")
    else:
        exit(1)

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
    redis_conn = redis.StrictRedis(host="localhost", port=ssh_tunel.local_bind_port)
    redis_conn.ping()
    return redis_conn, ssh_tunel


def check_connection(ssh_conn):
    """
    This will check if the connection is still available.

    Return (bool) : True if it's still alive, False otherwise.
    """
    try:
        ssh_conn.exec_command("ls", timeout=5)
        return True
    except Exception as e:
        logging.error(
            "unable to execute a simple command on remote connection. Error: {}".format(
                e.__str__()
            )
        )
        return False


def ssh_pem_check(EC2_PRIVATE_PEM, private_key):
    if EC2_PRIVATE_PEM is not None and EC2_PRIVATE_PEM != "":
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
