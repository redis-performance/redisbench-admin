#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#

from redisbench_admin.utils.remote import (
    execute_remote_commands,
)


def setup_remote_benchmark_tool_redisgraph_benchmark_go(
    client_public_ip, username, private_key, redisbenchmark_go_link, client_ssh_port
):
    commands = [
        "wget {} -q -O /tmp/redisgraph-benchmark-go".format(redisbenchmark_go_link),
        "chmod 755 /tmp/redisgraph-benchmark-go",
    ]
    execute_remote_commands(
        client_public_ip, username, private_key, commands, client_ssh_port
    )


def setup_remote_benchmark_tool_ycsb_redisearch(
    client_public_ip, username, private_key, tool_link, client_ssh_port
):
    commands = [
        "rm -rf /tmp/ycsb*",
        "wget {} -q -O /tmp/ycsb.tar.gz".format(tool_link),
        "tar -xvf /tmp/ycsb.tar.gz -C /tmp",
        "mv /tmp/ycsb-* /tmp/ycsb",
    ]
    execute_remote_commands(
        client_public_ip, username, private_key, commands, client_ssh_port
    )
