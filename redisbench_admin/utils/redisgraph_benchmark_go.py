#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#

from redisbench_admin.utils.remote import (
    execute_remote_commands,
)


def setup_remote_benchmark_tool_redisgraph_benchmark_go(
    client_public_ip, username, private_key, redisbenchmark_go_link
):
    commands = [
        "wget {} -q -O /tmp/redisgraph-benchmark-go".format(redisbenchmark_go_link),
        "chmod 755 /tmp/redisgraph-benchmark-go",
    ]
    execute_remote_commands(client_public_ip, username, private_key, commands)


def setup_remote_benchmark_tool_ycsb_redisearch(
    client_public_ip,
    username,
    private_key,
    tool_link="https://s3.amazonaws.com/benchmarks.redislabs/redisearch/ycsb/ycsb-redisearch-binding-0.18.0-SNAPSHOT.tar.gz",
):
    commands = [
        "wget {} -q -O /tmp/ycsb.tar.gz".format(tool_link),
        "tar -xvf /tmp/ycsb.tar.gz -C /tmp",
    ]
    execute_remote_commands(client_public_ip, username, private_key, commands)
