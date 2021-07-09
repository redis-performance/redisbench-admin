#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import logging

from python_terraform import Terraform

from redisbench_admin.run.common import BENCHMARK_REPETITIONS
from redisbench_admin.run.ssh import ssh_tunnel_redisconn
from redisbench_admin.utils.remote import (
    fetch_remote_setup_from_config,
    setup_remote_environment,
    retrieve_tf_connection_vars,
)


def terraform_spin_or_reuse_env(
    benchmark_config,
    remote_envs,
    repetition,
    test_name,
    tf_bin_path,
    tf_github_actor,
    tf_github_org,
    tf_github_repo,
    tf_github_sha,
    tf_setup_name_sufix,
    tf_triggering_env,
):
    (
        remote_setup,
        deployment_type,
        remote_id,
    ) = fetch_remote_setup_from_config(benchmark_config["remote"])
    logging.info(
        "Repetition {} of {}. Deploying test {} on AWS using {}".format(
            repetition, BENCHMARK_REPETITIONS, test_name, remote_setup
        )
    )
    tf_setup_name = "{}{}".format(remote_setup, tf_setup_name_sufix)
    logging.info("Using full setup name: {}".format(tf_setup_name))
    if remote_id not in remote_envs:
        # check if terraform is present
        tf = Terraform(
            working_dir=remote_setup,
            terraform_bin_path=tf_bin_path,
        )
        (
            tf_return_code,
            username,
            server_private_ip,
            server_public_ip,
            server_plaintext_port,
            client_private_ip,
            client_public_ip,
        ) = setup_remote_environment(
            tf,
            tf_github_sha,
            tf_github_actor,
            tf_setup_name,
            tf_github_org,
            tf_github_repo,
            tf_triggering_env,
        )
        remote_envs[remote_id] = tf
    else:
        logging.info("Reusing remote setup {}".format(remote_id))
        tf = remote_envs[remote_id]
        (
            tf_return_code,
            username,
            server_private_ip,
            server_public_ip,
            server_plaintext_port,
            client_private_ip,
            client_public_ip,
        ) = retrieve_tf_connection_vars(None, tf)

        local_redis_conn, ssh_tunnel = ssh_tunnel_redisconn(
            server_plaintext_port,
            server_private_ip,
            server_public_ip,
            username,
        )
        local_redis_conn.shutdown()
        ssh_tunnel.close()  # Close the tunnel
    return (
        client_public_ip,
        deployment_type,
        server_plaintext_port,
        server_private_ip,
        server_public_ip,
        username,
    )


def terraform_destroy(remote_envs):
    for remote_setup_name, tf in remote_envs.items():
        # tear-down
        logging.info("Tearing down setup {}".format(remote_setup_name))
        tf.destroy()
        logging.info("Tear-down completed")
