#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import logging

from python_terraform import Terraform, IsNotFlagged

from redisbench_admin.run.common import BENCHMARK_REPETITIONS
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
    tf_timeout_secs=7200,
    tf_override_name=None,
    tf_folder_path=None,
):
    (remote_setup, deployment_type, remote_id,) = fetch_remote_setup_from_config(
        benchmark_config["remote"],
        "https://github.com/redis-performance/testing-infrastructure.git",
        "master",
        tf_folder_path,
    )
    logging.info(
        "Repetition {} of {}. Deploying test {} on AWS using {}".format(
            repetition, BENCHMARK_REPETITIONS, test_name, remote_setup
        )
    )
    if tf_override_name is None:
        tf_setup_name = "{}{}".format(remote_setup, tf_setup_name_sufix)
    else:
        tf_setup_name = tf_override_name
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
            tf_timeout_secs,
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
    return (
        client_public_ip,
        deployment_type,
        server_plaintext_port,
        server_private_ip,
        server_public_ip,
        username,
    )


def terraform_destroy(remote_envs, keep_env=False):
    if keep_env is False:
        for remote_setup_name, tf in remote_envs.items():
            # tear-down
            logging.info("Tearing down setup {}".format(remote_setup_name))
            tf.destroy(
                capture_output="yes",
                no_color=IsNotFlagged,
                force=IsNotFlagged,
                auto_approve=True,
            )
        logging.info("Tear-down completed")
    else:
        logging.info("Keeping the environment UP uppon request")


def retrieve_inventory_info(inventory_str):
    inventory_list = inventory_str.split(",")
    client_public_ip = None
    server_private_ip = None
    server_public_ip = None
    for kv_pair in inventory_list:
        splitted = kv_pair.split("=")
        if len(splitted) == 2:
            key = splitted[0]
            value = splitted[1]
            if key == "client_public_ip":
                client_public_ip = value
            if key == "server_private_ip":
                server_private_ip = value
            if key == "server_public_ip":
                server_public_ip = value
    status = True
    if client_public_ip is None or client_public_ip is None or client_public_ip is None:
        status = False
    return status, client_public_ip, server_private_ip, server_public_ip
