#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import logging

from python_terraform import TerraformCommandError

from redisbench_admin.run_remote.terraform import (
    retrieve_inventory_info,
    terraform_spin_or_reuse_env,
)
from redisbench_admin.utils.remote import check_remote_setup_spot_instance


def remote_env_setup(
    args,
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
    spot_instance_error=False,
    spot_price_counter=0,
    full_price_counter=0,
):
    server_plaintext_port = args.db_port
    db_ssh_port = args.db_ssh_port
    client_ssh_port = args.client_ssh_port
    username = args.user
    if args.inventory is not None:
        (
            status,
            client_public_ip,
            server_private_ip,
            server_public_ip,
        ) = retrieve_inventory_info(args.inventory)
        if status is False:
            logging.error(
                "Missing one of the required keys for inventory usage. Exiting..."
            )
            exit(1)

    else:
        contains_spot, tf_folder_spot_path = check_remote_setup_spot_instance(
            benchmark_config["remote"]
        )
        spot_available_and_used = False
        if contains_spot:
            logging.info(f"detected spot instance config in {tf_folder_spot_path}.")
            if spot_instance_error is False:
                logging.info(
                    f"Will deploy the detected spot instance config in {tf_folder_spot_path}."
                )
                try:
                    (
                        client_public_ip,
                        _,
                        _,
                        server_private_ip,
                        server_public_ip,
                        username,
                    ) = terraform_spin_or_reuse_env(
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
                        tf_timeout_secs,
                        tf_override_name,
                        tf_folder_spot_path,
                    )
                    spot_available_and_used = True
                    spot_price_counter = spot_price_counter + 1
                except TerraformCommandError as error:
                    spot_instance_error = True
                    logging.error(
                        "Received the following error while trying to deploy the spot instance setup: {}.".format(
                            error.__str__()
                        )
                    )
                    pass
            else:
                logging.warning(
                    "Even though there is a spot instance config, avoiding deploying it."
                )
        if spot_available_and_used is False:
            (
                client_public_ip,
                _,
                _,
                server_private_ip,
                server_public_ip,
                username,
            ) = terraform_spin_or_reuse_env(
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
                tf_timeout_secs,
                tf_override_name,
                tf_folder_path,
            )
            full_price_counter = full_price_counter + 1
    logging.info("Using the following connection addresses.")
    logging.info(f"client_public_ip={client_public_ip}")
    logging.info(f"server_public_ip={server_public_ip}")
    logging.info(f"server_private_ip={server_private_ip}")
    return (
        client_public_ip,
        server_plaintext_port,
        server_private_ip,
        server_public_ip,
        db_ssh_port,
        client_ssh_port,
        username,
        spot_instance_error,
        spot_price_counter,
        full_price_counter,
    )
