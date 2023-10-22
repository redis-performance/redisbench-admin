#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import logging
import tarfile

from redisbench_admin.run_async.async_terraform import (
    retrieve_inventory_info,
    terraform_spin_or_reuse_env,
)


def tar_files(filename="archive.tar", path="./../../"):
    logging.info("Creating tar archive: {} for path {}".format(filename, path))
    with tarfile.open(filename, "w") as tar:
        tar.add(path, arcname=".")
    return filename


def async_env_setup(
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
        logging.info("Using the following connection addresses.")
        logging.info("client_public_ip={}".format(client_public_ip))
        logging.info("server_public_ip={}".format(server_public_ip))
        logging.info("server_private_ip={}".format(server_private_ip))
    else:
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
    return (
        client_public_ip,
        server_plaintext_port,
        server_private_ip,
        server_public_ip,
        db_ssh_port,
        client_ssh_port,
        username,
    )
