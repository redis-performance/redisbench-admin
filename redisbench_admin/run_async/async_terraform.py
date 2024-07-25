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
    extract_git_vars,
    tf_output_or_none,
    retrieve_tf_connection_vars,
    setup_remote_environment,
)


class TerraformClass:
    def __init__(
        self,
        tf_bin_path=None,
        tf_github_actor=None,
        tf_github_org=None,
        tf_github_repo=None,
        tf_github_sha=None,
        tf_setup_name_sufix=None,
        tf_triggering_env=None,
        tf_timeout_secs=7200,
        tf_override_name=None,
        tf_folder_path=None,
        tf_github_branch=None,
    ):
        self.tf_bin_path = tf_bin_path
        self.tf_github_actor = tf_github_actor
        self.tf_github_org = tf_github_org
        self.tf_github_repo = tf_github_repo
        self.tf_github_sha = tf_github_sha
        self.tf_setup_name_sufix = tf_setup_name_sufix
        self.tf_triggering_env = tf_triggering_env
        self.tf_timeout_secs = tf_timeout_secs
        self.tf_override_name = tf_override_name
        self.tf_folder_path = tf_folder_path
        self.tf_github_branch = tf_github_branch
        self.runner_public_ip = None

    def git_vars_crosscheck(self):
        (
            github_org_name,
            github_repo_name,
            github_sha,
            github_actor,
            github_branch,
            github_branch_detached,
        ) = extract_git_vars()
        if self.tf_github_org is None:
            logging.info(
                "Extracting tf_github_org given args.github_org was none. Extracte value {}".format(
                    github_org_name
                )
            )
            self.tf_github_org = github_org_name
        if self.tf_github_actor is None:
            logging.info(
                "Extracting tf_github_actor given args.github_actor was none. Extracte value {}".format(
                    github_actor
                )
            )
            self.tf_github_actor = github_actor
        if self.tf_github_repo is None:
            logging.info(
                "Extracting tf_github_repo given args.github_repo was none. Extracte value {}".format(
                    github_repo_name
                )
            )
            self.tf_github_repo = github_repo_name
        if self.tf_github_sha is None:
            logging.info(
                "Extracting tf_github_sha given args.github_sha was none. Extracte value {}".format(
                    github_sha
                )
            )
            self.tf_github_sha = github_sha
        if self.tf_github_branch is None:
            logging.info(
                "Extracting tf_github_branch given args.github_branch was none. Extracte value {}".format(
                    github_branch
                )
            )
            self.tf_github_branch = github_branch

        if self.tf_github_branch is None or self.tf_github_branch == "":
            logging.error(
                "The github branch information is not present!"
                " This implies that per-branch data is not pushed to the exporters!"
            )
        else:
            if type(self.tf_github_branch) is not str:
                self.tf_github_branch = str(self.tf_github_branch)

    def common_properties_log(self, private_key):
        logging.info("Using the following vars on deployment:")
        logging.info("\tPrivate key path: {}".format(private_key))
        logging.info("\tterraform bin path: {}".format(self.tf_bin_path))
        logging.info("\tgithub_actor: {}".format(self.tf_github_actor))
        logging.info("\tgithub_org: {}".format(self.tf_github_org))
        logging.info("\tgithub_repo: {}".format(self.tf_github_repo))
        logging.info("\tgithub_branch: {}".format(self.tf_github_branch))
        logging.info("\tgithub_sha: {}".format(self.tf_github_sha))
        logging.info("\ttriggering env: {}".format(self.tf_triggering_env))
        logging.info("\tsetup_name sufix: {}".format(self.tf_setup_name_sufix))

    def async_runner_setup(
        self,
    ):
        (remote_setup, deployment_type, remote_id,) = fetch_remote_setup_from_config(
            [{"type": "async", "setup": "runner"}],
            "https://github.com/RedisLabsModules/testing-infrastructure.git",
            "master",
            self.tf_folder_path,
        )
        logging.info("Deploying runner on AWS using")
        if self.tf_override_name is None:
            tf_setup_name = "{}{}".format(remote_setup, self.tf_setup_name_sufix)
        else:
            tf_setup_name = self.tf_override_name
        logging.info("Using full setup name: {}".format(tf_setup_name))
        tf = Terraform(
            working_dir=remote_setup,
            terraform_bin_path=self.tf_bin_path,
        )
        (
            tf_return_code,
            username,
            server_private_ip,
            server_public_ip,
            server_plaintext_port,
        ) = self.setup_remote_environment(
            tf,
            self.tf_github_sha,
            self.tf_github_actor,
            tf_setup_name,
            self.tf_github_org,
            self.tf_github_repo,
            self.tf_triggering_env,
            self.tf_timeout_secs,
        )
        return (
            server_plaintext_port,
            server_private_ip,
            server_public_ip,
            username,
        )

    def setup_remote_environment(
        self,
        tf: Terraform,
        tf_github_sha,
        tf_github_actor,
        tf_setup_name,
        tf_github_org,
        tf_github_repo,
        tf_triggering_env,
        tf_timeout_secs=7200,
    ):
        _, _, _ = tf.init(
            capture_output=True,
            backend_config={
                "key": "benchmarks/infrastructure/{}.tfstate".format(tf_setup_name)
            },
        )
        _, _, _ = tf.refresh()
        tf_output = tf.output()
        server_private_ip = tf_output_or_none(tf_output, "runner_private_ip")
        server_public_ip = tf_output_or_none(tf_output, "runner_public_ip")
        if server_private_ip is not None or server_public_ip is not None:
            logging.warning("Destroying previous setup")
            tf.destroy()
        return_code, stdout, stderr = tf.apply(
            skip_plan=True,
            capture_output=False,
            refresh=True,
            var={
                "github_sha": tf_github_sha,
                "github_actor": tf_github_actor,
                "setup_name": tf_setup_name,
                "github_org": tf_github_org,
                "github_repo": tf_github_repo,
                "triggering_env": tf_triggering_env,
                "timeout_secs": tf_timeout_secs,
                "Project": tf_github_org,
                "project": tf_github_org,
                "Environment": tf_github_org,
                "environment": tf_github_org,
            },
        )
        return self.retrieve_tf_connection_vars(return_code, tf)

    def retrieve_tf_connection_vars(self, return_code, tf):
        tf_output = tf.output()
        server_private_ip = tf_output["runner_private_ip"]["value"][0]
        server_public_ip = tf_output["runner_public_ip"]["value"][0]
        self.runner_public_ip = server_public_ip
        server_plaintext_port = 6379
        username = "ubuntu"
        return (
            return_code,
            username,
            server_private_ip,
            server_public_ip,
            server_plaintext_port,
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
        "https://github.com/RedisLabsModules/testing-infrastructure.git",
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
