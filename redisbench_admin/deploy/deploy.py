#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import json
import logging
import os

from redisbench_admin.run.git import git_vars_crosscheck
from redisbench_admin.run.ssh import ssh_pem_check
from redisbench_admin.utils.remote import (
    fetch_remote_setup_git_url,
    setup_remote_environment,
    check_ec2_env,
)
from python_terraform import Terraform

from redisbench_admin.utils.utils import EC2_PRIVATE_PEM


def deploy_command_logic(args, project_name, project_version):
    logging.info(
        "Using: {project_name} {project_version}".format(
            project_name=project_name, project_version=project_version
        )
    )
    terraform_bin_path = args.terraform_bin_path
    tf_github_org = args.github_org
    tf_github_actor = args.github_actor
    tf_github_repo = args.github_repo
    tf_github_sha = args.github_sha
    tf_github_branch = args.github_branch
    infra_timeout_secs = args.infra_timeout_secs

    (
        tf_github_actor,
        tf_github_branch,
        tf_github_org,
        tf_github_repo,
        tf_github_sha,
    ) = git_vars_crosscheck(
        tf_github_actor, tf_github_branch, tf_github_org, tf_github_repo, tf_github_sha
    )

    private_key = args.private_key
    ssh_pem_check(EC2_PRIVATE_PEM, private_key)

    if args.skip_env_vars_verify is False:
        env_check_status, failure_reason = check_ec2_env()
        if env_check_status is False:
            logging.critical("{}. Exiting right away!".format(failure_reason))
            exit(1)

    inventory_git = args.inventory_git
    inventory_local_dir = args.inventory_local_dir
    destroy = args.destroy
    if inventory_local_dir is not None:
        if os.path.isdir(inventory_local_dir) is False:
            os.mkdir(inventory_local_dir)
    (
        remote_setup,
        deployment_type,
        remote_id,
    ) = fetch_remote_setup_git_url(inventory_git, inventory_local_dir, destroy)
    tf = Terraform(
        working_dir=remote_setup,
        terraform_bin_path=terraform_bin_path,
    )
    tf_setup_name_sufix = "{}-{}".format(args.setup_name_sufix, tf_github_sha)
    tf_setup_name = "{}{}".format(remote_setup, tf_setup_name_sufix)
    terraform_backend_key = "benchmarks/infrastructure/{}.tfstate".format(tf_setup_name)
    tf_triggering_env = "redisbench-admin-deploy"
    logging.info("Setting an infra timeout of {} secs".format(infra_timeout_secs))
    if args.destroy is False:
        (
            tf_return_code,
            _,
            _,
            _,
            _,
            _,
            _,
        ) = setup_remote_environment(
            tf,
            tf_github_sha,
            tf_github_actor,
            tf_setup_name,
            tf_github_org,
            tf_github_repo,
            tf_triggering_env,
            infra_timeout_secs,
        )
        logging.info("Deploy terraform return code {}".format(tf_return_code))
        env_json = tf.output()
        output_dict = {}
        for k, v in env_json.items():
            k = k.upper()
            if type(v["value"]) == list:
                output_dict[k] = v["value"][0]
            else:
                output_dict[k] = v["value"]

        logging.info("JSON env variables {}".format(output_dict))
        if args.set_env_vars_json != "":
            with open(args.set_env_vars_json, "w") as json_fd:
                json.dump(output_dict, json_fd)
    else:
        _, _, _ = tf.init(
            capture_output=True,
            backend_config={"key": terraform_backend_key},
        )
        logging.info("Refreshing remote state")
        _, _, _ = tf.refresh()
        logging.info("Triggering destroy")
        output = tf.destroy(capture_output=True)
        logging.info("Finished destroying the remote env. Output:")
        for line in output[1].split("\n"):
            print(line)
