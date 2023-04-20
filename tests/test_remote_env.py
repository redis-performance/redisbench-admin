#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import argparse

from redisbench_admin.run_remote.args import create_run_remote_arguments
from redisbench_admin.run_remote.remote_env import remote_env_setup


def test_remote_env_setup():
    parser = argparse.ArgumentParser(
        description="test",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser = create_run_remote_arguments(parser)
    args = parser.parse_args(
        args=[
            "--inventory",
            "server_private_ip=10.0.0.1,server_public_ip=1.1.1.1,client_public_ip=2.2.2.2",
        ]
    )
    remote_envs = {}
    repetition = 1
    tf_bin_path = args.terraform_bin_path
    tf_github_org = args.github_org
    tf_github_actor = args.github_actor
    tf_github_repo = args.github_repo
    tf_github_sha = args.github_sha
    tf_github_branch = args.github_branch
    test_name = "test1"
    tf_triggering_env = "ci"
    tf_setup_name_sufix = "suffix"
    benchmark_config = {}
    (
        n_db_hosts,
        n_client_hosts,
        client_public_ip,
        server_plaintext_port,
        server_private_ip,
        server_public_ip,
        db_ssh_port,
        client_ssh_port,
        username,
    ) = remote_env_setup(
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
    )

    assert client_public_ip == "2.2.2.2"
    assert server_private_ip == "10.0.0.1"
    assert server_public_ip == "1.1.1.1"
    assert n_client_hosts == 1
    assert n_db_hosts == 1

    # using inventory but missing one manadatory key
    args = parser.parse_args(
        args=[
            "--inventory",
            "server_public_ip=1.1.1.1,client_public_ip=2.2.2.2",
        ]
    )
    remote_env_setup(
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
    )
