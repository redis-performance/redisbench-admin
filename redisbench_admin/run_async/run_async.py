#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import logging
import os
import random
import string
from shutil import copy

from redisbench_admin.run.common import (
    CIRCLE_BUILD_URL,
    CIRCLE_JOB,
    WH_TOKEN,
)
from redisbench_admin.run.modules import redis_modules_check
from redisbench_admin.run.run import define_benchmark_plan
from redisbench_admin.run.ssh import ssh_pem_check
from redisbench_admin.run_remote.args import TF_OVERRIDE_NAME, TF_OVERRIDE_REMOTE
from redisbench_admin.run_async.async_env import tar_files
from redisbench_admin.run_async.benchmark import BenchmarkClass
from redisbench_admin.run_remote.notifications import generate_failure_notification
from redisbench_admin.run_async.render_files import (
    renderServiceFile,
    renderRunFile,
    savePemFile,
)
from redisbench_admin.run_async.async_terraform import (
    TerraformClass,
)
from redisbench_admin.utils.remote import (
    get_overall_dashboard_keynames,
    check_ec2_env,
    get_project_ts_tags,
    push_data_to_redistimeseries,
    execute_remote_commands,
    copy_file_to_remote_setup,
)

from redisbench_admin.utils.utils import (
    EC2_PRIVATE_PEM,
    EC2_ACCESS_KEY,
    EC2_SECRET_KEY,
    EC2_REGION,
)

from slack_sdk.webhook import WebhookClient

# 7 days expire
STALL_INFO_DAYS = 7
EXPIRE_TIME_SECS_PROFILE_KEYS = 60 * 60 * 24 * STALL_INFO_DAYS
EXPIRE_TIME_MSECS_PROFILE_KEYS = EXPIRE_TIME_SECS_PROFILE_KEYS * 1000


def is_important_data(tf_github_branch, artifact_version):
    if artifact_version is not None or (
        tf_github_branch == "master" or tf_github_branch == "main"
    ):
        return True
    else:
        return False


def run_async_command_logic(argv, args, project_name, project_version):
    logging.info(
        "Using: {project_name} {project_version}".format(
            project_name=project_name, project_version=project_version
        )
    )
    tf = TerraformClass()
    tf.tf_bin_path = args.terraform_bin_path
    tf.tf_github_org = args.github_org
    tf.tf_github_actor = args.github_actor
    tf.tf_github_repo = args.github_repo
    tf.tf_github_sha = args.github_sha
    tf.tf_github_branch = args.github_branch
    tf.tf_override_name = TF_OVERRIDE_NAME
    tf.tf_folder_path = TF_OVERRIDE_REMOTE

    # check tf variables relation to git
    tf.git_vars_crosscheck()

    tf.tf_triggering_env = args.triggering_env
    tf.tf_setup_name_suffix = "{}-{}".format(args.setup_name_sufix, tf.tf_github_sha)
    local_module_files = args.module_path
    private_key = args.private_key
    webhook_notifications_active = None
    if WH_TOKEN is not None:
        webhook_notifications_active = True

    webhook_url = "https://hooks.slack.com/services/{}".format(WH_TOKEN)

    ci_job_link = CIRCLE_BUILD_URL
    ci_job_name = CIRCLE_JOB
    failure_reason = ""
    webhook_client_slack = None
    if ci_job_link is None:
        webhook_notifications_active = False
        logging.warning(
            "Disabling webhook notificaitons given CIRCLE_BUILD_URL is None"
        )

    if webhook_notifications_active is True:
        logging.info(
            "Detected where in a CI flow named {}. Here's the reference link: {}".format(
                ci_job_name, ci_job_link
            )
        )
        webhook_client_slack = WebhookClient(webhook_url)

    if args.skip_env_vars_verify is False:
        env_check_status, failure_reason = check_ec2_env()
        if env_check_status is False:
            if webhook_notifications_active:
                generate_failure_notification(
                    webhook_client_slack,
                    ci_job_name,
                    ci_job_link,
                    failure_reason,
                    tf.tf_github_org,
                    tf.tf_github_repo,
                    tf.tf_github_branch,
                    None,
                )
            logging.critical("{}. Exiting right away!".format(failure_reason))
            exit(1)

    module_check_status, error_message = redis_modules_check(local_module_files)
    if module_check_status is False:
        if webhook_notifications_active:
            failure_reason = error_message
            generate_failure_notification(
                webhook_client_slack,
                ci_job_name,
                ci_job_link,
                failure_reason,
                tf.tf_github_org,
                tf.tf_github_repo,
                tf.tf_github_branch,
                None,
            )
        exit(1)

    # log properties for terraform
    tf.common_properties_log(private_key)

    ssh_pem_check(EC2_PRIVATE_PEM, private_key)

    benchmark = BenchmarkClass()

    benchmark.prepare_benchmark_definitions(args)

    return_code = 0
    if benchmark.benchmark_defs_result is False:
        return_code = 1
        if args.fail_fast:
            failure_reason = "Detected errors while preparing benchmark definitions"
            logging.critical("{}. Exiting right away!".format(failure_reason))
            if webhook_notifications_active:
                generate_failure_notification(
                    webhook_client_slack,
                    ci_job_name,
                    ci_job_link,
                    failure_reason,
                    tf.tf_github_org,
                    tf.tf_github_repo,
                    tf.tf_github_branch,
                    None,
                )

            exit(return_code)

    (
        _,
        _,
        _,
        tsname_project_total_failures,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
    ) = get_overall_dashboard_keynames(
        tf.tf_github_org, tf.tf_github_repo, tf.tf_triggering_env
    )

    benchmark.populate_remote_envs_timeout()

    for remote_id, termination_timeout_secs in benchmark.remote_envs_timeout.items():
        logging.info(
            "Using a timeout of {} seconds for remote setup: {}".format(
                termination_timeout_secs, remote_id
            )
        )

    # we have a map of test-type, dataset-name, topology, test-name
    benchmark.benchmark_runs_plan = define_benchmark_plan(
        benchmark.benchmark_definitions, benchmark.default_specs
    )

    # create service file
    renderServiceFile(
        access_key=EC2_ACCESS_KEY,
        secret_key=EC2_SECRET_KEY,
        region=EC2_REGION,
        gh_token=os.getenv("GH_TOKEN", None),
        job_name=os.getenv("CIRCLE_JOB", None),
        args=args,
        argv=argv,
    )

    # create run.py file for running redisbench-cli
    renderRunFile()
    savePemFile(EC2_PRIVATE_PEM)

    # copy module file
    if len(args.module_path) != 0:
        copy(args.module_path[0], os.getcwd())

    # zip all
    archive_name = tar_files()

    # create runner
    tf.async_runner_setup()

    # push archive to runner

    copy_file_to_remote_setup(
        tf.runner_public_ip,
        "ubuntu",
        private_key,
        archive_name,
        archive_name,
        dirname=None,
        port=22,
    )

    # send commands
    execute_remote_commands(
        tf.runner_public_ip,
        "ubuntu",
        private_key,
        [
            "mkdir -p work_dir",
            "tar xf {} -C work_dir".format(archive_name),
            "cp work_dir/tests/benchmarks/benchmarks.redislabs.pem /tmp/benchmarks.redislabs.pem",
            "sudo apt update",
            "sudo apt install -y gnupg software-properties-common",
            "wget -O- https://apt.releases.hashicorp.com/gpg | gpg --dearmor | sudo tee "
            "/usr/share/keyrings/hashicorp-archive-keyring.gpg >/dev/null",
            "gpg --no-default-keyring --keyring /usr/share/keyrings/hashicorp-archive-keyring.gpg --fingerprint",
            'echo "deb [signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] '
            'https://apt.releases.hashicorp.com $(lsb_release -cs) main" | sudo tee '
            "/etc/apt/sources.list.d/hashicorp.list",
            "sudo apt update",
            "sudo apt install terraform",
            "curl -sSL https://install.python-poetry.org | POETRY_VERSION=1.2.2 python3 -",
            'cd work_dir/redisbench-admin && PATH="/home/ubuntu/.local/bin:$PATH" poetry config '
            "virtualenvs.in-project true",
            'cd work_dir/redisbench-admin && PATH="/home/ubuntu/.local/bin:$PATH" poetry install',
            "./work_dir/deps/readies/bin/getdocker",
            "cd work_dir && sudo cp tests/benchmarks/redisbench-admin.service /etc/systemd/system",
            "sudo systemctl daemon-reload",
            "sudo systemctl start redisbench-admin.service",
        ],
        "22",
        get_pty=True,
    )

    # render service file

    if return_code != 0 and webhook_notifications_active:
        if failure_reason == "":
            failure_reason = "Some unexpected exception was caught during remote work"
        generate_failure_notification(
            webhook_client_slack,
            ci_job_name,
            ci_job_link,
            failure_reason,
            tf.tf_github_org,
            tf.tf_github_repo,
            tf.tf_github_branch,
            None,
        )

    exit(return_code)


def get_tmp_folder_rnd():
    temporary_dir = "/tmp/{}".format(
        "".join(random.choice(string.ascii_lowercase) for i in range(20))
    )
    return temporary_dir


def ro_benchmark_reuse(
    artifact_version,
    benchmark_type,
    cluster_enabled,
    dataset_load_duration_seconds,
    full_logfiles,
    redis_conns,
    return_code,
    server_plaintext_port,
    setup_details,
    ssh_tunnel,
):
    assert benchmark_type == "read-only"
    logging.info(
        "Given the benchmark for this setup is ready-only, and this setup was already spinned we will reuse the previous, conns and process info."
    )
    artifact_version = setup_details["env"]["artifact_version"]
    cluster_enabled = setup_details["env"]["cluster_enabled"]
    dataset_load_duration_seconds = setup_details["env"][
        "dataset_load_duration_seconds"
    ]
    full_logfiles = setup_details["env"]["full_logfiles"]
    redis_conns = setup_details["env"]["redis_conns"]
    return_code = setup_details["env"]["return_code"]
    server_plaintext_port = setup_details["env"]["server_plaintext_port"]
    ssh_tunnel = setup_details["env"]["ssh_tunnel"]
    return (
        artifact_version,
        cluster_enabled,
        dataset_load_duration_seconds,
        full_logfiles,
        redis_conns,
        return_code,
        server_plaintext_port,
        ssh_tunnel,
    )


def ro_benchmark_set(
    artifact_version,
    cluster_enabled,
    dataset_load_duration_seconds,
    redis_conns,
    return_code,
    server_plaintext_port,
    setup_details,
    ssh_tunnel,
    full_logfiles,
):
    logging.info(
        "Given the benchmark for this setup is ready-only we will prepare to reuse it on the next read-only benchmarks (if any )."
    )
    setup_details["env"] = {}
    setup_details["env"]["full_logfiles"] = full_logfiles

    setup_details["env"]["artifact_version"] = artifact_version
    setup_details["env"]["cluster_enabled"] = cluster_enabled
    setup_details["env"][
        "dataset_load_duration_seconds"
    ] = dataset_load_duration_seconds
    setup_details["env"]["redis_conns"] = redis_conns
    setup_details["env"]["return_code"] = return_code
    setup_details["env"]["server_plaintext_port"] = server_plaintext_port
    setup_details["env"]["ssh_tunnel"] = ssh_tunnel


def export_redis_metrics(
    artifact_version,
    end_time_ms,
    overall_end_time_metrics,
    rts,
    setup_name,
    setup_type,
    test_name,
    tf_github_branch,
    tf_github_org,
    tf_github_repo,
    tf_triggering_env,
    metadata_dict=None,
    expire_ms=0,
):
    datapoint_errors = 0
    datapoint_inserts = 0
    sprefix = (
        "ci.benchmarks.redislabs/"
        + "{triggering_env}/{github_org}/{github_repo}".format(
            triggering_env=tf_triggering_env,
            github_org=tf_github_org,
            github_repo=tf_github_repo,
        )
    )
    logging.info(
        "Adding a total of {} server side metrics collected at the end of benchmark".format(
            len(list(overall_end_time_metrics.items()))
        )
    )
    timeseries_dict = {}
    by_variants = {}
    if tf_github_branch is not None and tf_github_branch != "":
        by_variants["by.branch/{}".format(tf_github_branch)] = {
            "branch": tf_github_branch
        }
    if artifact_version is not None and artifact_version != "":
        by_variants["by.version/{}".format(artifact_version)] = {
            "version": artifact_version
        }
    for (
        by_variant,
        variant_labels_dict,
    ) in by_variants.items():
        for (
            metric_name,
            metric_value,
        ) in overall_end_time_metrics.items():
            tsname_metric = "{}/{}/{}/benchmark_end/{}".format(
                sprefix,
                test_name,
                by_variant,
                metric_name,
            )

            logging.debug(
                "Adding a redis server side metric collected at the end of benchmark."
                + " metric_name={} metric_value={} time-series name: {}".format(
                    metric_name,
                    metric_value,
                    tsname_metric,
                )
            )
            variant_labels_dict["test_name"] = test_name
            variant_labels_dict["metric"] = metric_name
            if metadata_dict is not None:
                variant_labels_dict.update(metadata_dict)

            timeseries_dict[tsname_metric] = {
                "labels": get_project_ts_tags(
                    tf_github_org,
                    tf_github_repo,
                    setup_name,
                    setup_type,
                    tf_triggering_env,
                    variant_labels_dict,
                    None,
                    None,
                ),
                "data": {end_time_ms: metric_value},
            }
    i_errors, i_inserts = push_data_to_redistimeseries(rts, timeseries_dict, expire_ms)
    datapoint_errors = datapoint_errors + i_errors
    datapoint_inserts = datapoint_inserts + i_inserts
    return datapoint_errors, datapoint_inserts


def shutdown_remote_redis(redis_conns, ssh_tunnel):
    logging.info("Shutting down remote redis.")
    for conn in redis_conns:
        conn.shutdown(save=False)
    ssh_tunnel.close()  # Close the tunnel
