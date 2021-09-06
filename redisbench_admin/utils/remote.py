#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#

import configparser
import logging
import os
import sys
import tempfile

import git
import paramiko
import pysftp
import redis
from git import Repo
from jsonpath_ng import parse
from python_terraform import Terraform

from redisbench_admin.utils.local import check_dataset_local_requirements
from redisbench_admin.utils.utils import (
    get_ts_metric_name,
    EC2_REGION,
    EC2_SECRET_KEY,
    EC2_ACCESS_KEY,
)

# environment variables
PERFORMANCE_RTS_PUSH = bool(os.getenv("PUSH_RTS", False))
PERFORMANCE_RTS_AUTH = os.getenv("PERFORMANCE_RTS_AUTH", None)
PERFORMANCE_RTS_USER = os.getenv("PERFORMANCE_RTS_USER", None)
PERFORMANCE_RTS_HOST = os.getenv("PERFORMANCE_RTS_HOST", "localhost")
PERFORMANCE_RTS_PORT = os.getenv("PERFORMANCE_RTS_PORT", 6379)
TERRAFORM_BIN_PATH = os.getenv("TERRAFORM_BIN_PATH", "terraform")


def get_git_root(path):
    git_repo = git.Repo(path, search_parent_directories=True)
    git_root = git_repo.git.rev_parse("--show-toplevel")
    return git_root


def view_bar_simple(a, b):
    res = a / int(b) * 100
    sys.stdout.write("\r    Complete precent: %.2f %%" % res)
    sys.stdout.flush()


def copy_file_to_remote_setup(
    server_public_ip,
    username,
    private_key,
    local_file,
    remote_file,
    dirname=None,
    port=22,
):
    full_local_path = local_file
    if dirname is not None:
        full_local_path = "{}/{}".format(dirname, local_file)
    logging.info(
        "Copying local file {} to remote server {}".format(full_local_path, remote_file)
    )
    logging.info(
        "Checking if local file {} exists: {}.".format(
            full_local_path, os.path.exists(full_local_path)
        )
    )
    if os.path.exists(full_local_path):
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        srv = pysftp.Connection(
            host=server_public_ip,
            username=username,
            private_key=private_key,
            cnopts=cnopts,
            port=port,
        )
        srv.put(full_local_path, remote_file, callback=view_bar_simple)
        srv.close()
        logging.info(
            "Finished Copying file {} to remote server {} ".format(
                full_local_path, remote_file
            )
        )
        res = True
    else:
        logging.error(
            "Local file {} does not exists. aborting...".format(full_local_path)
        )
        raise Exception(
            "Local file {} does not exists. aborting...".format(full_local_path)
        )
    return res


def fetch_file_from_remote_setup(
    server_public_ip, username, private_key, local_file, remote_file
):
    logging.info(
        "Retrieving remote file {} from remote server {} ".format(
            remote_file, server_public_ip
        )
    )
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    srv = pysftp.Connection(
        host=server_public_ip, username=username, private_key=private_key, cnopts=cnopts
    )
    srv.get(remote_file, local_file, callback=view_bar_simple)
    srv.close()
    logging.info(
        "Finished retrieving remote file {} from remote server {} ".format(
            remote_file, server_public_ip
        )
    )


def execute_remote_commands(server_public_ip, username, private_key, commands, port=22):
    res = []
    k = paramiko.RSAKey.from_private_key_file(private_key)
    c = paramiko.SSHClient()
    c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    logging.info("Connecting to remote server {}".format(server_public_ip))
    c.connect(hostname=server_public_ip, port=port, username=username, pkey=k)
    logging.info("Connected to remote server {}".format(server_public_ip))
    for command in commands:
        logging.info('Executing remote command "{}"'.format(command))
        stdin, stdout, stderr = c.exec_command(command)
        recv_exit_status = stdout.channel.recv_exit_status()  # status is 0
        stdout = stdout.readlines()
        stderr = stderr.readlines()
        res.append([recv_exit_status, stdout, stderr])
    c.close()
    return res


def check_dataset_remote_requirements(
    benchmark_config,
    server_public_ip,
    username,
    private_key,
    remote_dataset_file,
    dirname,
    number_primaries,
    is_cluster,
):
    res = True
    dataset, fullpath, tmppath = check_dataset_local_requirements(
        benchmark_config,
        ".",
        dirname,
        "./datasets",
        "dbconfig",
        number_primaries,
        is_cluster,
    )
    if dataset is not None:
        logging.info(
            'Detected dataset config. Will copy file to remote setup... "{}"'.format(
                dataset
            )
        )
        res = copy_file_to_remote_setup(
            server_public_ip,
            username,
            private_key,
            fullpath,
            remote_dataset_file,
            None,
        )
    return res, dataset, fullpath, tmppath


def setup_remote_environment(
    tf: Terraform,
    tf_github_sha,
    tf_github_actor,
    tf_setup_name,
    tf_github_org,
    tf_github_repo,
    tf_triggering_env,
):
    # key    = "benchmarks/infrastructure/tf-oss-redisgraph-standalone-r5.tfstate"
    _, _, _ = tf.init(
        capture_output=True,
        backend_config={
            "key": "benchmarks/infrastructure/{}.tfstate".format(tf_setup_name)
        },
    )
    _, _, _ = tf.refresh()
    tf_output = tf.output()
    server_private_ip = tf_output_or_none(tf_output, "server_private_ip")
    server_public_ip = tf_output_or_none(tf_output, "server_public_ip")
    client_private_ip = tf_output_or_none(tf_output, "client_private_ip")
    client_public_ip = tf_output_or_none(tf_output, "client_public_ip")
    if (
        server_private_ip is not None
        or server_public_ip is not None
        or client_private_ip is not None
        or client_public_ip is not None
    ):
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
        },
    )
    return retrieve_tf_connection_vars(return_code, tf)


def retrieve_tf_connection_vars(return_code, tf):
    tf_output = tf.output()
    server_private_ip = tf_output["server_private_ip"]["value"][0]
    server_public_ip = tf_output["server_public_ip"]["value"][0]
    server_plaintext_port = 6379
    client_private_ip = tf_output["client_private_ip"]["value"][0]
    client_public_ip = tf_output["client_public_ip"]["value"][0]
    username = "ubuntu"
    return (
        return_code,
        username,
        server_private_ip,
        server_public_ip,
        server_plaintext_port,
        client_private_ip,
        client_public_ip,
    )


def tf_output_or_none(tf_output, output_prop):
    res = None
    if output_prop in tf_output:
        res = tf_output[output_prop]["value"][0]
    return res


def extract_git_vars(path=None, github_url=None):
    github_org_name = None
    github_repo_name = None
    github_sha = None
    github_actor = None
    github_branch = None
    github_branch_detached = None
    try:
        if path is None:
            path = get_git_root(".")
        github_repo = Repo(path)
        if github_url is None:
            github_url = github_repo.remotes[0].config_reader.get("url")
        if "/" in github_url[-1:]:
            github_url = github_url[:-1]
        if "http" in github_url:
            github_org_name = github_url.split("/")[-2]
            github_repo_name = github_url.split("/")[-1].split(".")[0]
        else:
            github_url = github_url.replace(".git", "")
            github_org_name = github_url.split(":")[1].split("/")[0]
            github_repo_name = github_url.split(":")[1].split("/")[1]
        github_sha = github_repo.head.object.hexsha
        github_branch = None
        github_branch_detached = False
        try:
            github_branch = github_repo.active_branch
        except TypeError as e:
            logging.warning(
                "Unable to detected github_branch. caught the following error: {}".format(
                    e.__str__()
                )
            )
            github_branch_detached = True

        github_actor = None
        try:
            github_actor = github_repo.config_reader().get_value("user", "name")
        except configparser.NoSectionError as e:
            logging.warning(
                "Unable to detected github_actor. caught the following error: {}".format(
                    e.__str__()
                )
            )
            github_branch_detached = True
    except git.exc.InvalidGitRepositoryError as e:
        logging.warning(
            "Unable to fill git vars. caught the following error: {}".format(
                e.__str__()
            )
        )
        github_branch_detached = True
    return (
        github_org_name,
        github_repo_name,
        github_sha,
        github_actor,
        github_branch,
        github_branch_detached,
    )


def validate_result_expectations(
    benchmark_config, results_dict, result, expectations_key="kpis"
):
    for expectation in benchmark_config[expectations_key]:
        for comparison_mode, rules in expectation.items():
            for jsonpath, expected_value in rules.items():
                try:
                    jsonpath_expr = parse(jsonpath)
                except Exception:
                    pass
                finally:
                    r = jsonpath_expr.find(results_dict)
                    if len(r) > 0:
                        actual_value = float(r[0].value)
                        expected_value = float(expected_value)
                        if comparison_mode == "eq":
                            if actual_value != expected_value:
                                result &= False
                                logging.error(
                                    "Condition on {} {} {} {} is False. Failing test expectations".format(
                                        jsonpath,
                                        actual_value,
                                        comparison_mode,
                                        expected_value,
                                    )
                                )
                            else:
                                logging.info(
                                    "Condition on {} {} {} {} is True.".format(
                                        jsonpath,
                                        actual_value,
                                        comparison_mode,
                                        expected_value,
                                    )
                                )
                        if comparison_mode == "le":
                            if actual_value > expected_value:
                                result &= False
                                logging.error(
                                    "Condition on {} {} {} {} is False. Failing test expectations".format(
                                        jsonpath,
                                        actual_value,
                                        comparison_mode,
                                        expected_value,
                                    )
                                )
                            else:
                                logging.info(
                                    "Condition on {} {} {} {} is True.".format(
                                        jsonpath,
                                        actual_value,
                                        comparison_mode,
                                        expected_value,
                                    )
                                )
                        if comparison_mode == "ge":
                            if actual_value < expected_value:
                                result &= False
                                logging.error(
                                    "Condition on {} {} {} {} is False. Failing test expectations".format(
                                        jsonpath,
                                        actual_value,
                                        comparison_mode,
                                        expected_value,
                                    )
                                )
                            else:
                                logging.info(
                                    "Condition on {} {} {} {} is True.".format(
                                        jsonpath,
                                        actual_value,
                                        comparison_mode,
                                        expected_value,
                                    )
                                )
    return result


def check_and_fix_pem_str(ec2_private_pem: str):
    pem_str = ec2_private_pem.replace("-----BEGIN RSA PRIVATE KEY-----", "")
    pem_str = pem_str.replace("-----END RSA PRIVATE KEY-----", "")
    pem_str = pem_str.replace(" ", "\n")
    pem_str = "-----BEGIN RSA PRIVATE KEY-----\n" + pem_str
    pem_str = pem_str + "-----END RSA PRIVATE KEY-----\n"
    # remove any dangling whitespace
    pem_str = os.linesep.join([s for s in pem_str.splitlines() if s])
    return pem_str


def get_run_full_filename(
    start_time_str,
    deployment_type,
    github_org,
    github_repo,
    github_branch,
    test_name,
    github_sha,
):
    benchmark_output_filename = (
        "{start_time_str}-{github_org}-{github_repo}-{github_branch}"
        "-{test_name}-{deployment_type}-{github_sha}.json".format(
            start_time_str=start_time_str,
            github_org=github_org,
            github_repo=github_repo,
            github_branch=github_branch,
            test_name=test_name,
            deployment_type=deployment_type,
            github_sha=github_sha,
        )
    )
    return benchmark_output_filename


def fetch_remote_setup_from_config(
    remote_setup_config,
    repo="https://github.com/RedisLabsModules/testing-infrastructure.git",
    branch="master",
):
    setup_type = None
    setup = None
    for remote_setup_property in remote_setup_config:
        if "type" in remote_setup_property:
            setup_type = remote_setup_property["type"]
        if "setup" in remote_setup_property:
            setup = remote_setup_property["setup"]
    # fetch terraform folder
    path = "/terraform/{}-{}".format(setup_type, setup)
    temporary_dir = tempfile.mkdtemp()
    logging.info(
        "Fetching infrastructure definition from git repo {}/{} (branch={})".format(
            repo, path, branch
        )
    )
    git.Repo.clone_from(repo, temporary_dir, branch=branch, depth=1)
    terraform_working_dir = temporary_dir + path
    return terraform_working_dir, setup_type, setup


def push_data_to_redistimeseries(rts, branch_time_series_dict: dict):
    datapoint_errors = 0
    datapoint_inserts = 0
    if rts is not None:
        for timeseries_name, time_series in branch_time_series_dict.items():
            exporter_create_ts(rts, time_series, timeseries_name)
            for timestamp, value in time_series["data"].items():
                try:
                    rts.add(
                        timeseries_name,
                        timestamp,
                        value,
                        duplicate_policy="last",
                    )
                    datapoint_inserts += 1
                except redis.exceptions.ResponseError:
                    logging.warning(
                        "Error while inserting datapoint ({} : {}) in timeseries named {}. ".format(
                            timestamp, value, timeseries_name
                        )
                    )
                    datapoint_errors += 1
                    pass
    return datapoint_errors, datapoint_inserts


def exporter_create_ts(rts, time_series, timeseries_name):
    try:
        logging.info(
            "Creating timeseries named {} with labels {}".format(
                timeseries_name, time_series["labels"]
            )
        )
        rts.create(timeseries_name, labels=time_series["labels"])
    except redis.exceptions.ResponseError:
        logging.warning("Timeseries named {} already exists".format(timeseries_name))
        pass


def extract_redisgraph_version_from_resultdict(results_dict: dict):
    version = None
    if "DBSpecificConfigs" in results_dict:
        if "RedisGraphVersion" in results_dict["DBSpecificConfigs"]:
            version = results_dict["DBSpecificConfigs"]["RedisGraphVersion"]
    return version


def extract_perversion_timeseries_from_results(
    datapoints_timestamp: int,
    metrics: list,
    results_dict: dict,
    project_version: str,
    tf_github_org: str,
    tf_github_repo: str,
    deployment_type: str,
    test_name: str,
    tf_triggering_env: str,
    metadata_tags={},
    build_variant_name=None,
    running_platform=None,
    testcase_metric_context_paths=[],
):
    break_by_key = "version"
    break_by_str = "by.{}".format(break_by_key)
    branch_time_series_dict = common_timeseries_extraction(
        break_by_key,
        break_by_str,
        datapoints_timestamp,
        deployment_type,
        metrics,
        project_version,
        results_dict,
        test_name,
        tf_github_org,
        tf_github_repo,
        tf_triggering_env,
        metadata_tags,
        build_variant_name,
        running_platform,
        testcase_metric_context_paths,
    )
    return True, branch_time_series_dict


def common_timeseries_extraction(
    break_by_key,
    break_by_str,
    datapoints_timestamp,
    deployment_type,
    metrics,
    project_version,
    results_dict,
    test_name,
    tf_github_org,
    tf_github_repo,
    tf_triggering_env,
    metadata_tags={},
    build_variant_name=None,
    running_platform=None,
    testcase_metric_context_paths=[],
):
    branch_time_series_dict = {}
    for jsonpath in metrics:
        try:
            jsonpath_expr = parse(jsonpath)
        except Exception:
            pass
        finally:
            find_res = jsonpath_expr.find(results_dict)
            if find_res is not None:
                use_metric_context_path = False
                if len(find_res) > 1:
                    use_metric_context_path = True
                for metric in find_res:
                    metric_name = str(metric.path)
                    metric_value = float(metric.value)
                    metric_jsonpath = jsonpath
                    metric_context_path = str(metric.context.path)
                    if metric_jsonpath[0] == "$":
                        metric_jsonpath = metric_jsonpath[1:]
                    if metric_jsonpath[0] == ".":
                        metric_jsonpath = metric_jsonpath[1:]

                    # retro-compatible naming
                    if use_metric_context_path is False:
                        metric_name = metric_jsonpath

                    # prepare tags
                    timeserie_tags = get_project_ts_tags(
                        tf_github_org,
                        tf_github_repo,
                        deployment_type,
                        tf_triggering_env,
                        metadata_tags,
                        build_variant_name,
                        running_platform,
                    )
                    timeserie_tags[break_by_key] = project_version
                    timeserie_tags["test_name"] = str(test_name)
                    timeserie_tags["metric"] = str(metric_name)
                    timeserie_tags["metric_name"] = metric_name
                    timeserie_tags["metric_context_path"] = metric_context_path
                    timeserie_tags["metric_jsonpath"] = metric_jsonpath
                    if metric_context_path not in testcase_metric_context_paths:
                        testcase_metric_context_paths.append(metric_context_path)

                    ts_name = get_ts_metric_name(
                        break_by_str,
                        project_version,
                        tf_github_org,
                        tf_github_repo,
                        deployment_type,
                        test_name,
                        tf_triggering_env,
                        metric_name,
                        metric_context_path,
                        use_metric_context_path,
                        build_variant_name,
                        running_platform,
                    )
                    branch_time_series_dict[ts_name] = {
                        "labels": timeserie_tags.copy(),
                        "data": {datapoints_timestamp: metric_value},
                    }
            else:
                logging.warning(
                    "Unable to find metric path {} in result dict".format(jsonpath)
                )
    return branch_time_series_dict


def get_project_ts_tags(
    tf_github_org: str,
    tf_github_repo: str,
    deployment_type: str,
    tf_triggering_env: str,
    metadata_tags={},
    build_variant_name=None,
    running_platform=None,
):
    tags = {
        "github_org": tf_github_org,
        "github_repo": tf_github_repo,
        "github_org/github_repo": "{}/{}".format(tf_github_org, tf_github_repo),
        "deployment_type": deployment_type,
        "triggering_env": tf_triggering_env,
    }
    if build_variant_name is not None:
        tags["build_variant"] = build_variant_name
    if running_platform is not None:
        tags["running_platform"] = running_platform
    for k, v in metadata_tags.items():
        tags[k] = str(v)
    return tags


def extract_perbranch_timeseries_from_results(
    datapoints_timestamp: int,
    metrics: list,
    results_dict: dict,
    tf_github_branch: str,
    tf_github_org: str,
    tf_github_repo: str,
    deployment_type: str,
    test_name: str,
    tf_triggering_env: str,
    metadata_tags={},
    build_variant_name=None,
    running_platform=None,
    testcase_metric_context_paths=[],
):
    break_by_key = "branch"
    break_by_str = "by.{}".format(break_by_key)
    branch_time_series_dict = common_timeseries_extraction(
        break_by_key,
        break_by_str,
        datapoints_timestamp,
        deployment_type,
        metrics,
        tf_github_branch,
        results_dict,
        test_name,
        tf_github_org,
        tf_github_repo,
        tf_triggering_env,
        metadata_tags,
        build_variant_name,
        running_platform,
        testcase_metric_context_paths,
    )
    return True, branch_time_series_dict


def get_overall_dashboard_keynames(
    tf_github_org,
    tf_github_repo,
    tf_triggering_env,
    build_variant_name=None,
    running_platform=None,
    test_name=None,
):
    build_variant_str = ""
    if build_variant_name is not None:
        build_variant_str = "/{}".format(build_variant_name)
    running_platform_str = ""
    if running_platform is not None:
        running_platform_str = "/{}".format(running_platform)
    sprefix = (
        "ci.benchmarks.redislabs/"
        + "{triggering_env}/{github_org}/{github_repo}".format(
            triggering_env=tf_triggering_env,
            github_org=tf_github_org,
            github_repo=tf_github_repo,
        )
    )
    testcases_setname = "{}:testcases".format(sprefix)
    project_archs_setname = "{}:archs".format(sprefix)
    project_oss_setname = "{}:oss".format(sprefix)
    project_branches_setname = "{}:branches".format(sprefix)
    project_versions_setname = "{}:versions".format(sprefix)
    project_compilers_setname = "{}:compilers".format(sprefix)
    running_platforms_setname = "{}:platforms".format(sprefix)
    build_variant_setname = "{}:build_variants".format(sprefix)
    build_variant_prefix = "{sprefix}{build_variant_str}".format(
        sprefix=sprefix,
        build_variant_str=build_variant_str,
    )
    prefix = "{build_variant_prefix}{running_platform_str}".format(
        build_variant_prefix=build_variant_prefix,
        running_platform_str=running_platform_str,
    )
    tsname_project_total_success = "{}:total_success".format(
        prefix,
    )
    tsname_project_total_failures = "{}:total_failures".format(
        prefix,
    )
    testcases_metric_context_path_setname = ""
    if test_name is None:
        testcases_metric_context_path_setname = (
            "{testcases_setname}:metric_context_path:{test_name}".format(
                testcases_setname=testcases_setname, test_name=test_name
            )
        )
    testcases_and_metric_context_path_setname = (
        "{testcases_setname}_AND_metric_context_path".format(
            testcases_setname=testcases_setname
        )
    )
    return (
        prefix,
        testcases_setname,
        tsname_project_total_failures,
        tsname_project_total_success,
        running_platforms_setname,
        build_variant_setname,
        testcases_metric_context_path_setname,
        testcases_and_metric_context_path_setname,
        project_archs_setname,
        project_oss_setname,
        project_branches_setname,
        project_versions_setname,
        project_compilers_setname,
    )


def check_ec2_env():
    if EC2_ACCESS_KEY is None or EC2_ACCESS_KEY == "":
        logging.error("missing required AWS_ACCESS_KEY_ID env variable")
        exit(1)
    if EC2_REGION is None or EC2_REGION == "":
        logging.error("missing required AWS_DEFAULT_REGION env variable")
        exit(1)
    if EC2_SECRET_KEY is None or EC2_SECRET_KEY == "":
        logging.error("missing required AWS_SECRET_ACCESS_KEY env variable")
        exit(1)
