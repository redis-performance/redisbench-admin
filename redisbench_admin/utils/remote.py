import logging
import os
import sys

import boto3
import paramiko
import pysftp
from git import Repo
from jsonpath_ng import parse
from python_terraform import Terraform
from tqdm import tqdm


def viewBarSimple(a, b):
    res = a / int(b) * 100
    sys.stdout.write("\r    Complete precent: %.2f %%" % (res))
    sys.stdout.flush()


def copyFileToRemoteSetup(
        server_public_ip, username, private_key, local_file, remote_file
):
    logging.info(
        "\tCopying local file {} to remote server {}".format(local_file, remote_file)
    )
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    srv = pysftp.Connection(
        host=server_public_ip, username=username, private_key=private_key, cnopts=cnopts
    )
    srv.put(local_file, remote_file, callback=viewBarSimple)
    srv.close()
    logging.info("")


def getFileFromRemoteSetup(
        server_public_ip, username, private_key, local_file, remote_file
):
    logging.info(
        "\Retrieving remote file {} from remote server {} ".format(
            remote_file, server_public_ip
        )
    )
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    srv = pysftp.Connection(
        host=server_public_ip, username=username, private_key=private_key, cnopts=cnopts
    )
    srv.get(remote_file, local_file, callback=viewBarSimple)
    srv.close()
    logging.info("")


def executeRemoteCommands(server_public_ip, username, private_key, commands):
    res = []
    k = paramiko.RSAKey.from_private_key_file(private_key)
    c = paramiko.SSHClient()
    c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    logging.info("Connecting to remote server {}".format(server_public_ip))
    c.connect(hostname=server_public_ip, username=username, pkey=k)
    logging.info("Connected to remote server {}".format(server_public_ip))
    for command in commands:
        logging.info('Executing remote command "{}"'.format(command))
        stdin, stdout, stderr = c.exec_command(command)
        stdout = stdout.readlines()
        stderr = stderr.readlines()
        res.append([stdout, stderr])
    c.close()
    return res


def checkDatasetRemoteRequirements(
        benchmark_config, server_public_ip, username, private_key, remote_dataset_file
):
    for k in benchmark_config["dbconfig"]:
        if "dataset" in k:
            dataset = k["dataset"]
    if dataset is not None:
        copyFileToRemoteSetup(
            server_public_ip,
            username,
            private_key,
            dataset,
            remote_dataset_file,
        )


def setupRemoteEnviroment(
        tf: Terraform,
        tf_github_sha,
        tf_github_actor,
        tf_setup_name,
        tf_github_org,
        tf_github_repo,
        tf_triggering_env,
):
    # key    = "benchmarks/infrastructure/tf-oss-redisgraph-standalone-r5.tfstate"
    return_code, stdout, stderr = tf.init(
        capture_output=True,
        backend_config={
            "key": "benchmarks/infrastructure/{}.tfstate".format(tf_setup_name)
        },
    )
    return_code, stdout, stderr = tf.refresh()
    tf_output = tf.output()
    server_private_ip = tf_output["server_private_ip"]["value"][0]
    server_public_ip = tf_output["server_public_ip"]["value"][0]
    client_private_ip = tf_output["client_private_ip"]["value"][0]
    client_public_ip = tf_output["client_public_ip"]["value"][0]
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


def extract_git_vars():
    github_repo = Repo("{}/../..".format(os.getcwd()))
    github_url = github_repo.remotes[0].config_reader.get("url")
    github_org_name = github_url.split("/")[-2]
    github_repo_name = github_url.split("/")[-1].split(".")[0]
    github_sha = github_repo.head.object.hexsha
    github_branch = github_repo.active_branch
    github_actor = github_repo.config_reader().get_value("user", "name")
    return github_org_name, github_repo_name, github_sha, github_actor, github_branch


def validateResultExpectations(benchmark_config, results_dict, result, expectations_key="expectations"):
    for expectation in benchmark_config[expectations_key]:
        for comparison_mode, rules in expectation.items():
            for jsonpath, expected_value in rules.items():
                jsonpath_expr = parse(jsonpath)
                actual_value = float(jsonpath_expr.find(results_dict)[0].value)
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


def upload_artifacts_to_s3(artifacts, s3_bucket_name, s3_bucket_path, acl="public-read"):
    logging.info("Uploading results to s3")
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(s3_bucket_name)
    progress = tqdm(unit="files", total=len(artifacts))
    for input in artifacts:
        object_key = "{bucket_path}{filename}".format(
            bucket_path=s3_bucket_path, filename=input
        )
        bucket.upload_file(input, object_key)
        object_acl = s3.ObjectAcl(s3_bucket_name, object_key)
        response = object_acl.put(ACL=acl)
        progress.update()
    progress.close()
