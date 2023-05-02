#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#

import csv
import datetime as dt
import json
import logging
import operator
import os
import os.path
import tarfile
import time
from functools import reduce
from urllib.parse import quote_plus
from zipfile import ZipFile

import boto3
import redis
import requests
from tqdm import tqdm

EPOCH = dt.datetime.utcfromtimestamp(0)


def redis_server_config_module_part(
    command, local_module_file, modules_configuration_parameters_map
):
    # in the case of modules with plugins we split by space
    splitted_module_and_plugins = local_module_file.split(" ")
    if len(splitted_module_and_plugins) > 1:
        logging.info(
            "Detected a module and plugin(s) pairs {}".format(
                splitted_module_and_plugins
            )
        )

    abs_splitted_module_and_plugins = []
    for x in splitted_module_and_plugins:
        if os.path.exists(os.path.abspath(x)):
            abs_splitted_module_and_plugins.append(os.path.abspath(x))
        else:
            abs_splitted_module_and_plugins.append(x)

    command.append("--loadmodule")
    command.extend(abs_splitted_module_and_plugins)
    for (
        module_config_modulename,
        module_config_dict,
    ) in modules_configuration_parameters_map.items():
        if module_config_modulename in local_module_file:
            for (
                module_config_parameter_name,
                module_config_parameter_value,
            ) in module_config_dict.items():
                if type(module_config_parameter_value) != str:
                    module_config_parameter_value = "{}".format(
                        module_config_parameter_value
                    )
                command.extend(
                    [
                        module_config_parameter_name,
                        module_config_parameter_value,
                    ]
                )


def generate_common_server_args(
    binary,
    daemonize,
    dbdir,
    dbfilename,
    enable_debug_command,
    ip,
    logfile,
    port,
    enable_redis_7_config_directives=False,
):
    if type(binary) == list:
        command = binary
    else:
        command = [binary]
    command.extend(
        [
            "--appendonly",
            "no",
            "--logfile",
            logfile,
            "--daemonize",
            daemonize,
            "--dbfilename",
            dbfilename,
            "--protected-mode",
            "no",
            "--bind",
            "{}".format(ip),
            "--save",
            "''",
            "--port",
            "{}".format(port),
            "--dir",
            dbdir,
        ]
    )
    if enable_redis_7_config_directives:
        command.extend(
            [
                "--enable-debug-command",
                enable_debug_command,
            ]
        )

    return command


def upload_artifacts_to_s3(
    artifacts,
    s3_bucket_name,
    s3_bucket_path,
    aws_access_key_id=None,
    aws_secret_access_key=None,
    aws_session_token=None,
    region_name=None,
):
    artifacts_map = {}
    if region_name is None:
        region_name = EC2_REGION
    logging.info("-- uploading results to s3 -- ")
    if aws_access_key_id is not None and aws_secret_access_key is not None:
        logging.info("-- Using REQUEST PROVIDED AWS credentials -- ")
        session = boto3.Session(
            aws_access_key_id, aws_secret_access_key, aws_session_token, region_name
        )
        s3 = session.resource("s3")
    else:
        logging.info("-- Using default AWS credentials -- ")
        s3 = boto3.resource("s3")
    bucket = s3.Bucket(s3_bucket_name)
    progress = tqdm(unit="files", total=len(artifacts))

    for full_artifact_path in artifacts:
        artifact = os.path.basename(full_artifact_path)
        object_key = "{bucket_path}{filename}".format(
            bucket_path=s3_bucket_path, filename=artifact
        )

        bucket.upload_file(full_artifact_path, object_key)
        object_acl = s3.ObjectAcl(s3_bucket_name, object_key)
        object_acl.put(ACL="public-read")
        progress.update()
        url = "https://s3.{0}.amazonaws.com/{1}/{2}{3}".format(
            region_name, s3_bucket_name, s3_bucket_path, quote_plus(artifact)
        )
        artifacts_map[artifact] = url
    progress.close()
    return artifacts_map


def whereis(program):
    for path in os.environ.get("PATH", "").split(":"):
        if os.path.exists(os.path.join(path, program)) and not os.path.isdir(
            os.path.join(path, program)
        ):
            return os.path.join(path, program)
    return None


# Check if system has the required utilities: ftsb_redisearch, etc
def required_utilities(utility_list):
    result = 1
    for index in utility_list:
        if whereis(index) is None:
            print("Cannot locate " + index + " in path!")
            result = 0
    return result


def get_decompressed_filename(compressed_filename: str):
    uncompressed_filename = None
    for suffix in [".zip", ".tar.gz", "tar"]:
        if compressed_filename.endswith(suffix):
            uncompressed_filename = compressed_filename[: -len(suffix)]
    return uncompressed_filename


def decompress_file(compressed_filename: str, path=None):
    uncompressed_filename = compressed_filename
    logging.warning("Decompressing {}...".format(compressed_filename))
    if compressed_filename.endswith(".zip"):
        with ZipFile(compressed_filename, "r") as zipObj:
            zipObj.extractall(path)
            suffix = ".zip"
        uncompressed_filename = compressed_filename[: -len(suffix)]

    elif compressed_filename.endswith(".tar.gz"):
        tar = tarfile.open(compressed_filename, "r:gz")
        tar.extractall(path)
        tar.close()
        suffix = ".tar.gz"
        uncompressed_filename = compressed_filename[: -len(suffix)]

    elif compressed_filename.endswith(".tar"):
        tar = tarfile.open(compressed_filename, "r:")
        tar.extractall(path)
        tar.close()
        suffix = ".tar"
        uncompressed_filename = compressed_filename[: -len(suffix)]
    else:
        logging.warning(
            "Filename {} was not in a supported compression extension [zip|tar.gz|tar]".format(
                compressed_filename
            )
        )
    return uncompressed_filename


def find_json_path(element, json_dict):
    return reduce(operator.getitem, element.split("."), json_dict)


def ts_milli(at_dt):
    return int((at_dt - dt.datetime(1970, 1, 1)).total_seconds() * 1000)


def retrieve_local_or_remote_input_json(
    config_filename, local_path, option_name, input_format="json", csv_header=False
):
    benchmark_config = {}
    if config_filename.startswith("http"):
        print(
            "retrieving benchmark config file from remote url {}".format(
                config_filename
            )
        )
        r = requests.get(config_filename)
        benchmark_config[config_filename] = r.json()
        filename_start_pos = config_filename.rfind("/") + 1
        remote_filename = config_filename[filename_start_pos:]
        local_config_file = "{}/{}".format(local_path, remote_filename)
        open(local_config_file, "wb").write(r.content)
        print(
            "To avoid fetching again the config file use the option {option_name} {filename}".format(
                option_name=option_name, filename=local_config_file
            )
        )

    elif config_filename.startswith("S3://") or config_filename.startswith("s3://"):
        print("s3")
        s3 = boto3.resource("s3")
        bucket_str = config_filename[5:].split("/")[0]

        bucket_prefix = ""
        if len(config_filename[5:].split("/")) > 0:
            bucket_prefix = "/".join(config_filename[5:].split("/")[1:])
        my_bucket = s3.Bucket(bucket_str)

        print(
            "Retrieving data from s3 bucket: {bucket_str}. Prefix={bucket_prefix}".format(
                bucket_str=bucket_str, bucket_prefix=bucket_prefix
            )
        )
        benchmark_config = {}
        objects = list(my_bucket.objects.filter(Prefix=bucket_prefix))
        for object_summary in tqdm(objects, total=len(objects)):
            filename = object_summary.key.split("/")[-1]
            local_config_file = "{}/{}".format(local_path, filename)
            my_bucket.download_file(object_summary.key, local_config_file)
            with open(local_config_file, "r") as local_file:
                read_json_or_csv(
                    benchmark_config,
                    config_filename,
                    input_format,
                    local_file,
                    csv_header,
                )

    else:
        with open(config_filename, "r") as local_file:
            read_json_or_csv(
                benchmark_config, config_filename, input_format, local_file, csv_header
            )

    return benchmark_config


def read_json_or_csv(
    benchmark_config, config_filename, read_format, local_file, csv_has_header
):
    if read_format == "json":
        benchmark_config[config_filename] = json.load(local_file)
    if read_format == "csv":
        reader = csv.reader(local_file)
        header_array = []
        res_dict = {}
        header_row = next(reader)
        body_rows = [x for x in reader]
        if csv_has_header:
            for col in header_row:
                res_dict[col] = []
                header_array.append(col)
        else:
            for pos, _ in enumerate(header_row):
                col_name = "col_{}".format(pos)
                res_dict[col_name] = []
                header_array.append(col_name)
            newbd = [header_row]
            for x in body_rows:
                newbd.append(x)
            body_rows = newbd

        for row in body_rows:
            for col_pos, col in enumerate(row):
                col_name = header_array[col_pos]
                res_dict[col_name].append(col)
        benchmark_config[config_filename] = res_dict


def get_ts_metric_name(
    by,
    by_value,
    tf_github_org,
    tf_github_repo,
    deployment_name,
    deployment_type,
    test_name,
    tf_triggering_env,
    metric_name,
    metric_context_path=None,
    use_metric_context_path=False,
    build_variant_name=None,
    running_platform=None,
):
    if use_metric_context_path:
        metric_name = "{}/{}".format(metric_name, metric_context_path)
    build_variant_str = ""
    if build_variant_name is not None:
        build_variant_str = "{}/".format(str(build_variant_name))
    running_platform_str = ""
    if running_platform is not None:
        running_platform_str = "{}/".format(str(running_platform))
    if deployment_name != deployment_type:
        deployment_name = "/{}".format(deployment_name)
    else:
        deployment_name = ""
    ts_name = (
        "ci.benchmarks.redislabs/{by}/"
        "{triggering_env}/{github_org}/{github_repo}/"
        "{test_name}/{build_variant_str}{running_platform_str}{deployment_type}{deployment_name}/{by_value}/{metric}".format(
            by=by,
            triggering_env=tf_triggering_env,
            github_org=tf_github_org,
            github_repo=tf_github_repo,
            test_name=test_name,
            deployment_type=deployment_type,
            deployment_name=deployment_name,
            build_variant_str=build_variant_str,
            running_platform_str=running_platform_str,
            by_value=str(by_value),
            metric=metric_name,
        )
    )
    return ts_name


def wait_for_conn(conn, retries=20, command="PING", should_be=True, initial_sleep=1):
    """Wait until a given Redis connection is ready"""
    result = False
    if initial_sleep > 0:
        time.sleep(initial_sleep)  # give extra 1sec in case of RDB loading
    while retries > 0 and result is False:
        try:
            if conn.execute_command(command) == should_be:
                result = True
        except redis.exceptions.BusyLoadingError:
            time.sleep(1)  # give extra 1sec in case of RDB loading
        except redis.ConnectionError as err:
            logging.error(
                "Catched error while waiting for connection {}. Retries available {}".format(
                    err, retries
                )
            )
        except redis.ResponseError as err:
            err1 = str(err)
            if not err1.startswith("DENIED"):
                raise
        time.sleep(1)
        retries -= 1
        logging.debug("Waiting for Redis")
    if retries == 0:
        logging.debug(
            "Redis busy loading time surpassed the timeout of {} secs".format(retries)
        )
    return result


def make_dashboard_callback(
    callback_url,
    return_code,
    ci_job_name,
    tf_github_repo,
    tf_github_branch,
    tf_github_sha,
):
    callback_headers = {}
    status = "success"
    if return_code != 0:
        status = "failed"
    github_token = os.getenv("GH_TOKEN", None)
    if github_token is None:
        logging.error(
            "-- github token is None. Callback will be send without github-token header --"
        )
    else:
        callback_headers = {"Github-Token": github_token}
    callback_url = (
        "{}"
        "?repository={}"
        "&test_name={}"
        "&status={}"
        "&commit={}".format(
            callback_url,
            tf_github_repo,
            ci_job_name,
            status,
            tf_github_sha,
        )
    )
    logging.info("-- make callback to {} -- ".format(callback_url))
    try:
        request = requests.get(callback_url, headers=callback_headers, timeout=10)
    except Exception as ex:
        logging.error("-- callback request exception: {}".format(ex))
        return
    logging.info(
        "-- callback response {} and body {} -- ".format(
            request.status_code, request.text.replace("\n", " ")
        )
    )


EC2_REGION = os.getenv("AWS_DEFAULT_REGION", None)
EC2_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", None)
EC2_PRIVATE_PEM = os.getenv("EC2_PRIVATE_PEM", None)
EC2_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", None)
