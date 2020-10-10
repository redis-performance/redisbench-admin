import datetime as dt
import json
import os
import os.path
import sys

import humanize
import pandas as pd
import redis
import requests
from cpuinfo import cpuinfo
from rediscluster import RedisCluster
from tqdm import tqdm

from redisbench_admin.compare.compare import generate_comparison_dataframe_configs
from redisbench_admin.run.ftsb_redisearch.ftsb_redisearch import run_ftsb_redisearch, get_run_options
from redisbench_admin.utils.redisearch import check_and_extract_redisearch_info
from redisbench_admin.utils.results import from_resultsDF_to_key_results_dict
from redisbench_admin.utils.utils import required_utilities, whereis, decompress_file, findJsonPath, ts_milli, \
    retrieve_local_or_remote_input_json, upload_artifacts_to_s3


def run_command_logic(args):
    use_case_specific_arguments = dict(args.__dict__)
    s3_bucket_name = "benchmarks.redislabs"

    local_path = os.path.abspath(args.local_dir)
    workers = args.workers
    pipeline = args.pipeline
    oss_cluster_mode = args.cluster_mode
    max_rps = args.max_rps
    requests = args.requests

    benchmark_machine_info = cpuinfo.get_cpu_info()
    total_cores = benchmark_machine_info['count']
    benchmark_infra = {"total-benchmark-machines": 0, "benchmark-machines": {}, "total-db-machines": 0,
                       "db-machines": {}}
    benchmark_machine_1 = {"machine_info": benchmark_machine_info}
    benchmark_infra["benchmark-machines"]["benchmark-machine-1"] = benchmark_machine_1
    benchmark_infra["total-benchmark-machines"] += 1
    if workers == 0:
        print('Setting number of workers equal to machine VCPUs {}'.format(total_cores))
        workers = total_cores
    else:
        print('Setting number of workers to {}'.format(workers))

    print('Setting pipeline to {}'.format(pipeline))

    deployment_type = args.deployment_type
    config_filename = args.benchmark_config_file
    benchmark_config = retrieve_local_or_remote_input_json(config_filename, local_path, "--benchmark-config-file")
    if benchmark_config is None:
        print('Error while retrieving {}! Exiting..'.format(config_filename))
        sys.exit(1)

    benchmark_config = list(benchmark_config.values())[0]
    project = benchmark_config["project"]
    test_name = benchmark_config["name"]
    description = benchmark_config["description"]
    print("Testing project: {}".format(project))
    print("Preparing to run test: {}.\nDescription: {}.".format(test_name, description))

    deployment_requirements = benchmark_config["deployment-requirements"]
    required_utilities_list = deployment_requirements["utilities"].keys()
    print("Checking required utilities are in place ({})".format(" ".join(required_utilities_list)))

    if required_utilities(required_utilities_list) == 0:
        print('Utilities Missing! Exiting..')
        sys.exit(1)
    benchmark_tool = deployment_requirements["benchmark-tool"]
    print(benchmark_tool)
    benchmark_tool_path = whereis(benchmark_tool)
    if benchmark_tool_path is None:
        benchmark_tool_path = benchmark_tool

    s3_bucket_path = "{project}/results/{test_name}/".format(project=project,test_name=test_name)
    if args.output_file_prefix != "":
        s3_bucket_path = "{}{}/".format(s3_bucket_path, args.output_file_prefix)
    s3_uri = "https://s3.amazonaws.com/{bucket_name}/{bucket_path}".format(bucket_name=s3_bucket_name,
                                                                           bucket_path=s3_bucket_path)
    run_stages = benchmark_config["run-stages"]
    benchmark_output_dict = {
        "key-configs": {},
        "key-results": {},
        "benchmark-config": benchmark_config,
        "setup": {},
        "benchmark": {},
        "infastructure": benchmark_infra
    }
    print("Checking required inputs are in place...")
    benchmark_inputs_dict = benchmark_config["inputs"]
    run_stages_inputs = check_and_get_inputs(benchmark_inputs_dict, local_path, run_stages)
    aux_client = None

    print("Checking required modules available at {}...".format(args.redis_url))
    key_configs_git_sha = None
    key_configs_version = None
    server_info = None
    redisearch_benchmark = True if "ft" in deployment_requirements["redis-server"]["modules"] else False
    if redisearch_benchmark:
        key_configs_git_sha, key_configs_version, server_info = check_and_extract_redisearch_info(args.redis_url)

    key_configs = {"deployment-type": deployment_type,
                                            "deployment-shards": args.deployment_shards,
                                            "version": key_configs_version, "git_sha": key_configs_git_sha}

    db_machine_1 = {"machine_info": None, "redis_info": server_info}
    benchmark_infra["db-machines"]["db-machine-1"] = db_machine_1
    benchmark_infra["total-db-machines"] += 1
    benchmark_output_dict["key-configs"] = key_configs

    ###############################
    # Go client stage starts here #
    ###############################
    options = get_run_options()
    start_time = dt.datetime.now()
    benchmark_repetitions_require_teardown = benchmark_config["benchmark"][
        "repetitions-require-teardown-and-re-setup"]
    total_steps = args.repetitions + len(run_stages) -1
    if benchmark_repetitions_require_teardown is True:
        total_steps += total_steps
    progress = tqdm(unit="bench steps", total=total_steps)
    for repetition in range(1, args.repetitions + 1):
        if benchmark_repetitions_require_teardown is True or repetition == 1:
            aux_client = run_setup_commands(args, "setup", benchmark_config["setup"]["commands"], oss_cluster_mode)
            if "setup" in run_stages_inputs:
                setup_run_key = "setup-run-{}.json".format(repetition)
                setup_run_json_output_fullpath = "{}/{}".format(local_path, setup_run_key)
                input_file = run_stages_inputs["setup"]
                benchmark_output_dict["setup"][setup_run_key] = run_ftsb_redisearch(args.redis_url, benchmark_tool_path,
                                                                                    setup_run_json_output_fullpath,
                                                                                    options, input_file, workers,
                                                                                    pipeline, oss_cluster_mode, max_rps, requests)
            progress.update()

        ######################
        # Benchmark commands #
        ######################
        benchmark_run_key = "benchmark-run-{}.json".format(repetition)
        benchmark_run_json_output_fullpath = "{}/{}".format(local_path, benchmark_run_key)
        input_file = run_stages_inputs["benchmark"]

        benchmark_output_dict["benchmark"][benchmark_run_key] = run_ftsb_redisearch(args.redis_url, benchmark_tool_path,
                                                                                    benchmark_run_json_output_fullpath,
                                                                                    options, input_file, workers,
                                                                                    pipeline, oss_cluster_mode, max_rps, requests)

        progress.update()
    end_time = dt.datetime.now()
    progress.close()
    ##################################
    # Repetitions Results Comparison #
    ##################################
    step_df_dict = generate_comparison_dataframe_configs(benchmark_config, run_stages)

    benchmark_output_dict["results-comparison"] = {}
    for step in run_stages:
        for run_name, result_run in benchmark_output_dict[step].items():
            step_df_dict[step]["df_dict"]["run-name"].append(run_name)
            for pos, metric_json_path in enumerate(step_df_dict[step]["metric_json_path"]):
                metric_name = step_df_dict[step]["sorting_metric_names"][pos]
                metric_value = None
                try:
                    metric_value = findJsonPath(metric_json_path, result_run)
                except KeyError:
                    print(
                        "Error retrieving {} metric from JSON PATH {} on file {}".format(metric_name, metric_json_path,
                                                                                         run_name))
                    pass
                step_df_dict[step]["df_dict"][metric_name].append(metric_value)
        resultsDataFrame = pd.DataFrame(step_df_dict[step]["df_dict"])
        resultsDataFrame.sort_values(step_df_dict[step]["sorting_metric_names"],
                                     ascending=step_df_dict[step]["sorting_metric_sorting_direction"], inplace=True)
        benchmark_output_dict["key-results"][step] = from_resultsDF_to_key_results_dict(resultsDataFrame, step,
                                                                                        step_df_dict)
    #####################
    # Run Info Metadata #
    #####################
    run_info, start_time_str = prepare_run_info_metadata_dict(end_time, start_time)
    benchmark_output_dict["run-info"] = run_info
    benchmark_output_filename = get_run_ftsb_redisearch_full_filename(args, deployment_type, key_configs_git_sha,
                                                                      key_configs_version, start_time_str, test_name)
    with open(benchmark_output_filename, "w") as json_out_file:
        json.dump(benchmark_output_dict, json_out_file, indent=2)

    if args.upload_results_s3:
        artifacts = [benchmark_output_filename]
        upload_artifacts_to_s3(artifacts, s3_bucket_name, s3_bucket_path)

def run_setup_commands(args, step_string_description, commands, cluster_enabled):
    print("Running {} steps...".format(step_string_description))
    try:
        if cluster_enabled:
            host_port_arr = args.redis_url.split(":")
            host = host_port_arr[0]
            port = host_port_arr[1]
            startup_nodes = [{"host": host, "port": port}]
            aux_client = RedisCluster(startup_nodes=startup_nodes, decode_responses=True)
            cluster_nodes = aux_client.cluster_nodes()
            for command in commands:
                for master_node in aux_client.connection_pool.nodes.all_masters():
                    redis.from_url("redis://"+master_node["name"]).execute_command(" ".join(command))
        else:
            aux_client = redis.from_url(args.redis_url)
            for command in commands:
                aux_client.execute_command(" ".join(command))
    except redis.connection.ConnectionError as e:
        print('Error while issuing {} steps command to Redis.Command {}! Error message: {} Exiting..'.format(step_string_description, command,
                                                                                                          e.__str__()))
        sys.exit(1)
    return aux_client


def prepare_run_info_metadata_dict(end_time, start_time):
    start_time_str = start_time.strftime("%Y-%m-%d-%H-%M-%S")
    end_time_str = end_time.strftime("%Y-%m-%d-%H-%M-%S")
    duration_ms = ts_milli(end_time) - ts_milli(start_time)
    start_time_ms = ts_milli(start_time)
    end_time_ms = ts_milli(end_time)
    duration_humanized = humanize.naturaldelta((end_time - start_time))
    run_info = {"start-time-ms": start_time_ms, "start-time-humanized": start_time_str, "end-time-ms": end_time_ms,
                "end-time-humanized": end_time_str, "duration-ms": duration_ms,
                "duration-humanized": duration_humanized}
    return run_info, start_time_str


def check_and_get_inputs(benchmark_inputs_dict, local_path, run_stages):
    run_stages_inputs = {}
    for stage, input_description in benchmark_inputs_dict.items():
        remote_url = input_description["remote-url"]
        local_uncompressed_filename = input_description["local-uncompressed-filename"]
        local_compressed_filename = input_description["local-compressed-filename"]
        local_uncompressed_filename_path = "{}/{}".format(local_path, local_uncompressed_filename)
        local_compressed_filename_path = "{}/{}".format(local_path, local_compressed_filename)
        local_uncompressed_exists = os.path.isfile(local_uncompressed_filename_path)
        local_compressed_exists = os.path.isfile(local_compressed_filename_path)
        if stage in run_stages:
            # if the local uncompressed file exists dont need to do work
            if local_uncompressed_exists:
                print(
                    "\tLocal uncompressed file {} exists at {}. Nothing to do here".format(local_uncompressed_filename,
                                                                                           local_uncompressed_filename_path))
            # if the local compressed file exists then we need to uncompress it
            elif local_compressed_exists:
                print("\tLocal compressed file {} exists at {}. Uncompressing it".format(local_uncompressed_filename,
                                                                                         local_uncompressed_filename_path))
                decompress_file(local_compressed_filename_path, local_uncompressed_filename_path)

            elif remote_url is not None:
                print("\tRetrieving {} and saving to {}".format(remote_url, local_compressed_filename_path))
                r = requests.get(remote_url)
                open(local_compressed_filename_path, 'wb').write(r.content)
                decompress_file(local_compressed_filename_path, local_uncompressed_filename_path)

            else:
                print('\tFor stage {}, unable to retrieve required file {}! Exiting..'.format(stage,
                                                                                              local_uncompressed_filename))
                sys.exit(1)

            run_stages_inputs[stage] = local_uncompressed_filename_path

    return run_stages_inputs


def get_run_ftsb_redisearch_full_filename(args, deployment_type, redisearch_git_sha, redisearch_version, start_time_str,
                                          test_name):
    benchmark_output_filename = "{prefix}{time_str}-{deployment_type}-{use_case}-{version}-{git_sha}.json".format(
        prefix=args.output_file_prefix,
        time_str=start_time_str, deployment_type=deployment_type, use_case=test_name,
        version=redisearch_version, git_sha=redisearch_git_sha)
    return benchmark_output_filename
