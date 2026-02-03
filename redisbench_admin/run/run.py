#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import logging
import copy
from collections import defaultdict

from pytablewriter import MarkdownTableWriter

from redisbench_admin.run.common import extract_test_feasible_setups
from redisbench_admin.run_remote.consts import min_recommended_benchmark_duration
from redisbench_admin.utils.benchmark_config import (
    extract_benchmark_type_from_config,
    extract_redis_dbconfig_parameters,
    extract_client_dataset_name,
)


class EnvironmentTracker:
    """
    Tracks environment creation and reuse across benchmark runs.
    Used to generate a summary table at the end of all runs.
    """

    def __init__(self):
        # Key: (dataset_name, setup_name) -> dict with env info
        self.environments = {}
        # List of (test_name, dataset_name, setup_name, action) tuples
        self.benchmark_actions = []

    def record_env_created(self, dataset_name, setup_name, test_name, redis_pids=None):
        """Record that a new environment was created for this benchmark."""
        env_key = (dataset_name, setup_name)
        self.environments[env_key] = {
            "created_by": test_name,
            "reused_by": [],
            "redis_pids": redis_pids or [],
        }
        self.benchmark_actions.append((test_name, dataset_name, setup_name, "created"))

    def record_env_reused(self, dataset_name, setup_name, test_name, pids_match=True):
        """Record that an existing environment was reused for this benchmark."""
        env_key = (dataset_name, setup_name)
        if env_key in self.environments:
            self.environments[env_key]["reused_by"].append(test_name)
        action = "reused" if pids_match else "reused (PID mismatch!)"
        self.benchmark_actions.append((test_name, dataset_name, setup_name, action))

    def log_summary_table(self):
        """Log a summary table of all environments and their reuse."""
        logging.info("")
        logging.info("=" * 80)
        logging.info("ENVIRONMENT REUSE SUMMARY")
        logging.info("=" * 80)

        # Summary statistics
        total_envs = len(self.environments)
        total_reuses = sum(len(env["reused_by"]) for env in self.environments.values())
        total_benchmarks = len(self.benchmark_actions)

        logging.info(f"Total environments created: {total_envs}")
        logging.info(f"Total environment reuses: {total_reuses}")
        logging.info(f"Total benchmarks executed: {total_benchmarks}")
        logging.info("")

        # Environment details table
        if self.environments:
            table_headers = [
                "Dataset",
                "Setup",
                "Created By",
                "Reused By",
                "Reuse Count",
            ]
            table_rows = []

            for (dataset_name, setup_name), env_info in self.environments.items():
                reused_by_str = (
                    ", ".join(env_info["reused_by"]) if env_info["reused_by"] else "-"
                )
                table_rows.append(
                    [
                        dataset_name,
                        setup_name,
                        env_info["created_by"],
                        reused_by_str,
                        len(env_info["reused_by"]),
                    ]
                )

            writer = MarkdownTableWriter(
                table_name="Environment Details",
                headers=table_headers,
                value_matrix=table_rows,
            )
            table_str = writer.dumps()
            for line in table_str.split("\n"):
                if line.strip():
                    logging.info(line)

        logging.info("")

        # Benchmark execution log table
        if self.benchmark_actions:
            table_headers = ["Test Name", "Dataset", "Setup", "Action"]
            table_rows = []

            for test_name, dataset_name, setup_name, action in self.benchmark_actions:
                # Add visual indicator
                if action == "created":
                    action_str = "🆕 Created new environment"
                elif action == "reused":
                    action_str = "🟢 Reused environment"
                else:
                    action_str = f"🔴 {action}"
                table_rows.append([test_name, dataset_name, setup_name, action_str])

            writer = MarkdownTableWriter(
                table_name="Benchmark Execution Log",
                headers=table_headers,
                value_matrix=table_rows,
            )
            table_str = writer.dumps()
            for line in table_str.split("\n"):
                if line.strip():
                    logging.info(line)

        logging.info("=" * 80)


def ensure_mixed_types_first(benchmark_runs_plan):
    """
    Returns an iterator over (benchmark_type, bench_by_dataset_map) tuples,
    ensuring that 'mixed' benchmark types are processed before others.

    This ensures that load benchmarks (which produce datasets) run before
    query benchmarks (which consume datasets).
    """
    return sorted(benchmark_runs_plan.items(), key=lambda x: (x[0] != "mixed", x[0]))


def log_benchmark_plan_table(benchmark_runs_plan):
    """
    Log the benchmark execution plan as a formatted table.
    Shows the order in which benchmarks will be executed.
    """
    table_headers = ["Order", "Benchmark Type", "Dataset Name", "Setup", "Test Name"]
    table_rows = []
    order = 1

    for benchmark_type, bench_by_dataset_map in ensure_mixed_types_first(
        benchmark_runs_plan
    ):
        for (
            dataset_name,
            bench_by_dataset_and_setup_map,
        ) in bench_by_dataset_map.items():
            for setup_name, setup_details in bench_by_dataset_and_setup_map.items():
                benchmarks_map = setup_details["benchmarks"]
                for test_name in benchmarks_map.keys():
                    table_rows.append(
                        [order, benchmark_type, dataset_name, setup_name, test_name]
                    )
                    order += 1

    writer = MarkdownTableWriter(
        table_name="Benchmark Execution Plan",
        headers=table_headers,
        value_matrix=table_rows,
    )
    # Use logging to ensure the table appears in log output
    table_str = writer.dumps()
    for line in table_str.split("\n"):
        if line.strip():
            logging.info(line)


def calculate_client_tool_duration_and_check(
    benchmark_end_time, benchmark_start_time, step_name="Benchmark", warn_min=True
):
    benchmark_duration_seconds = (benchmark_end_time - benchmark_start_time).seconds
    logging.info("{} duration {} secs.".format(step_name, benchmark_duration_seconds))
    if benchmark_duration_seconds < min_recommended_benchmark_duration and warn_min:
        logging.warning(
            "{} duration of {} secs is bellow the considered"
            " minimum duration for a stable run ({} secs).".format(
                step_name,
                benchmark_duration_seconds,
                min_recommended_benchmark_duration,
            )
        )
    return benchmark_duration_seconds


def merge_dicts(dict1, dict2):
    result = copy.deepcopy(dict1)  # Start with dict1's keys and values
    for key, value in dict2.items():
        if key in result:
            if isinstance(result[key], dict) and isinstance(value, dict):
                # Recursively merge nested dictionaries
                result[key] = merge_dicts(result[key], value)
            # If it's not a dict, we keep the value from dict1 (result)
        else:
            result[key] = value
    print(f"merging dict1 {dict1} with with dict2 {dict2}. final {result}")
    return result


def define_benchmark_plan(benchmark_definitions, default_specs):
    benchmark_runs_plan = {}
    for test_name, benchmark_config in benchmark_definitions.items():
        # extract benchmark-type
        _, benchmark_type = extract_benchmark_type_from_config(benchmark_config)
        logging.info(
            "Using benchmark type: {} for test {}".format(benchmark_type, test_name)
        )
        if benchmark_type not in benchmark_runs_plan:
            benchmark_runs_plan[benchmark_type] = {}

        # extract dataset-name from dbconfig first
        (
            benchmark_contains_dbconfig,
            dataset_name,
            _,
            _,
            _,
        ) = extract_redis_dbconfig_parameters(benchmark_config, "dbconfig")
        logging.info(
            f"Benchmark contains specific dbconfig on test {test_name}: {benchmark_contains_dbconfig}"
        )

        # If no dataset_name in dbconfig, check clientconfig
        # This allows benchmarks to reuse datasets from other benchmarks
        if dataset_name is None:
            dataset_name = extract_client_dataset_name(benchmark_config, "clientconfig")
            if dataset_name is not None:
                logging.info(
                    f"Found dataset_name '{dataset_name}' in clientconfig for test {test_name}"
                )

        if dataset_name is None:
            dataset_name = test_name
            logging.info(
                "Given no dataset name was found on the db config or client config, using test name as key for unique dataset reference: {}".format(
                    test_name
                )
            )
        if dataset_name not in benchmark_runs_plan[benchmark_type]:
            benchmark_runs_plan[benchmark_type][dataset_name] = {}

        test_setups = extract_test_feasible_setups(
            benchmark_config, "setups", default_specs
        )

        for setup_name, setup_settings in test_setups.items():
            test_benchmark_config = copy.deepcopy(benchmark_config)
            setup_contains_dbconfig = False
            if "dbconfig" in setup_settings:
                setup_contains_dbconfig = True
            logging.debug(
                f"setup ({setup_name}): {setup_settings}. contains dbconfig {setup_contains_dbconfig}"
            )

            if setup_name not in benchmark_runs_plan[benchmark_type][dataset_name]:
                benchmark_runs_plan[benchmark_type][dataset_name][setup_name] = {}
                benchmark_runs_plan[benchmark_type][dataset_name][setup_name][
                    "setup_settings"
                ] = setup_settings
                benchmark_runs_plan[benchmark_type][dataset_name][setup_name][
                    "benchmarks"
                ] = {}

            if (
                test_name
                in benchmark_runs_plan[benchmark_type][dataset_name][setup_name][
                    "benchmarks"
                ]
            ):
                raise Exception(
                    "Test named: {} was already present in benchmark definition".format(
                        test_name
                    )
                )
            else:
                # check if we need to merge dbconfigs from the setup defaults
                if setup_contains_dbconfig:
                    if "dbconfig" not in test_benchmark_config:
                        test_benchmark_config["dbconfig"] = {}
                    setup_dbconfig = setup_settings["dbconfig"]
                    benchmark_dbconfig = test_benchmark_config["dbconfig"]

                    # Handle legacy list format - convert to dict by merging all entries
                    if isinstance(benchmark_dbconfig, list):
                        converted_dbconfig = {}
                        for entry in benchmark_dbconfig:
                            if isinstance(entry, dict):
                                converted_dbconfig.update(entry)
                        benchmark_dbconfig = converted_dbconfig

                    logging.info(
                        f"Merging setup dbconfig: {setup_dbconfig}, with benchmark dbconfig {test_benchmark_config}"
                    )
                    final_db_config = merge_dicts(benchmark_dbconfig, setup_dbconfig)
                    logging.info(f"FINAL DB CONFIG: {final_db_config}")
                    test_benchmark_config["dbconfig"] = final_db_config

                logging.debug(
                    f"final benchmark config for setup: {setup_name} and test: {test_name}. {test_benchmark_config}"
                )
                # add benchmark
                benchmark_runs_plan[benchmark_type][dataset_name][setup_name][
                    "benchmarks"
                ][test_name] = test_benchmark_config

    return benchmark_runs_plan
