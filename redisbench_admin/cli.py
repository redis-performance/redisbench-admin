#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#

import argparse
import logging
import os
import sys

import toml

from redisbench_admin import __version__
from redisbench_admin.export.args import create_export_arguments
from redisbench_admin.export.export import export_command_logic
from redisbench_admin.extract.args import create_extract_arguments
from redisbench_admin.extract.extract import extract_command_logic
from redisbench_admin.run_local.args import create_run_local_arguments
from redisbench_admin.run_local.run_local import run_local_command_logic
from redisbench_admin.run_remote.args import create_run_remote_arguments
from redisbench_admin.run_remote.run_remote import run_remote_command_logic
from redisbench_admin.watchdog.args import create_watchdog_arguments
from redisbench_admin.watchdog.watchdog import watchdog_command_logic


LOG_LEVEL = logging.INFO
if os.getenv("VERBOSE", "1") == "0":
    LOG_LEVEL = logging.WARN
LOG_FORMAT = "%(asctime)s %(levelname)-4s %(message)s"
LOG_DATEFMT = "%Y-%m-%d %H:%M:%S"


def populate_with_poetry_data():
    project_name = "redisbench-admin"
    project_version = __version__
    project_description = None
    try:
        poetry_data = toml.load("pyproject.toml")["tool"]["poetry"]
        project_name = poetry_data["name"]
        project_version = poetry_data["version"]
        project_description = poetry_data["description"]
    except FileNotFoundError:
        pass

    return project_name, project_description, project_version


def main():
    if len(sys.argv) < 2:
        print(
            "A minimum of 2 arguments is required: redisbench-admin <tool> <arguments>."
            " Use redisbench-admin --help if you need further assistance."
        )
        sys.exit(1)
    requested_tool = sys.argv[1]
    project_name, project_description, project_version = populate_with_poetry_data()
    parser = argparse.ArgumentParser(
        description=project_description,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    # common arguments to all tools
    parser.add_argument(
        "--version", default=False, action="store_true", help="print version and exit"
    )
    parser.add_argument(
        "--local-dir", type=str, default="./", help="local dir to use as storage"
    )
    parser.add_argument(
        "--logname", type=str, default=None, help="logname to write the logs to"
    )

    if requested_tool == "run-remote":
        parser = create_run_remote_arguments(parser)
    elif requested_tool == "run-local":
        parser = create_run_local_arguments(parser)
    elif requested_tool == "extract":
        parser = create_extract_arguments(parser)
    elif requested_tool == "export":
        parser = create_export_arguments(parser)
    elif requested_tool == "watchdog":
        parser = create_watchdog_arguments(parser)
    elif requested_tool == "--version":
        print_version(project_name, project_version)
        sys.exit(0)
    elif requested_tool == "--help":
        print_help(project_name, project_version)
        sys.exit(0)
    else:
        valid_tool_options = [
            "run-local",
            "run-remote",
            "export",
            "extract",
            "watchdog",
        ]
        print_invalid_tool_option(requested_tool, valid_tool_options)
        sys.exit(1)

    argv = sys.argv[2:]
    args = parser.parse_args(args=argv)

    if args.logname is not None:
        print("Writting log to {}".format(args.logname))
        logging.basicConfig(
            filename=args.logname,
            filemode="a",
            format=LOG_FORMAT,
            datefmt=LOG_DATEFMT,
            level=LOG_LEVEL,
        )
    else:
        # logging settings
        logging.basicConfig(
            format=LOG_FORMAT,
            level=LOG_LEVEL,
            datefmt=LOG_DATEFMT,
        )

    if requested_tool == "run-local":
        run_local_command_logic(args)
    if requested_tool == "run-remote":
        run_remote_command_logic(args)
    if requested_tool == "export":
        export_command_logic(args)
    if requested_tool == "extract":
        extract_command_logic(args)
    if requested_tool == "watchdog":
        watchdog_command_logic(args)


def print_invalid_tool_option(requested_tool, valid_tool_options):
    print(
        "Invalid redisbench-admin <tool>. Requested tool: {}. Available tools: {}".format(
            requested_tool, ",".join(valid_tool_options)
        )
    )


def print_version(project_name, project_version):
    print(
        "{project_name} {project_version}".format(
            project_name=project_name, project_version=project_version
        )
    )


def print_help(project_name, project_version):
    print(
        "{project_name} {project_version}".format(
            project_name=project_name, project_version=project_version
        )
    )
    print("usage: {project_name} <tool> <args>...".format(project_name=project_name))
    print(
        "\t-) To know more on how to run benchmarks: {project_name} run-remote/run-local --help".format(
            project_name=project_name
        )
    )
    print(
        "\t-) To know more on how to compare benchmark results: {project_name} compare --help".format(
            project_name=project_name
        )
    )
    print(
        "\t-) To know more on how to export benchmark results: {project_name} export --help".format(
            project_name=project_name
        )
    )
