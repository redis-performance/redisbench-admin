#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#

import argparse
import json

# !/usr/bin/env python3
import logging
import os
import sys

import botocore
import daemonize
from flask import Flask, request

from redisbench_admin.cli import populate_with_poetry_data
from redisbench_admin.profilers.perf import Perf
from redisbench_admin.profilers.perf_daemon_caller import PERF_DAEMON_LOGNAME
from redisbench_admin.profilers.profilers_local import local_profilers_platform_checks
from redisbench_admin.run.args import S3_BUCKET_NAME
from redisbench_admin.run.common import get_start_time_vars
from redisbench_admin.run.s3 import get_test_s3_bucket_path
from redisbench_admin.utils.remote import extract_git_vars
from redisbench_admin.utils.utils import upload_artifacts_to_s3

PID_FILE = "/tmp/perfdaemon.pid"
DEFAULT_PROFILE_FREQ = 99
LOG_LEVEL = logging.DEBUG
if os.getenv("VERBOSE", "0") == "0":
    LOG_LEVEL = logging.INFO
LOG_FORMAT = "%(asctime)s %(levelname)-4s %(message)s"
LOG_DATEFMT = "%Y-%m-%d %H:%M:%S"

app = Flask(__name__)
app.use_reloader = False


class PerfDaemon:
    def __init__(self, user=None, group=None):
        self.user = user
        self.group = group
        self.perf = Perf()
        self.set_app_loggers(app)
        self.create_app_endpoints(app)

    def main(self):
        self.set_app_loggers(app)
        app.run(host="0.0.0.0", debug=False, port=5000)

    def set_app_loggers(self, app):
        print("Writting log to {}".format(PERF_DAEMON_LOGNAME))
        handler = logging.handlers.RotatingFileHandler(
            PERF_DAEMON_LOGNAME, maxBytes=1024 * 1024
        )
        logging.getLogger("werkzeug").setLevel(logging.DEBUG)
        logging.getLogger("werkzeug").addHandler(handler)
        app.logger.setLevel(LOG_LEVEL)
        app.logger.addHandler(handler)
        self.perf.set_logger(app.logger)

    def update_ec2_vars_from_request(self, request, app):
        aws_access_key_id = None
        aws_secret_access_key = None
        aws_session_token = None
        region_name = "us-east-2"
        if request.is_json:
            data = request.get_json()
            if "aws_access_key_id" in data:
                app.logger.info("detected aws_access_key_id in request")
                aws_access_key_id = data["aws_access_key_id"]
            if "aws_secret_access_key" in data:
                app.logger.info("detected aws_secret_access_key in request")
                aws_secret_access_key = data["aws_secret_access_key"]
            if "aws_session_token" in data:
                app.logger.info("detected aws_session_token in request")
                aws_session_token = data["aws_session_token"]
            if "region_name" in data:
                app.logger.info("detected region_name in request")
                region_name = data["region_name"]

        if aws_access_key_id is not None:
            app.logger.info("aws_access_key_id is properly set")
        if aws_secret_access_key is not None:
            app.logger.info("aws_secret_access_key is properly set")
        if aws_session_token is not None:
            app.logger.info("aws_session_token is properly set")

        return aws_access_key_id, aws_secret_access_key, aws_session_token, region_name

    def update_vars_from_request(self, request, app):
        app.logger.info("Updating vars from request")
        self.dso = ""
        self.test_name = ""
        self.setup_name = ""
        if request.is_json:
            data = request.get_json()
            app.logger.info("Received the JSON payload {}".format(data))
            if "dso" in data:
                self.dso = data["dso"]
            if "test_name" in data:
                self.test_name = data["test_name"]
            if "setup_name" in data:
                self.setup_name = data["setup_name"]
            if "github_actor" in data:
                self.github_actor = data["github_actor"]
            if "github_branch" in data:
                self.github_branch = data["github_branch"]
            if "github_repo_name" in data:
                self.github_repo_name = data["github_repo_name"]
            if "github_org_name" in data:
                self.github_org_name = data["github_org_name"]
            if "github_sha" in data:
                self.github_sha = data["github_sha"]

    def create_app_endpoints(self, app):
        @app.before_first_request
        def before_first_request():
            app.logger.setLevel(logging.INFO)

        @app.get("/ping")
        def ping():
            return json.dumps({"result": True})

        @app.route("/profiler/<profiler_name>/start/<pid>", methods=["POST"])
        def profile_start(profiler_name, pid):
            callgraph_mode = request.args.get("callgraph_mode", default="fp", type=str)
            setup_process_number = 1
            total_involved_processes = 1
            (
                start_time,
                start_time_ms,
                start_time_str,
            ) = get_start_time_vars()
            (
                self.github_org_name,
                self.github_repo_name,
                self.github_sha,
                self.github_actor,
                self.github_branch,
                github_branch_detached,
            ) = extract_git_vars()
            self.update_vars_from_request(request, app)

            self.collection_summary_str = local_profilers_platform_checks(
                self.dso,
                self.github_actor,
                self.github_branch,
                self.github_repo_name,
                self.github_sha,
            )
            msg = "Starting profiler {} for Process {} of {}: pid {}".format(
                profiler_name,
                setup_process_number,
                total_involved_processes,
                pid,
            )
            app.logger.info(msg)
            profile_filename = (
                "profile_{setup_name}".format(
                    setup_name=self.setup_name,
                )
                + "__primary-{primary_n}-of-{total_primaries}".format(
                    primary_n=setup_process_number,
                    total_primaries=total_involved_processes,
                )
                + "__{test_name}_{profile}_{start_time_str}.out".format(
                    profile=profiler_name,
                    test_name=self.test_name,
                    start_time_str=start_time_str,
                )
            )
            app.logger.info("Storing profile in {}".format(profile_filename))
            result = self.perf.start_profile(
                pid, profile_filename, DEFAULT_PROFILE_FREQ, callgraph_mode
            )
            status_dict = {
                "result": result,
                "message": msg,
                "start_time": start_time_str,
            }
            return json.dumps(status_dict)

        @app.get("/profiler/<profiler_name>/status/<pid>")
        def profile_status(profiler_name, pid):
            _is_alive = self.perf._is_alive(self.perf.profiler_process)
            status_dict = {"running": _is_alive}
            return json.dumps(status_dict)

        @app.post("/profiler/<profiler_name>/stop/<pid>")
        def profile_stop(profiler_name, pid):
            profile_res = self.perf.stop_profile()
            profilers_artifacts_matrix = []

            primary_id = 1
            total_primaries = 1
            (
                aws_access_key_id,
                aws_secret_access_key,
                aws_session_token,
                region_name,
            ) = self.update_ec2_vars_from_request(request, app)
            if profile_res is True:
                # Generate:
                #  - artifact with Flame Graph SVG
                #  - artifact with output graph image in PNG format
                #  - artifact with top entries in text form
                (
                    profile_res,
                    profile_res_artifacts_map,
                    tabular_data_map,
                ) = self.perf.generate_outputs(
                    self.test_name,
                    details=self.collection_summary_str,
                    binary=self.dso,
                    primary_id=primary_id,
                    total_primaries=total_primaries,
                )
                summary_msg = "Profiler {} for pid {} ran successfully and generated {} artifacts. Generated also {} tables with data(keys:{}).".format(
                    profiler_name,
                    self.perf.profiler_process.pid,
                    len(profile_res_artifacts_map.values()),
                    len(tabular_data_map.values()),
                    ",".join(tabular_data_map.keys()),
                )
                if profile_res is True:
                    app.logger.info(summary_msg)
            s3_bucket_path = get_test_s3_bucket_path(
                S3_BUCKET_NAME,
                self.test_name,
                self.github_org_name,
                self.github_repo_name,
                "profiles",
            )
            try:
                for (
                    artifact_name,
                    profile_artifact,
                ) in profile_res_artifacts_map.items():
                    s3_link = None
                    upload_results_s3 = True
                    if upload_results_s3:
                        app.logger.info(
                            "Uploading results to s3. s3 bucket name: {}. s3 bucket path: {}".format(
                                S3_BUCKET_NAME, s3_bucket_path
                            )
                        )
                        url_map = upload_artifacts_to_s3(
                            [profile_artifact],
                            S3_BUCKET_NAME,
                            s3_bucket_path,
                            aws_access_key_id,
                            aws_secret_access_key,
                            aws_session_token,
                            region_name,
                        )
                        s3_link = list(url_map.values())[0]
                    profilers_artifacts_matrix.append(
                        {
                            "test_name": self.test_name,
                            "profiler_name": profiler_name,
                            "artifact_name": artifact_name,
                            "s3_link": s3_link,
                        }
                    )
            except botocore.exceptions.NoCredentialsError:
                profile_res = False
                summary_msg = (
                    "Unable to push profile artifacts to s3. Missing credentials."
                )
                app.logger.error(summary_msg)

            status_dict = {
                "result": profile_res,
                "summary": summary_msg,
                "profiler_artifacts": profilers_artifacts_matrix,
            }
            self.perf = Perf()
            return json.dumps(status_dict)


def main():
    _, project_description, project_version = populate_with_poetry_data()
    project_name = "perf-daemon"
    parser = argparse.ArgumentParser(
        description=project_description,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    if len(sys.argv) < 2:
        print(
            "A minimum of 2 arguments is required: perf-daemon <mode> <arguments>."
            " Use perf-daemon --help if you need further assistance."
        )
        sys.exit(1)
    requested_tool = sys.argv[1]
    # common arguments to all tools
    parser.add_argument("--user", default=None)
    parser.add_argument("--group", default=None)
    argv = sys.argv[2:]
    args = parser.parse_args(args=argv)
    d = PerfDaemon(args.user, args.group)

    def start(foreground=False):
        current_path = os.path.abspath(os.getcwd())
        print(
            "Starting {}. PID file {}. Daemon workdir: {}".format(
                project_name, PID_FILE, current_path
            )
        )
        if d.user is not None:
            print("Specifying user {}. group {}.".format(d.user, d.group))
        daemonize.Daemonize(
            app=project_name,
            pid=PID_FILE,
            action=d.main,
            chdir=current_path,
            foreground=foreground,
            user=d.user,
            group=d.group,
        ).start()

    def stop():
        if not os.path.exists(PID_FILE):
            sys.exit(0)
        with open(PID_FILE, "r") as pidfile:
            pid = pidfile.read()
        os.system("kill -9 %s" % pid)

    def foreground():
        start(True)

    def usage():
        print("usage: start|stop|restart|foreground")
        sys.exit(1)

    if requested_tool == "start":
        start()
    elif requested_tool == "stop":
        stop()
    elif requested_tool == "restart":
        stop()
        start()
    elif requested_tool == "foreground":
        foreground()
    else:
        usage()


if __name__ == "__main__":
    main()
