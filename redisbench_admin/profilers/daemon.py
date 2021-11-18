#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#

# !/usr/bin/env python3
import logging
from threading import Thread

from flask import Flask, request
import daemonize
from time import sleep
import json
import sys
import os

from redisbench_admin.run.args import S3_BUCKET_NAME
from redisbench_admin.run.s3 import get_test_s3_bucket_path
from redisbench_admin.utils.remote import extract_git_vars

from redisbench_admin.profilers.perf import Perf
from redisbench_admin.run.common import get_start_time_vars

from redisbench_admin.run_local.profile_local import local_profilers_platform_checks
from redisbench_admin.utils.utils import upload_artifacts_to_s3

PID_FILE = "/tmp/perfdaemon.pid"
DEFAULT_PROFILE_FREQ = 99
LOG_LEVEL = logging.DEBUG
if os.getenv("VERBOSE", "0") == "0":
    LOG_LEVEL = logging.INFO
LOG_FORMAT = "%(asctime)s %(levelname)-4s %(message)s"
LOG_DATEFMT = "%Y-%m-%d %H:%M:%S"
LOGNAME = "/tmp/perf-daemon.log"


class PerfDaemon:
    def __init__(self):
        pass

    def main(self):
        app = Flask(__name__)
        app.debug = False
        app.use_reloader = True
        self.perf = Perf()

        print("Writting log to {}".format(LOGNAME))
        handler = logging.handlers.RotatingFileHandler(LOGNAME, maxBytes=1024 * 1024)
        logging.getLogger("werkzeug").setLevel(logging.DEBUG)
        logging.getLogger("werkzeug").addHandler(handler)
        app.logger.setLevel(LOG_LEVEL)
        app.logger.addHandler(handler)
        self.perf.set_logger(app.logger)

        @app.before_first_request
        def before_first_request():
            app.logger.setLevel(logging.INFO)

        @app.route("/profiler/<profiler_name>/start/<pid>", methods=["POST"])
        def profile_start(profiler_name, pid):
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
            self.dso = ""
            self.test_name = ""
            self.setup_name = ""
            if request.is_json:
                data = request.get_json()
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
            result = self.perf.start_profile(
                pid,
                profile_filename,
                DEFAULT_PROFILE_FREQ,
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
            for (
                artifact_name,
                profile_artifact,
            ) in profile_res_artifacts_map.items():
                s3_link = None
                upload_results_s3 = True
                if upload_results_s3:
                    logging.info(
                        "Uploading results to s3. s3 bucket name: {}. s3 bucket path: {}".format(
                            S3_BUCKET_NAME, s3_bucket_path
                        )
                    )
                    url_map = upload_artifacts_to_s3(
                        [profile_artifact],
                        S3_BUCKET_NAME,
                        s3_bucket_path,
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

            status_dict = {
                "result": profile_res,
                "summary": summary_msg,
                "profiler_artifacts": profilers_artifacts_matrix,
            }
            self.perf = Perf()
            return json.dumps(status_dict)

        Thread(target=app.run).start()

        self.loop()

    def loop(self):
        while True:
            sleep(1)


d = PerfDaemon()


def main():
    global stop
    global d

    def start():
        current_path = os.path.abspath(os.getcwd())
        print(
            "Starting perfdaemon. PID file {}. Daemon workdir: {}".format(
                PID_FILE, current_path
            )
        )
        daemonize.Daemonize(
            app="perfdaemon", pid=PID_FILE, action=d.main, chdir=current_path
        ).start()

    def stop():
        if not os.path.exists(PID_FILE):
            sys.exit(0)
        with open(PID_FILE, "r") as pidfile:
            pid = pidfile.read()
        os.system("kill -9 %s" % pid)

    def foreground():
        d.main()

    def usage():
        print("usage: start|stop|restart|foreground")
        sys.exit(1)

    if len(sys.argv) < 2:
        usage()
    if sys.argv[1] == "start":
        start()
    elif sys.argv[1] == "stop":
        stop()
    elif sys.argv[1] == "restart":
        stop()
        start()
    elif sys.argv[1] == "foreground":
        foreground()
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
