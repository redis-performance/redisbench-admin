#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#

import logging
import requests
from redisbench_admin.utils.remote import extract_git_vars

PERF_CALLGRAPH_MODE_DEFAULT = "fp"


class PerfDaemonRemoteCaller:
    def __init__(self, remote_endpoint, **kwargs):
        self.result = False
        self.remote_endpoint = remote_endpoint
        self.logger = logging
        self.outputs = {}
        self.tabular_data_map = {}
        (
            self.github_org_name,
            self.github_repo_name,
            self.github_sha,
            self.github_actor,
            self.github_branch,
            github_branch_detached,
        ) = extract_git_vars()
        self.dso = ""
        self.pid = None
        self.test_name = ""
        self.setup_name = ""
        if "dso" in kwargs:
            self.dso = kwargs.get("dso")
        if "test_name" in kwargs:
            self.test_name = kwargs.get("test_name")
        if "setup_name" in kwargs:
            self.setup_name = kwargs.get("setup_name")
        if "github_actor" in kwargs:
            self.github_actor = kwargs.get("github_actor")
        if "github_branch" in kwargs:
            self.github_branch = kwargs.get("github_branch")
        if "github_repo_name" in kwargs:
            self.github_repo_name = kwargs.get("github_repo_name")
        if "github_org_name" in kwargs:
            self.github_org_name = kwargs.get("github_org_name")
        if "github_sha" in kwargs:
            self.github_sha = kwargs.get("github_sha")

    def set_logger(self, logger_app):
        self.logger = logger_app

    def start_profile(self, pid, output="", frequency=99):
        """
        @param pid: profile events on specified process id
        @param output: output file name
        @param frequency: profile at this frequency
        @return: returns True if profiler started, False if unsuccessful
        """
        self.pid = pid
        data = {
            "dso": self.dso,
            "test_name": self.test_name,
            "setup_name": self.setup_name,
        }
        url = "http://{}/profiler/perf/start/{}?frequency={}".format(
            self.remote_endpoint, pid, frequency
        )

        response = requests.post(url, data)
        if response.status_code == 200:
            self.result = True

        return self.result

    def stop_profile(self, **kwargs):
        """
        @return: returns True if profiler stop, False if unsuccessful
        """
        result = False
        if self.pid is not None:
            url = "http://{}/profiler/perf/stop/{}".format(
                self.remote_endpoint, self.pid
            )

            response = requests.post(url, {})
            if response.status_code == 200:
                result = True
                status_dict = response.json()
                self.outputs = status_dict["profiler_artifacts"]

        return result

    def generate_outputs(self, use_case, **kwargs):

        return self.result, self.outputs, self.tabular_data_map
