#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import logging

import requests

from redisbench_admin.utils.remote import extract_git_vars

PERF_CALLGRAPH_MODE_DEFAULT = "fp"
PERF_DAEMON_LOGNAME = "/tmp/perf-daemon.log"


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
        self.aws_access_key_id = None
        self.aws_secret_access_key = None
        self.aws_session_token = None
        self.region_name = None

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
        if "aws_access_key_id" in kwargs:
            self.aws_access_key_id = kwargs["aws_access_key_id"]
        if "aws_secret_access_key" in kwargs:
            self.aws_secret_access_key = kwargs["aws_secret_access_key"]
        if "aws_session_token" in kwargs:
            self.aws_session_token = kwargs["aws_session_token"]
        if "region_name" in kwargs:
            self.region_name = kwargs["region_name"]

    def start_profile(self, pid, output="", frequency=99, call_graph_mode="fp"):
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
            "github_actor": self.github_actor,
            "github_branch": self.github_branch,
            "github_repo_name": self.github_repo_name,
            "github_org_name": self.github_org_name,
            "github_sha": self.github_sha,
        }
        url = "http://{}/profiler/perf/start/{}?frequency={}&callgraph_mode={}".format(
            self.remote_endpoint, pid, frequency, call_graph_mode
        )

        response = requests.post(url, data=None, json=data)
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
            data = {}
            if self.aws_access_key_id is not None:
                logging.info("Sending aws_access_key_id stop request")
                data["aws_access_key_id"] = self.aws_access_key_id
            if self.aws_secret_access_key is not None:
                logging.info("Sending aws_secret_access_key stop request")
                data["aws_secret_access_key"] = self.aws_secret_access_key
            if self.aws_session_token is not None:
                logging.info("Sending aws_session_token stop request")
                data["aws_session_token"] = self.aws_session_token
            if self.region_name is not None:
                logging.info("Sending region_name stop request")
                data["region_name"] = self.region_name

            response = requests.post(url, data=None, json=data)
            if response.status_code == 200:
                result = True
                status_dict = response.json()
                self.outputs = status_dict["profiler_artifacts"]
            else:
                logging.error(
                    "Remote profiler status_code {} != 200. Message: {}".format(
                        response.status_code, response.content
                    )
                )

        return result

    def generate_outputs(self, use_case, **kwargs):
        return self.result, self.outputs, self.tabular_data_map
