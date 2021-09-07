#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#

import logging
import os.path
import subprocess
import time


class Vtune:
    def __init__(self):
        """
        Profiling on top of intel's vtune
        """
        self.vtune = os.getenv("VTUNE")
        if not self.vtune:
            self.vtune = "vtune"

        self.output = None
        self.profiler_process = None
        self.profiler_process_stdout = None
        self.profiler_process_stderr = None
        self.profiler_process_exit_code = None
        self.trace_file = None
        self.pid = None
        self.started_profile = False
        self.environ = os.environ.copy()
        self.retrieve_vtune_version()
        self.profile_start_time = None
        self.profile_end_time = None

    def retrieve_vtune_version(self):
        try:
            self.version = subprocess.Popen(
                [self.vtune, "--version"], stdout=subprocess.PIPE
            ).communicate()[0]
        except OSError:
            raise Exception("Unable to run vtune %{}".format(self.vtune))

    def generate_collect_command(self, pid, output, analysis_type="hotspots"):
        self.output = output
        self.pid = pid
        cmd = [
            self.vtune,
            "-collect",
            analysis_type,
            "-target-pid",
            "{}".format(pid),
            "-r",
            output,
        ]
        return cmd

    def start_profile(self, pid, output, frequency=99):
        """
        @param pid: profile events on specified process id
        @param output: output file name
        @param frequency: profile at this frequency
        @return: returns True if profiler started, False if unsuccessful
        """
        result = False
        self.profile_start_time = time.time()

        # profiler is already running
        if not self.started_profile:
            stderrPipe = subprocess.PIPE
            stdoutPipe = subprocess.PIPE
            stdinPipe = subprocess.PIPE

            options = {
                "stderr": stderrPipe,
                "stdin": stdinPipe,
                "stdout": stdoutPipe,
                "env": self.environ,
            }

            args = self.generate_collect_command(pid, output, "hotspots")
            logging.info("Starting profile of pid {} with args {}".format(pid, args))
            self.profiler_process = subprocess.Popen(args=args, **options)
            self.started_profile = True
            result = True
        return result

    def _is_alive(self, process):
        """
        @param process:
        @return: returns True if specified process is running, False if not running
        """
        if not process:
            return False
        # Check if child process has terminated. Set and return returncode
        # attribute
        if process.poll() is None:
            return True
        return False

    def stop_profile(self, **kwargs):
        """
        @return: returns True if profiler stop, False if unsuccessful
        """
        result = False
        self.profile_end_time = time.time()
        if not self._is_alive(self.profiler_process):
            logging.error(
                "Profiler process is not alive, might have crash during test execution, "
            )
            return result
        try:
            self.profiler_process.terminate()
            self.profiler_process.wait()
            # (
            #     self.profiler_process_stdout,
            #     self.profiler_process_stderr,
            # ) = self.profiler_process.communicate()
            self.profiler_process_exit_code = self.profiler_process.poll()
            if self.profiler_process_exit_code <= 0:
                logging.info("Generating trace file from profile.")
            else:
                logging.error(
                    "Profiler process exit with error. Exit code: {}\n\n".format(
                        self.profiler_process_exit_code
                    )
                )

        except OSError as e:
            logging.error(
                "OSError caught while waiting for profiler process to end: {0}".format(
                    e.__str__()
                )
            )
            result = False
            pass
        return result

    def generate_outputs(self, use_case, **kwargs):
        outputs = {}
        tabular_data_map = {}
        # binary = kwargs.get("binary")
        # details = kwargs.get("details")
        # if details is None:
        #     details = ""
        result = True

        return result, outputs, tabular_data_map
