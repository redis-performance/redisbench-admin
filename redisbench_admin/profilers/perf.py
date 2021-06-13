#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#

import logging
import os.path
import re
import subprocess
import time


from redisbench_admin.profilers.pprof import (
    PPROF_FORMAT_TEXT,
    run_pprof,
    PPROF_FORMAT_PNG,
)
from redisbench_admin.profilers.profilers import STACKCOLLAPSE_PATH, FLAMEGRAPH_PATH
from redisbench_admin.utils.utils import whereis


PERF_CALLGRAPH_MODE_DEFAULT = "lbr"
LINUX_PERF_SETTINGS_MESSAGE = (
    "If running in non-root user please confirm that you have:\n"
    + " - access to Kernel address maps."
    + " Check if `0` ( disabled ) appears from the output of `cat /proc/sys/kernel/kptr_restrict`\n"
    + '          If not then fix via: `sudo sh -c " echo 0 > /proc/sys/kernel/kptr_restrict"`\n'
    + " - permission to collect stats."
    + " Check if `-1` appears from the output of `cat /proc/sys/kernel/perf_event_paranoid`\n"
    + "          If not then fix via: `sudo sh -c 'echo -1 > /proc/sys/kernel/perf_event_paranoid'`\n"
)


class Perf:
    def __init__(self):
        """
        Profiling on top of perf
        """
        self.minor = 0
        self.perf = os.getenv("PERF")
        if not self.perf:
            self.perf = "perf"

        self.stack_collapser = os.getenv("STACKCOLLAPSE_PATH", STACKCOLLAPSE_PATH)
        self.flamegraph_utity = os.getenv("FLAMEGRAPH_PATH", FLAMEGRAPH_PATH)
        self.callgraph_mode = os.getenv(
            "PERF_CALLGRAPH_MODE", PERF_CALLGRAPH_MODE_DEFAULT
        )

        self.output = None
        self.profiler_process = None
        self.profiler_process_stdout = None
        self.profiler_process_stderr = None
        self.profiler_process_exit_code = None
        self.trace_file = None
        self.collapsed_stack_file = None
        self.stack_collapse_file = None
        self.collapsed_stacks = []
        self.pid = None
        self.started_profile = False
        self.environ = os.environ.copy()

        self.version = ""
        self.version_major = ""
        self.version_minor = ""
        self.retrieve_perf_version()
        self.profile_start_time = None
        self.profile_end_time = None

        self.pprof_bin = whereis("pprof")

    def retrieve_perf_version(self):
        try:
            self.version = subprocess.Popen(
                [self.perf, "--version"], stdout=subprocess.PIPE
            ).communicate()[0]
        except OSError:
            raise Exception("Unable to run perf %{}".format(self.perf))
        m = re.match(r"perf version (\d+)\.(\d+)\.", self.version.decode("utf-8"))
        if m:
            self.version_major = m.group(1)
            self.version_minor = m.group(2)
        return m, self.version_major, self.version_minor

    def generate_record_command(self, pid, output, frequency=None):
        self.output = output
        self.pid = pid
        cmd = [
            self.perf,
            "record",
            "-e",
            "cycles:pp",
            "-g",
            "--pid",
            "{}".format(pid),
            "--output",
            output,
            "--call-graph",
            self.callgraph_mode,
        ]
        if frequency:
            cmd += ["--freq", "{}".format(frequency)]
        return cmd

    def generate_report_command(self, tid, input, dso, percentage_mode):
        cmd = [self.perf, "report"]
        if dso is not None:
            cmd += ["--dso", dso]
        cmd += [
            "--header",
            "--tid",
            "{}".format(tid),
            "--no-children",
            "--stdio",
            "-g",
            "none,1.0,caller,function",
            "--percentage",
            percentage_mode,
            "--input",
            input,
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

            args = self.generate_record_command(pid, output, frequency)
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
            (
                self.profiler_process_stdout,
                self.profiler_process_stderr,
            ) = self.profiler_process.communicate()
            self.profiler_process_exit_code = self.profiler_process.poll()
            if self.profiler_process_exit_code <= 0:
                logging.info("Generating trace file from profile.")
                result = self.generate_trace_file_from_profile()
                if result is True:
                    logging.info("Trace file generation ran OK.")
                    result = self.stack_collapse()
                    if result is True:
                        logging.info("Stack collapsing from trace file ran OK.")
                    else:
                        logging.error(
                            "Something went wrong on stack collapsing from trace file."
                        )
                else:
                    logging.error("Stack collapsing from trace file exit with error.")
            else:
                logging.error(
                    "Profiler process exit with error. Exit code: {}\n\n".format(
                        self.profiler_process_exit_code
                    )
                    + LINUX_PERF_SETTINGS_MESSAGE
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

    def get_profiler_output_file(self):
        """
        @return:  output file name
        """
        return self.output

    def get_profiler_stdout(self):
        """
        @return: returns the stdout output ( bytes ) of the profiler process if we have ran a profiler. If not returns None
        """
        return self.profiler_process_stdout

    def get_profiler_stderr(self):
        """
        @return: returns the stderr output ( bytes ) of the profiler process if we have ran a profiler. If not returns None
        """
        return self.profiler_process_stderr

    def get_trace_file(self):
        return self.trace_file

    def generate_trace_file_from_profile(self, filename=None):
        result = False
        if self.output is not None:
            if os.path.isfile(self.output):
                if filename is None:
                    filename = self.output + ".script"
                with open(filename, "w") as outfile:
                    args = [self.perf, "script", "-i", self.output]
                    try:
                        subprocess.Popen(args=args, stdout=outfile).wait()
                    except OSError as e:
                        logging.error(
                            "Unable to run {} script {}".format(self.perf, e.__str__())
                        )
                result = True
                self.trace_file = filename

        return result

    def stack_collapse(self, filename=None):
        result = False
        if self.trace_file is not None:
            if os.path.isfile(self.trace_file):
                if filename is None:
                    filename = self.output + ".stacks-folded"
                with open(filename, "w") as outfile:
                    args = [self.stack_collapser, os.path.abspath(self.trace_file)]
                    try:
                        subprocess.Popen(args=args, stdout=outfile).wait()
                    except OSError as e:
                        logging.error(
                            "Unable to stack collapse using: {0} {1}. Error {2}".format(
                                self.stack_collapser, self.trace_file, e.__str__()
                            )
                        )
                self.stack_collapse_file = filename
                result = True
            else:
                logging.error("Unable to open {0}".format(self.trace_file))
        return result

    def generate_outputs(self, use_case, **kwargs):
        outputs = {}
        binary = kwargs.get("binary")
        details = kwargs.get("details")
        if details is None:
            details = ""
        result = True
        # generate flame graph
        artifact_result, flame_graph_output = self.generate_flame_graph(
            "Flame Graph: " + use_case, details
        )
        if artifact_result is True:
            outputs["Flame Graph"] = flame_graph_output
        result &= artifact_result

        # save perf output
        if artifact_result is True:
            outputs["perf output"] = os.path.abspath(self.output)

        tid = self.pid
        # generate perf report --stdio report
        logging.info("Generating perf report text outputs")
        perf_report_output = self.output + ".perf-report.top-cpu.txt"

        artifact_result, perf_report_artifact = self.run_perf_report(
            tid, perf_report_output, None, "absolute"
        )

        if artifact_result is True:
            outputs["perf report top self-cpu"] = perf_report_artifact
        result &= artifact_result

        # generate perf report --stdio report
        logging.info(
            "Generating perf report text outputs only for dso {}".format(binary)
        )
        perf_report_output_dso = self.output + ".perf-report.top-cpu.dso.txt"

        artifact_result, perf_report_artifact = self.run_perf_report(
            tid, perf_report_output_dso, binary, "relative"
        )

        if artifact_result is True:
            outputs[
                "perf report top self-cpu (dso={})".format(binary)
            ] = perf_report_artifact
        result &= artifact_result

        if self.callgraph_mode == "dwarf":
            logging.warning(
                "Unable to use perf output collected with callgraph dwarf mode in pprof. Skipping artifacts generation."
            )
            logging.warning(
                "Check https://github.com/google/perf_data_converter/issues/40."
            )
        else:
            if self.pprof_bin is None:
                logging.error(
                    "Unable to detect pprof. Some of the capabilities will be disabled"
                )
            else:
                logging.info("Generating pprof text output")
                pprof_text_output = self.output + ".pprof.txt"
                artifact_result, pprof_artifact_text_output = run_pprof(
                    self.pprof_bin,
                    PPROF_FORMAT_TEXT,
                    pprof_text_output,
                    binary,
                    self.output,
                )
                result &= artifact_result

                if artifact_result is True:
                    outputs["Top entries in text form"] = pprof_artifact_text_output
                logging.info("Generating pprof png output")
                pprof_png_output = self.output + ".pprof.png"
                artifact_result, pprof_artifact_png_output = run_pprof(
                    self.pprof_bin,
                    PPROF_FORMAT_PNG,
                    pprof_png_output,
                    binary,
                    self.output,
                )
                if artifact_result is True:
                    outputs[
                        "Output graph image in PNG format"
                    ] = pprof_artifact_png_output
                result &= artifact_result

        return result, outputs

    def generate_flame_graph(self, title="Flame Graph", subtitle="", filename=None):
        result = False
        result_artifact = None
        # total_profile_duration = self.profile_end_time - self.profile_start_time

        if self.stack_collapse_file is not None:
            if os.path.isfile(self.stack_collapse_file):
                if filename is None:
                    filename = self.output + ".flamegraph.svg"
                with open(filename, "w") as outfile:
                    args = [
                        self.flamegraph_utity,
                        "--title",
                        title,
                        "--subtitle",
                        subtitle,
                        "--width",
                        "1600",
                        os.path.abspath(self.stack_collapse_file),
                    ]
                    try:
                        subprocess.Popen(args=args, stdout=outfile).wait()
                    except OSError as e:
                        logging.error(
                            "Unable t use flamegraph.pl to render a SVG. using: {0} {1}. Error {2}".format(
                                self.flamegraph_utity,
                                self.stack_collapse_file,
                                e.__str__(),
                            )
                        )
                result = True
                result_artifact = os.path.abspath(filename)
                logging.info(
                    "Successfully rendered flame graph to {0}".format(result_artifact)
                )
            else:
                logging.error("Unable to open {0}".format(self.stack_collapse_file))
        return result, result_artifact

    def get_collapsed_stacks(self):
        return self.collapsed_stacks

    def run_perf_report(self, tid, output, dso, percentage_mode):
        status = False
        result_artifact = None
        args = self.generate_report_command(tid, self.output, dso, percentage_mode)
        logging.info("Running {} report with args {}".format(self.perf, args))
        try:
            stdout, _ = subprocess.Popen(
                args=args, stdout=subprocess.PIPE
            ).communicate()
            logging.debug(stdout)
            with open(output, "w") as outfile:
                outfile.write(stdout.decode())
            status = True
            result_artifact = os.path.abspath(output)
        except OSError as e:
            logging.error(
                "Unable to run {} report. Error {}".format(self.perf, e.__str__())
            )
        return status, result_artifact
