#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#

import logging
import os
import subprocess
import re

PPROF_FORMAT_TEXT = "-text"
PPROF_FORMAT_PS = "-ps"
PPROF_FORMAT_PNG = "-png"


def generate_pprof_cmd_args(
    pprof_bin,
    format,
    output,
    main_binary,
    profile,
    edge_fraction=0.01,
    node_fraction=0.01,
):
    cmd = [pprof_bin]
    if type(format) == str:
        cmd.extend([format])
    if type(format) == list:
        cmd.extend(format)
    cmd.extend(
        [
            "-edgefraction",
            "{}".format(edge_fraction),
            "-nodefraction",
            "{}".format(node_fraction),
            "-output",
            "{}".format(output),
        ]
    )
    if main_binary is not None:
        if main_binary != "":
            cmd.append(main_binary)
    cmd.append(profile)
    return cmd


def process_pprof_text_to_tabular(result_artifact, type="text"):
    tabular_data = {}
    # we're interested in groups 1, 5, and 7
    pprof_regex = re.compile(
        r"^\s*\d+\s+(\d+(.\d+)?)\%\s+(\d+(.\d+)?)\%\s+\d+\s+(\d+(.\d+)?)\%\s+(.*)$"
    )

    with open(result_artifact, "r") as pprof_text:
        raw_lines = pprof_text.readlines()
        start_processing = False
        flat_percent_list = []
        cum_percent_list = []
        entry_list = []
        for line_number, line in enumerate(raw_lines):
            if "cum%" in line and "flat%" in line:
                start_processing = True
                continue
            if start_processing:
                # sample line
                #      flat  flat%   sum%        cum   cum%
                # 116913708521 36.49% 36.49% 119190968309 37.20%  generate_digits (inline)
                m = pprof_regex.match(line)
                logging.info(len(m.groups()))
                assert len(m.groups()) == 7
                logging.info(m.group(1, 5, 7))
                flat_percent, cum_percent, entry = m.group(1, 5, 7)
                flat_percent_list.append(flat_percent)
                cum_percent_list.append(cum_percent)
                entry_list.append(entry)
        tabular_data = {
            "columns:text": ["self%", "cum%", "entry"],
            "columns:type": ["number", "number", "text"],
            "rows:self%": flat_percent_list,
            "rows:cum%": cum_percent_list,
            "rows:entry": entry_list,
            "type": type,
        }
    return tabular_data


def run_pprof(
    pprof_bin,
    format,
    output,
    main_binary,
    profile,
    edge_fraction=0.01,
    node_fraction=0.01,
):
    status = False
    result_artifact = None
    tabular_data = None
    args = generate_pprof_cmd_args(
        pprof_bin, format, output, main_binary, profile, edge_fraction, node_fraction
    )
    try:
        logging.info("Running pprof {} with args {}".format(pprof_bin, args))
        pprof_process = subprocess.Popen(args=args)
        pprof_return_code = pprof_process.wait()
        if pprof_return_code <= 0:
            status = True
            result_artifact = os.path.abspath(output)
            if (type(format) == str and format == PPROF_FORMAT_TEXT) or (
                type(format) == list and format[0] == PPROF_FORMAT_TEXT
            ):
                logging.info("generating tabular data from pprof output")
                type_str = ""
                if type(format) == str:
                    type_str = format
                if type(format) == list:
                    type_str = "".join(format)
                tabular_data = process_pprof_text_to_tabular(result_artifact, type_str)
        else:
            logging.error("pprof returned an exit code >0 {}".format(pprof_return_code))
    except OSError as e:
        logging.error(
            "Unable to run {} format {}. Error {}".format(
                pprof_bin, format, e.__str__()
            )
        )
    return status, result_artifact, tabular_data
