#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#

import logging
import os
import subprocess

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
    cmd = [
        pprof_bin,
        format,
        "-edgefraction",
        "{}".format(edge_fraction),
        "-nodefraction",
        "{}".format(node_fraction),
        "-output",
        "{}".format(output),
        main_binary,
        profile,
    ]
    return cmd


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
    except OSError as e:
        logging.error(
            "Unable to run {} format {}. Error {}".format(
                pprof_bin, format, e.__str__()
            )
        )
    return status, result_artifact
