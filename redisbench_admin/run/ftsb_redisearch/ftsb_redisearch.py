# Copyright (C) 2020 Redis Labs Ltd.
#
# This file is part of redisbench-admin.
#
# redisbench-admin is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, version 2.
#
# redisbench-admin is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with redisbench-admin.  If not, see <http://www.gnu.org/licenses/>.

import json
import os
import subprocess
import sys


def get_run_options():
    environ = os.environ.copy()
    stdoutPipe = subprocess.PIPE
    stderrPipe = subprocess.STDOUT
    stdinPipe = subprocess.PIPE
    options = {
        'stderr': stderrPipe,
        'env': environ,
    }
    return options


def run_ftsb_redisearch(redis_url, ftsb_redisearch_path, setup_run_json_output_fullpath, options, input_file, workers=1,
                        pipeline=1, oss_cluster_mode=False, max_rps=0, requests=0, args=[] ):
    ##################
    # Setup commands #
    ##################
    output_json = None
    ftsb_args = []
    ftsb_args += [ftsb_redisearch_path, "--host={}".format(redis_url),
                  "--input={}".format(input_file), "--workers={}".format(workers),
                  "--pipeline={}".format(pipeline),
                  "--json-out-file={}".format(setup_run_json_output_fullpath)]
    if max_rps > 0:
        ftsb_args += ["--max-rps={}".format(max_rps)]
    if requests > 0:
        ftsb_args += ["--requests={}".format(requests)]
    if oss_cluster_mode:
        ftsb_args += ["--cluster-mode"]

    ftsb_process = subprocess.Popen(args=ftsb_args, **options)
    if ftsb_process.poll() is not None:
        print('Error while issuing setup commands. FTSB process is not alive. Exiting..')
        sys.exit(1)
    output = ftsb_process.communicate()
    if ftsb_process.returncode != 0:
        print('FTSB process returned non-zero exit status {}. Exiting..'.format(ftsb_process.returncode))
        print('catched output:\n\t{}'.format(output))
        sys.exit(1)
    with open(setup_run_json_output_fullpath) as json_result:
        output_json = json.load(json_result)
    return output_json
