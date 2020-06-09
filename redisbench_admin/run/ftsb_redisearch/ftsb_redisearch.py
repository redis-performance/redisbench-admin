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


def run_ftsb_redisearch(redis_url, ftsb_redisearch_path, setup_run_json_output_fullpath, options, input_file,
                        workers):
    ##################
    # Setup commands #
    ##################
    output_json = None
    ftsb_args = []
    ftsb_args += [ftsb_redisearch_path, "--host={}".format(redis_url),
                  "--input={}".format(input_file), "--workers={}".format(workers),
                  "--json-out-file={}".format(setup_run_json_output_fullpath)]
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
