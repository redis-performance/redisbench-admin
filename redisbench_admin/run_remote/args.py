#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import os

# environment variables
from redisbench_admin.run.args import common_run_args
from redisbench_admin.run_remote.consts import (
    SERVER_PRV_IP_KEY,
    SERVER_PUB_IP_KEY,
    CLIENT_PUB_IP_KEY,
    DEFAULT_PRIVATE_KEY,
)
from redisbench_admin.utils.remote import TERRAFORM_BIN_PATH


REMOTE_INVENTORY = os.getenv("INVENTORY", None)
TF_OVERRIDE_NAME = os.getenv("TF_OVERRIDE_NAME", None)
REMOTE_DB_PORT = int(os.getenv("REMOTE_DB_PORT", "6379"))
REMOTE_DB_PASS = os.getenv("REMOTE_DB_PASS", None)
REMOTE_PRIVATE_KEYNAME = os.getenv("REMOTE_PRIVATE_KEYNAME", DEFAULT_PRIVATE_KEY)
FLUSHALL_AT_START = bool(int(os.getenv("FLUSHALL_AT_START", "0")))
FLUSHALL_AT_END = bool(int(os.getenv("FLUSHALL_AT_END", "0")))
IGNORE_KEYSPACE_ERRORS = bool(int(os.getenv("IGNORE_KEYSPACE_ERRORS", "0")))
TF_OVERRIDE_REMOTE = os.getenv("TF_OVERRIDE_REMOTE", None)
REMOTE_USER = os.getenv("REMOTE_USER", "ubuntu")
OVERRIDE_MODULES = os.getenv("OVERRIDE_MODULES", None)


def create_run_remote_arguments(parser):
    parser = common_run_args(parser)
    minimum_required_inv = "=<value>,".join(
        [SERVER_PRV_IP_KEY, SERVER_PUB_IP_KEY, CLIENT_PUB_IP_KEY, ""]
    )
    parser.add_argument(
        "--inventory",
        required=False,
        default=REMOTE_INVENTORY,
        type=str,
        help="specify comma separated kv hosts in the format k=v. At least the following keys should be present: {}".format(
            minimum_required_inv
        ),
    )
    parser.add_argument(
        "--user",
        required=False,
        default=REMOTE_USER,
        type=str,
        help="connect as this user.",
    )
    parser.add_argument(
        "--db_ssh_port",
        required=False,
        default=22,
        type=int,
        help="connect using this ssh port.",
    )
    parser.add_argument("--db_port", type=int, default=REMOTE_DB_PORT)
    parser.add_argument("--db_pass", type=str, default=REMOTE_DB_PASS)
    parser.add_argument(
        "--flushall_on_every_test_start",
        type=bool,
        default=FLUSHALL_AT_START,
        help="At the start of every test send a FLUSHALL",
    )
    parser.add_argument(
        "--ignore_keyspace_errors",
        type=bool,
        default=IGNORE_KEYSPACE_ERRORS,
        help="Ignore keyspace check errors. Will still log them as errors",
    )
    parser.add_argument(
        "--flushall_on_every_test_end",
        type=bool,
        default=FLUSHALL_AT_END,
        help="At the end of every test send a FLUSHALL",
    )
    parser.add_argument(
        "--client_ssh_port",
        required=False,
        default=22,
        type=int,
        help="connect using this ssh port.",
    )
    parser.add_argument(
        "--private_key",
        required=False,
        default=REMOTE_PRIVATE_KEYNAME,
        type=str,
        help="Use this key for ssh connections.",
    )
    parser.add_argument(
        "--callback",
        required=False,
        default=False,
        action="store_true",
        help="Push status to callback url",
    )
    parser.add_argument(
        "--callback_url",
        required=False,
        default="https://dashi.cto.redislabs.com/callback",
        help="Callback url",
    )
    parser.add_argument("--terraform_bin_path", type=str, default=TERRAFORM_BIN_PATH)
    parser.add_argument("--setup_name_sufix", type=str, default="")
    parser.add_argument(
        "--skip-env-vars-verify",
        default=False,
        action="store_true",
        help="skip environment variables check",
    )
    parser.add_argument(
        "--continue-on-module-check-error",
        default=False,
        action="store_true",
        help="Continue running benchmarks even if module check failed",
    )

    return parser
