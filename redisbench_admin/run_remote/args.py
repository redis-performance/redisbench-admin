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
)
from redisbench_admin.utils.remote import TERRAFORM_BIN_PATH


REMOTE_INVENTORY = os.getenv("INVENTORY", None)
REMOTE_USER = os.getenv("REMOTE_USER", "ubuntu")
KEEP_ENV = bool(os.getenv("KEEP_ENV", False))


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
        "--keep_env_and_topo",
        required=False,
        default=KEEP_ENV,
        action="store_true",
        help="Keep environment and topology up after benchmark.",
    )
    parser.add_argument(
        "--ssh_port",
        required=False,
        default=22,
        type=int,
        help="connect using this ssh port.",
    )
    parser.add_argument("--terraform_bin_path", type=str, default=TERRAFORM_BIN_PATH)
    parser.add_argument("--setup_name_sufix", type=str, default="")
    parser.add_argument(
        "--skip-env-vars-verify",
        default=False,
        action="store_true",
        help="skip environment variables check",
    )

    return parser
