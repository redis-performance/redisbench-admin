#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import os

DEFAULT_PRIVATE_KEY = "/tmp/benchmarks.redislabs.pem"
TERRAFORM_BIN_PATH = os.getenv("TERRAFORM_BIN_PATH", "terraform")
INFRA_TIMEOUT_SECS = int(os.getenv("INFRA_TIMEOUT_SECS", "1200"))
GIT_ORG = os.getenv("GIT_ORG", None)
GIT_REPO = os.getenv("GIT_REPO", None)


def create_deploy_arguments(parser):
    parser.add_argument(
        "--private_key",
        required=False,
        default=DEFAULT_PRIVATE_KEY,
        type=str,
        help="Use this key for ssh connections.",
    )
    parser.add_argument("--terraform_bin_path", type=str, default=TERRAFORM_BIN_PATH)
    parser.add_argument("--infra_timeout_secs", type=int, default=INFRA_TIMEOUT_SECS)
    parser.add_argument("--github_actor", type=str, default=None, nargs="?", const="")
    parser.add_argument("--github_repo", type=str, default=GIT_REPO)
    parser.add_argument("--github_org", type=str, default=GIT_ORG)
    parser.add_argument("--github_sha", type=str, default=None, nargs="?", const="")
    parser.add_argument("--github_branch", type=str, default=None, nargs="?", const="")
    parser.add_argument("--inventory_git", required=True, type=str)
    parser.add_argument("--inventory_local_dir", default=None, type=str)
    parser.add_argument("--set_env_vars_json", default="", type=str)
    parser.add_argument("--setup_name_sufix", type=str, default="")
    parser.add_argument(
        "--destroy", help="destroy the current env", action="store_true"
    )
    parser.add_argument(
        "--skip-env-vars-verify",
        default=False,
        action="store_true",
        help="skip environment variables check",
    )
    return parser
