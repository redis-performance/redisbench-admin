#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import logging

from redisbench_admin.utils.remote import extract_git_vars


def git_vars_crosscheck(
    tf_github_actor, tf_github_branch, tf_github_org, tf_github_repo, tf_github_sha
):
    if tf_github_org is None:
        (
            github_org_name,
            github_repo_name,
            github_sha,
            github_actor,
            github_branch,
            github_branch_detached,
        ) = extract_git_vars()
        logging.info(
            "Extracting tf_github_org given args.github_org was none. Extracte value {}".format(
                github_org_name
            )
        )
        tf_github_org = github_org_name
    if tf_github_actor is None:
        (
            github_org_name,
            github_repo_name,
            github_sha,
            github_actor,
            github_branch,
            github_branch_detached,
        ) = extract_git_vars()
        logging.info(
            "Extracting tf_github_actor given args.github_actor was none. Extracte value {}".format(
                github_actor
            )
        )
        tf_github_actor = github_actor
    if tf_github_repo is None:
        (
            github_org_name,
            github_repo_name,
            github_sha,
            github_actor,
            github_branch,
            github_branch_detached,
        ) = extract_git_vars()
        logging.info(
            "Extracting tf_github_repo given args.github_repo was none. Extracte value {}".format(
                github_repo_name
            )
        )
        tf_github_repo = github_repo_name
    if tf_github_sha is None:
        (
            github_org_name,
            github_repo_name,
            github_sha,
            github_actor,
            github_branch,
            github_branch_detached,
        ) = extract_git_vars()
        logging.info(
            "Extracting tf_github_sha given args.github_sha was none. Extracte value {}".format(
                github_sha
            )
        )
        tf_github_sha = github_sha
    if tf_github_branch is None:
        (
            github_org_name,
            github_repo_name,
            github_sha,
            github_actor,
            github_branch,
            github_branch_detached,
        ) = extract_git_vars()
        logging.info(
            "Extracting tf_github_branch given args.github_branch was none. Extracte value {}".format(
                github_branch
            )
        )
        tf_github_branch = github_branch
    return (
        tf_github_actor,
        tf_github_branch,
        tf_github_org,
        tf_github_repo,
        tf_github_sha,
    )
