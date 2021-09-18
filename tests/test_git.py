#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
from redisbench_admin.run.git import git_vars_crosscheck


def test_git_vars_crosscheck():
    (
        tf_github_actor,
        tf_github_branch,
        tf_github_org,
        tf_github_repo,
        tf_github_sha,
    ) = git_vars_crosscheck(None, None, None, None, None)
