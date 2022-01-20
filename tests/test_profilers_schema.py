#  BSD 3-Clause License
#
#  Copyright (c) 2022., Redis Labs Modules
#  All rights reserved.
#
from redisbench_admin.profilers.profilers_schema import get_profilers_rts_key_prefix


def test_get_profilers_rts_key_prefix():
    triggering_env = "ci"
    tf_github_org = "redislabs"
    tf_github_repo = "redisbench-admin"
    res = get_profilers_rts_key_prefix(triggering_env, tf_github_org, tf_github_repo)
    assert res == "ci.benchmarks.redis.com/ci/redislabs/redisbench-admin:profiles"
