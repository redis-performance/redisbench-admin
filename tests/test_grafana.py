#  BSD 3-Clause License
#
#  Copyright (c) 2022., Redis Labs Modules
#  All rights reserved.
#
import os
import time

import redis

from redisbench_admin.run.grafana import (
    generate_artifacts_table_grafana_redis,
    get_profile_zset_names,
    get_profile_id_keyname,
)


def test_generate_artifacts_table_grafana_redis():
    rts_host = os.getenv("RTS_DATASINK_HOST", None)
    rts_port = 16379
    if rts_host is None:
        return
    redis_conn = redis.Redis(port=rts_port, host=rts_host, decode_responses=True)
    redis_conn.ping()
    redis_conn.flushall()
    push_results_redistimeseries = True
    grafana_profile_dashboard = (
        "https://benchmarksrediscom.grafana.net/d/uRPZar57k/ci-profiler-viewer"
    )
    setup_name = "oss-standalone"
    test_name = (
        "memtier_benchmark-1Mkeys-load-stream-1-fields-with-100B-values-pipeline-10"
    )
    current_time = time.time() * 1000
    start_time_ms = current_time
    start_time_str = "2022-04-22-10-34-25"
    tf_github_org = "redis"
    tf_github_repo = "redis"
    tf_github_sha = "96c8751069a89ecfa62e4291d8f879882bc0f0aa"
    tf_github_branch = "unstable"
    profile_artifacts = [
        {
            "artifact_name": "artifact 1",
            "s3_link": "s3:212312",
        }
    ]
    generate_artifacts_table_grafana_redis(
        push_results_redistimeseries,
        grafana_profile_dashboard,
        profile_artifacts,
        redis_conn,
        setup_name,
        start_time_ms,
        start_time_str,
        test_name,
        tf_github_org,
        tf_github_repo,
        tf_github_sha,
        tf_github_branch,
    )
    profile_id = get_profile_id_keyname(setup_name, start_time_str, tf_github_sha)
    (
        profile_set_redis_key,
        zset_profiles,
        zset_profiles_setup,
        zset_profiles_setups_testcases,
        zset_profiles_setups_testcases_branches,
        zset_profiles_setups_testcases_branches_latest_link,
        zset_profiles_setups_testcases_profileid,
    ) = get_profile_zset_names(
        profile_id,
        setup_name,
        test_name,
        tf_github_branch,
        tf_github_org,
        tf_github_repo,
    )
    assert redis_conn.exists(zset_profiles)
    assert redis_conn.exists(profile_set_redis_key)
    assert redis_conn.exists(zset_profiles_setup)
    assert redis_conn.exists(zset_profiles_setups_testcases)
    assert redis_conn.exists(zset_profiles_setups_testcases_branches)
    assert redis_conn.exists(zset_profiles_setups_testcases_branches_latest_link)
    assert redis_conn.exists(zset_profiles_setups_testcases_profileid)
    assert redis_conn.type(zset_profiles) == "zset"
    assert redis_conn.zcard(zset_profiles) == 1
    assert redis_conn.zcard(zset_profiles_setup) == 1
    assert redis_conn.zcard(zset_profiles_setups_testcases) == 1
    assert redis_conn.zcard(zset_profiles_setups_testcases_branches) == 1
    assert redis_conn.zcard(zset_profiles_setups_testcases_branches_latest_link) == 1
    assert redis_conn.zcard(zset_profiles_setups_testcases_profileid) == 1
    assert redis_conn.zrangebyscore(
        zset_profiles_setups_testcases, start_time_ms - 1, start_time_ms + 1
    ) == [test_name]
    assert redis_conn.zrangebyscore(
        zset_profiles_setup, start_time_ms - 1, start_time_ms + 1
    ) == [setup_name]
    assert redis_conn.zrangebyscore(
        zset_profiles_setups_testcases_branches, start_time_ms - 1, start_time_ms + 1
    ) == [tf_github_branch]
