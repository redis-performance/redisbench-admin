#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import logging
import time

import pytablewriter
from pytablewriter import MarkdownTableWriter

# 7 days expire
STALL_INFO_DAYS = 7
EXPIRE_TIME_SECS_PROFILE_KEYS = 60 * 60 * 24 * STALL_INFO_DAYS
EXPIRE_TIME_MSECS_PROFILE_KEYS = EXPIRE_TIME_SECS_PROFILE_KEYS * 1000


def generate_artifacts_table_grafana_redis(
    args,
    grafana_profile_dashboard,
    profile_artifacts,
    rts,
    setup_name,
    start_time_ms,
    start_time_str,
    test_name,
    tf_github_org,
    tf_github_repo,
    tf_github_sha,
    tf_github_branch,
):
    logging.info("Printing profiler generated artifacts")
    table_name = "Profiler artifacts for test case {}".format(test_name)
    headers = ["artifact_name", "s3_link"]
    profilers_final_matrix = []
    profilers_final_matrix_html = []
    for artifact in profile_artifacts:
        profilers_final_matrix.append(
            [
                artifact["artifact_name"],
                artifact["s3_link"],
            ]
        )
        profilers_final_matrix_html.append(
            [
                artifact["artifact_name"],
                ' <a href="{}">{}</a>'.format(
                    artifact["s3_link"],
                    artifact["s3_link"],
                ),
            ]
        )
    writer = MarkdownTableWriter(
        table_name=table_name,
        headers=headers,
        value_matrix=profilers_final_matrix,
    )
    writer.write_table()
    htmlwriter = pytablewriter.HtmlTableWriter(
        table_name=table_name,
        headers=headers,
        value_matrix=profilers_final_matrix_html,
    )
    profile_markdown_str = htmlwriter.dumps()
    profile_markdown_str = profile_markdown_str.replace("\n", "")
    profile_id = "{}_{}_hash_{}".format(start_time_str, setup_name, tf_github_sha)
    profile_string_testcase_markdown_key = "profile:{}:{}".format(profile_id, test_name)
    zset_profiles = "profiles:{}_{}_{}".format(
        tf_github_org, tf_github_repo, setup_name
    )
    zset_profiles_setup = "profiles:setups:{}_{}".format(
        tf_github_org,
        tf_github_repo,
    )
    profile_set_redis_key = "profile:{}:testcases".format(profile_id)
    zset_profiles_setups_testcases = "profiles:testcases:{}_{}_{}".format(
        tf_github_org,
        tf_github_repo,
        setup_name,
    )
    zset_profiles_setups_testcases_profileid = "profiles:ids:{}_{}_{}_{}_{}".format(
        tf_github_org,
        tf_github_repo,
        setup_name,
        test_name,
        tf_github_branch,
    )
    zset_profiles_setups_testcases_branches = "profiles:branches:{}_{}_{}_{}".format(
        tf_github_org, tf_github_repo, setup_name, test_name
    )
    zset_profiles_setups_testcases_branches_latest_link = (
        "latest_profiles:by.branch:{}_{}_{}_{}".format(
            tf_github_org, tf_github_repo, setup_name, test_name
        )
    )
    https_link = "{}?var-org={}&var-repo={}&var-setup={}&var-branch={}".format(
        grafana_profile_dashboard,
        tf_github_org,
        tf_github_repo,
        setup_name,
        tf_github_branch,
    ) + "&var-test_case={}&var-profile_id={}".format(
        test_name,
        profile_id,
    )
    if args.push_results_redistimeseries:
        current_time = time.time() * 1000
        timeframe_by_branch = current_time - EXPIRE_TIME_MSECS_PROFILE_KEYS
        rts.zadd(
            zset_profiles_setups_testcases_branches,
            {tf_github_branch: start_time_ms},
        )
        rts.zadd(
            zset_profiles_setups_testcases_branches_latest_link,
            {https_link: start_time_ms},
        )
        rts.zadd(
            zset_profiles_setup,
            {setup_name: start_time_ms},
        )
        rts.zadd(
            zset_profiles_setups_testcases,
            {test_name: start_time_ms},
        )
        rts.zadd(
            zset_profiles_setups_testcases_profileid,
            {profile_id: start_time_ms},
        )
        rts.zadd(
            zset_profiles,
            {profile_id: start_time_ms},
        )
        sorted_set_keys = [
            zset_profiles,
            zset_profiles_setups_testcases_profileid,
            zset_profiles_setups_testcases,
            zset_profiles_setup,
            zset_profiles_setups_testcases_branches_latest_link,
        ]
        for keyname in sorted_set_keys:
            rts.zremrangebyscore(keyname, 0, int(timeframe_by_branch))

        rts.sadd(profile_set_redis_key, test_name)
        rts.expire(profile_set_redis_key, EXPIRE_TIME_SECS_PROFILE_KEYS)
        rts.setex(
            profile_string_testcase_markdown_key,
            EXPIRE_TIME_SECS_PROFILE_KEYS,
            profile_markdown_str,
        )
        logging.info(
            "Store html table with artifacts in: {}".format(
                profile_string_testcase_markdown_key
            )
        )
    return https_link
