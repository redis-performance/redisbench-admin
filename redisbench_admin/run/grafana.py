#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import logging
import time

import pytablewriter
import redis
from pytablewriter import MarkdownTableWriter
import urllib.request

# 30 days expire
STALL_INFO_DAYS = 30
EXPIRE_TIME_SECS_PROFILE_KEYS = 60 * 60 * 24 * STALL_INFO_DAYS
EXPIRE_TIME_MSECS_PROFILE_KEYS = EXPIRE_TIME_SECS_PROFILE_KEYS * 1000


def generate_artifacts_table_grafana_redis(
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
):
    logging.info("Printing profiler generated artifacts")
    table_name = "Profiler artifacts for test case {}".format(test_name)
    headers = ["artifact_name", "s3_link"]
    profilers_final_matrix = []
    profilers_final_matrix_html = []
    collapsed_stacks_all_threads_target_url = None
    for artifact in profile_artifacts:
        artifact_name = artifact["artifact_name"]
        artifact_link = artifact["s3_link"]
        if "Identical stacks collapsed" in artifact_name:
            collapsed_stacks_all_threads_target_url = artifact_link
        profilers_final_matrix.append(
            [
                artifact_name,
                artifact_link,
            ]
        )
        profilers_final_matrix_html.append(
            [
                artifact["artifact_name"],
                ' <a href="{}">{}</a>'.format(
                    artifact_link,
                    artifact_link,
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
    profile_id = get_profile_id_keyname(setup_name, start_time_str, tf_github_sha)
    profile_string_testcase_markdown_key = "profile:{}:{}".format(profile_id, test_name)
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
    if push_results_redistimeseries is True:
        current_time = time.time() * 1000
        timeframe_by_branch = current_time - EXPIRE_TIME_MSECS_PROFILE_KEYS
        if collapsed_stacks_all_threads_target_url is not None:
            collapsed_stack_lines = list(
                urllib.request.urlopen(collapsed_stacks_all_threads_target_url)
            )
            logging.info(
                "Storing {} lines of collapsed profile data in redis".format(
                    len(collapsed_stack_lines)
                )
            )
            # ft.create profiling_data:all_threads:idx on hash prefix 1 profiling_data:all_threads:* schema org text sortable repo text sortable branch text sortable setup text sortable  test_case text sortable hash tag  collapsed_stack text sortable collapsed_stack_count numeric sortable
            for line_n, line in enumerate(collapsed_stack_lines):
                splitted = line.decode().strip().split(" ")
                collapsed_stack = splitted[0]
                collapsed_stack_count = splitted[1]
                hash_key = {
                    "org": tf_github_org,
                    "repo": tf_github_repo,
                    "branch": tf_github_branch,
                    "setup": setup_name,
                    "test_case": test_name,
                    "hash": tf_github_sha,
                    "collapsed_stack": collapsed_stack,
                    "collapsed_stack_count": collapsed_stack_count,
                    "collection_time": str(current_time),
                }
                profile_hash_string_testcase_markdown_key = (
                    "profiling_data:all_threads:{}:{}:{}:{}:{}:#{}".format(
                        tf_github_org,
                        tf_github_repo,
                        setup_name,
                        tf_github_branch,
                        test_name,
                        line_n,
                    )
                )
                try:
                    redis_conn.hset(
                        profile_hash_string_testcase_markdown_key, mapping=hash_key
                    )
                    redis_conn.expire(
                        profile_hash_string_testcase_markdown_key,
                        EXPIRE_TIME_SECS_PROFILE_KEYS,
                    )
                except redis.exceptions.ResponseError as e:
                    logging.error(
                        "While setting the profile key: {} received the following error: {}".format(
                            profile_hash_string_testcase_markdown_key, e.__str__()
                        )
                    )
                    pass

        sorted_set_keys = [
            zset_profiles,
            zset_profiles_setups_testcases_profileid,
            zset_profiles_setups_testcases,
            zset_profiles_setup,
            zset_profiles_setups_testcases_branches_latest_link,
        ]
        logging.info(
            "Propulating the profile helper ZSETs: {}".format(" ".join(sorted_set_keys))
        )
        res = redis_conn.zadd(
            zset_profiles_setups_testcases_branches,
            {tf_github_branch: start_time_ms},
        )
        logging.info(
            "Result of ZADD {} {} {} = {}".format(
                zset_profiles_setups_testcases_branches,
                start_time_ms,
                tf_github_branch,
                res,
            )
        )
        redis_conn.zadd(
            zset_profiles_setups_testcases_branches_latest_link,
            {https_link: start_time_ms},
        )
        redis_conn.zadd(
            zset_profiles_setup,
            {setup_name: start_time_ms},
        )
        redis_conn.zadd(
            zset_profiles_setups_testcases,
            {test_name: start_time_ms},
        )
        redis_conn.zadd(
            zset_profiles_setups_testcases_profileid,
            {profile_id: start_time_ms},
        )
        res = redis_conn.zadd(
            zset_profiles,
            {profile_id: start_time_ms},
        )

        for keyname in sorted_set_keys:
            logging.info(
                "Expiring all elements with score between 0 and {}".format(
                    int(timeframe_by_branch)
                )
            )
            redis_conn.zremrangebyscore(keyname, 0, int(timeframe_by_branch))

        redis_conn.sadd(profile_set_redis_key, test_name)
        redis_conn.expire(profile_set_redis_key, EXPIRE_TIME_SECS_PROFILE_KEYS)
        redis_conn.setex(
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


def get_profile_id_keyname(setup_name, start_time_str, tf_github_sha):
    profile_id = "{}_{}_hash_{}".format(start_time_str, setup_name, tf_github_sha)
    return profile_id


def get_profile_zset_names(
    profile_id, setup_name, test_name, tf_github_branch, tf_github_org, tf_github_repo
):
    profile_set_redis_key = "profile:{}:testcases".format(profile_id)
    zset_profiles = "profiles:{}_{}_{}".format(
        tf_github_org, tf_github_repo, setup_name
    )
    zset_profiles_setup = "profiles:setups:{}_{}".format(
        tf_github_org,
        tf_github_repo,
    )
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
    return (
        profile_set_redis_key,
        zset_profiles,
        zset_profiles_setup,
        zset_profiles_setups_testcases,
        zset_profiles_setups_testcases_branches,
        zset_profiles_setups_testcases_branches_latest_link,
        zset_profiles_setups_testcases_profileid,
    )
