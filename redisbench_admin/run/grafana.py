#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import logging

import pytablewriter
from pytablewriter import MarkdownTableWriter


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
        rts.redis.zadd(
            zset_profiles_setups_testcases_branches,
            {tf_github_branch: start_time_ms},
        )
        rts.redis.zadd(
            zset_profiles_setups_testcases_branches_latest_link,
            {https_link: start_time_ms},
        )
        rts.redis.zadd(
            zset_profiles_setup,
            {setup_name: start_time_ms},
        )
        rts.redis.zadd(
            zset_profiles_setups_testcases,
            {test_name: start_time_ms},
        )
        rts.redis.zadd(
            zset_profiles_setups_testcases_profileid,
            {profile_id: start_time_ms},
        )
        rts.redis.zadd(
            zset_profiles,
            {profile_id: start_time_ms},
        )
        rts.redis.sadd(profile_set_redis_key, test_name)
        rts.redis.set(
            profile_string_testcase_markdown_key,
            profile_markdown_str,
        )
        logging.info(
            "Store html table with artifacts in: {}".format(
                profile_string_testcase_markdown_key
            )
        )
    return https_link
