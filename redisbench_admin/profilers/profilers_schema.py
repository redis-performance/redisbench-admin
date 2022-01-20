#  BSD 3-Clause License
#
#  Copyright (c) 2022., Redis Labs Modules
#  All rights reserved.
#
import logging

from pytablewriter import MarkdownTableWriter


def get_profilers_rts_key_prefix(triggering_env, tf_github_org, tf_github_repo):
    zset_name = "ci.benchmarks.redis.com/{triggering_env}/{github_org}/{github_repo}:profiles".format(
        triggering_env=triggering_env,
        github_org=tf_github_org,
        github_repo=tf_github_repo,
    )
    return zset_name


def local_profilers_print_artifacts_table(profilers_artifacts_matrix):
    logging.info("Printing profiler generated artifacts")
    test_cases = []
    profilers = []
    use_local_file = True
    table_name = "Profiler artifacts"
    for row in profilers_artifacts_matrix:
        test_case = row[0]
        profiler = row[1]
        artifact = row[2]
        local_file = row[3]
        s3_link = row[4]
        if s3_link is not None:
            if len(s3_link) > 0:
                use_local_file = False

        if test_case not in test_cases:
            test_cases.append(test_case)
        if profiler not in profilers:
            profilers.append(profiler)

    # We only need to print the testcase if there are more than one
    use_test_cases_row = True

    if len(test_cases) == 1:
        use_test_cases_row = False
        test_case = test_cases[0]
        table_name = "{} for testcase {}".format(table_name, test_case)

    # We only need to print the profiler type if there are more than one
    use_profilers_row = True
    if len(profilers) == 1:
        use_profilers_row = False
        profiler = test_cases[0]
        table_name = "{}. Used profiler {}".format(table_name, profiler)

    headers = []
    if use_test_cases_row:
        headers.append("Test Case")

    if use_profilers_row:
        headers.append("Profiler")

    headers.append("Artifact")
    if use_local_file:
        headers.append("Local file")
    else:
        headers.append("s3 link")

    profilers_final_matrix = []
    for row in profilers_artifacts_matrix:
        test_case = row[0]
        profiler = row[1]
        artifact = row[2]
        local_file = "{} ".format(row[3])
        s3_link = "{} ".format(row[4])

        final_row = []
        if use_test_cases_row:
            final_row.append(test_case)

        if use_profilers_row:
            final_row.append(profiler)

        final_row.append(artifact)
        if use_local_file:
            final_row.append(local_file)
        else:
            final_row.append(s3_link)
        profilers_final_matrix.append(final_row)

    writer = MarkdownTableWriter(
        table_name=table_name,
        headers=headers,
        value_matrix=profilers_final_matrix,
    )
    writer.write_table()
