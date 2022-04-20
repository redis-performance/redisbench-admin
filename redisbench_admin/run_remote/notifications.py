#  BSD 3-Clause License
#
#  Copyright (c) 2022., Redis Labs Modules
#  All rights reserved.
#

import logging


def generate_failure_notification(
    webhook_client,
    job_name,
    https_link,
    failure_reason,
    gh_org,
    gh_repo,
    branch=None,
    tag=None,
):

    headline_test = "{}/{} FAILED job {} due to {}".format(
        gh_org, gh_repo, job_name, failure_reason
    )
    extra_detail = ""
    if branch is not None:
        extra_detail = "This job was on a branch named {}\n".format(branch)
    if tag is not None:
        extra_detail = "This job was on a tag named {}\n".format(tag)
    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "{}/{} job name {} failed due to *{}*.\n{}<{}|Check CI job details>\n".format(
                    gh_org, gh_repo, job_name, failure_reason, extra_detail, https_link
                ),
            },
        }
    ]
    response = webhook_client.send(text=headline_test, blocks=blocks)
    if response.status_code != 200:
        logging.error(
            "Error while sending slack notification. Error message {}".response.body
        )
