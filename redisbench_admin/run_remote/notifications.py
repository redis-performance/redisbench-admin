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
    headline_test = "❌ FAILED CI job named {} in repo {}/{} due to {}".format(
        job_name, gh_org, gh_repo, failure_reason
    )
    extra_detail = ""
    if branch is not None:
        extra_detail = "This job was on a branch named {}\n".format(branch)
    if tag is not None:
        extra_detail = "This job was on a tag named {}\n".format(tag)
    message_text = (
        "{}/{} job name {} failed due to *{}*.\n{}<{}|Check CI job details>\n".format(
            gh_org, gh_repo, job_name, failure_reason, extra_detail, https_link
        )
    )
    slack_webhook_sent_message(headline_test, message_text, webhook_client)


def generate_new_pr_comment_notification(
    webhook_client,
    comparison_summary,
    https_link,
    gh_org,
    gh_repo,
    baseline,
    comparison,
    regression_count,
    action,
):
    headline_test = "👨‍💻 {} PR Performance comment in repo {}/{}\n".format(
        action, gh_org, gh_repo
    )
    if regression_count > 0:
        headline_test += "*DETECTED {} REGRESSIONS!*\n".format(regression_count)
    message_text = headline_test
    message_text += "This comparison was between {} and {}\n".format(
        baseline, comparison
    )
    message_text += comparison_summary
    message_text += "\n<{}|Check github comment details>\n".format(https_link)
    slack_webhook_sent_message(headline_test, message_text, webhook_client)


def slack_webhook_sent_message(headline_test, message_text, webhook_client):
    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": message_text,
            },
        }
    ]
    response = webhook_client.send(text=headline_test, blocks=blocks)
    if response.status_code != 200:
        logging.error(
            "Error while sending slack notification. Error message {}".response.body
        )
