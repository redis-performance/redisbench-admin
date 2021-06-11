#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#


import datetime
import logging
import time

import boto3
import redis
from redistimeseries.client import Client

from redisbench_admin.run.common import get_start_time_vars
from redisbench_admin.utils.remote import (
    EC2_REGION,
    EC2_SECRET_KEY,
    EC2_ACCESS_KEY,
    check_ec2_env,
)

terminate_after_secs = 45 * 60
dry_run = True
ci_machines_prefix = "/tmp/"


def get_ci_ec2_instances_by_state(ec2_client, ci_machines_prefix, requested_state):
    count = 0
    state_instances = []
    response = ec2_client.describe_instances()
    for group in response["Reservations"]:
        instances = group["Instances"]
        for instance in instances:
            state = instance["State"]["Name"]
            for tag_dict in instance["Tags"]:
                key = tag_dict["Key"]
                key_v = tag_dict["Value"]
                if key == "Name":
                    if ci_machines_prefix in key_v:
                        if state == requested_state:
                            count = count + 1
                            state_instances.append(instance)
    return count, state_instances


def watchdog_dangling_ec2_instances(
    ec2_client, terminate_after_secs, ci_machines_prefix, dry_run
):
    current_datetime = datetime.datetime.now(datetime.timezone.utc)
    total_instances = 0
    response = ec2_client.describe_instances()
    for group in response["Reservations"]:
        instances = group["Instances"]
        for instance in instances:
            launch_time = instance["LaunchTime"]
            instance_id = instance["InstanceId"]
            state = instance["State"]["Name"]
            if state != "terminated":
                for tag_dict in instance["Tags"]:
                    key = tag_dict["Key"]
                    key_v = tag_dict["Value"]
                    if key == "Name":
                        if ci_machines_prefix in key_v:
                            total_instances = total_instances + 1
                            elapsed = current_datetime - launch_time
                            will_terminate = False
                            if elapsed.total_seconds() > terminate_after_secs:
                                will_terminate = True

                            logging.info(
                                "Temporary machine {} {}. terminate? {}".format(
                                    key_v, elapsed, will_terminate
                                )
                            )
                            if will_terminate:
                                logging.warning(
                                    "Requesting to terminate instance with id {} given it ".format(
                                        instance_id
                                    )
                                    + "surpassed the maximum allowed ci duration"
                                )
                                response = ec2_client.terminate_instances(
                                    InstanceIds=[
                                        instance_id,
                                    ]
                                )
                                logging.info(
                                    "Request to terminate instance with id {} reply: {}".format(
                                        instance_id, response
                                    )
                                )
    logging.info("Detected a total of {} ci.bechmark VMs".format(total_instances))


def watchdog_command_logic(args):
    cloud = "aws"
    prefix = "ci.benchmarks.redislabs/{}/{}".format(cloud, EC2_REGION)
    tsname_overall_running = "{}/state-running".format(prefix)
    check_ec2_env()
    boto3.setup_default_session(
        region_name=EC2_REGION,
        aws_access_key_id=EC2_ACCESS_KEY,
        aws_secret_access_key=EC2_SECRET_KEY,
    )
    logging.info("Checking connection to RedisTimeSeries.")
    rts = Client(
        host=args.redistimesies_host,
        port=args.redistimesies_port,
        password=args.redistimesies_pass,
    )
    rts.redis.ping()
    ec2_client = boto3.client("ec2")
    update_interval = args.update_interval
    logging.info(
        "Entering watching loop. Ticking every {} secs".format(update_interval)
    )
    while True:
        starttime, start_time_ms, _ = get_start_time_vars()

        watchdog_dangling_ec2_instances(
            ec2_client, terminate_after_secs, ci_machines_prefix, dry_run
        )

        running_count, _ = get_ci_ec2_instances_by_state(
            ec2_client, ci_machines_prefix, "running"
        )
        try:
            rts.add(
                tsname_overall_running,
                start_time_ms,
                running_count,
                labels={"cloud": cloud, "region": EC2_REGION},
            )
        except redis.exceptions.ConnectionError as e:
            logging.error(
                "Detected an error while writing data to rts: {}".format(e.__str__())
            )
        sleep_time_secs = float(update_interval) - (
            (datetime.datetime.now() - starttime).total_seconds()
            % float(update_interval)
        )
        logging.info("Sleeping for {} secs".format(sleep_time_secs))
        time.sleep(sleep_time_secs)
