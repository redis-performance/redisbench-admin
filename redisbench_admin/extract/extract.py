#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#

import json
import time

import redis


def current_milli_time():
    return round(time.time() * 1000)


def extract_command_logic(args):
    redis_url = args.redis_url
    output_json = args.output_tags_json
    redis_client = redis.from_url(redis_url)
    server_info = redis_client.info("server")
    server_info["extract_milli_time"] = current_milli_time()
    with open(output_json, "w") as json_file:
        json.dump(server_info, json_file, indent=2)
