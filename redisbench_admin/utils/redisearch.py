#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#

import logging
import sys

import redis


def extract_module_git_sha(redis_conn, module_name="search"):
    """Return the git SHA of a loaded Redis module, or None if unavailable.

    As of redis-stack-server 7.4, only the search module (RediSearch /
    Searchlight) exposes a git SHA via a DEBUG subcommand
    (`FT.DEBUG GIT_SHA`). JSON, timeseries, bloom and graph DO NOT expose
    an equivalent command — calls against them fall through and the helper
    returns None. The module map below is intentionally conservative and
    will be extended when upstream modules add the capability.

    Never raises on a missing module or command — returns None so callers
    can fall through to other hash sources.
    """
    debug_cmd_by_module = {
        "search": "FT.DEBUG",
        "ft": "FT.DEBUG",
        "searchlight": "FT.DEBUG",
    }
    try:
        modules = redis_conn.execute_command("MODULE", "LIST")
    except redis.RedisError as err:
        logging.debug("MODULE LIST failed while inferring module git_sha: %s", err)
        return None

    loaded = {}
    for entry in modules:
        try:
            name = entry[1].decode() if isinstance(entry[1], bytes) else entry[1]
        except (IndexError, AttributeError):
            continue
        loaded[name.lower()] = name

    ordered = []
    preferred = module_name.lower() if module_name else None
    if preferred and preferred in loaded:
        ordered.append(loaded[preferred])
    for name in loaded.values():
        if name not in ordered:
            ordered.append(name)

    for name in ordered:
        debug_cmd = debug_cmd_by_module.get(name.lower())
        if debug_cmd is None:
            continue
        try:
            reply = redis_conn.execute_command(debug_cmd, "GIT_SHA")
        except redis.RedisError as err:
            logging.debug("%s GIT_SHA failed for module %s: %s", debug_cmd, name, err)
            continue
        if reply is None:
            continue
        sha = reply.decode() if isinstance(reply, bytes) else str(reply)
        sha = sha.strip()
        if sha:
            logging.info(
                "Inferred module git_sha=%s from module %s via %s",
                sha,
                name,
                debug_cmd,
            )
            return sha
    return None


def check_and_extract_redisearch_info(redis_url):
    redisearch_git_sha = None
    redisearch_version = None
    print("Checking RediSearch is reachable at {}".format(redis_url))
    try:
        found_redisearch = False
        redis_client = redis.from_url(redis_url)
        module_list_reply = redis_client.execute_command("module list")
        for module in module_list_reply:
            module_name = module[1].decode()
            module_version = module[3]
            if module_name == "ft":
                found_redisearch = True
                redisearch_version = module_version
                debug_gitsha_reply = redis_client.execute_command("ft.debug git_sha")
                redisearch_git_sha = debug_gitsha_reply.decode()
                print(
                    "Found RediSearch Module at {}! version: {} git_sha: {}".format(
                        redis_url, redisearch_version, redisearch_git_sha
                    )
                )
        if found_redisearch is False:
            print("Unable to find RediSearch Module at {}! Exiting..".format(redis_url))
            sys.exit(1)

        server_info = redis_client.info("Server")

    except redis.connection.ConnectionError as e:
        print(
            "Error establishing connection to Redis at {}! Message: {} Exiting..".format(
                redis_url, e.__str__()
            )
        )
        sys.exit(1)
    return redisearch_git_sha, redisearch_version, server_info
