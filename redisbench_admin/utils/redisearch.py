import sys

import redis


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
