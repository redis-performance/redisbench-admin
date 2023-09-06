from argparse import Namespace


def render_run_script(
    access_key: str,
    region: str,
    secret_key: str,
    gh_token: str,
    gh_branch: str,
    job_name: str,
    circle_pull_request: str,
    args: Namespace,
    argv: list,
) -> None:
    """
    Generate a bash script for running a remote RedisBench job.

    Args:
        access_key (str): AWS access key ID.
        region (str): AWS region.
        secret_key (str): AWS secret access key.
        gh_token (str): GitHub token.
        job_name (str): Name of the job.
        args (Namespace): Command-line arguments for the RedisBench job.
        argv (list): List of additional command-line arguments.
    """
    gh_branch = args.gh_branch if args.gh_branch else None
    circle_pull_request = args.circle_pull_request if args.circle_pull_request else None

    if "--private_key" not in argv:
        argv.extend(
            [
                "--private_key",
                "/home/ubuntu/work_dir/tests/benchmarks/benchmarks.redislabs.pem",
            ]
        )
    else:
        private_key_index = argv.index("--private_key")
        argv[
            private_key_index + 1
        ] = "/home/ubuntu/work_dir/tests/benchmarks/benchmarks.redislabs.pem"

    if args.module_path:
        module_path_index = argv.index(args.module_path[0])
        module_filename = args.module_path[0].split("/")[-1]
        argv[
            module_path_index
        ] = f"/home/ubuntu/work_dir/tests/benchmarks/{module_filename}"

    argv_str = " ".join(argv)
    script_lines = [
        f'export AWS_ACCESS_KEY_ID="{access_key}"',
        f'export AWS_DEFAULT_REGION="{region}"',
        f'export AWS_SECRET_ACCESS_KEY="{secret_key}"',
        f'export GH_TOKEN="{gh_token}"',
        f'export CIRCLE_JOB="{job_name}"',
        f'export GH_TOKEN="{gh_token}"' if gh_token is not None else "",
        f'export CIRCLE_JOB="{job_name}"' if job_name is not None else "",
        f"redisbench-admin run-remote {argv_str}",
        f"""redisbench-admin compare  \
                  --defaults_filename ./tests/benchmarks/defaults.yml \
                  --comparison-branch {gh_branch} \
                  --auto-approve \
                  --pull-request {circle_pull_request}
         """
        if circle_pull_request is not "None"
        else "",
    ]
    with open("runbenchmark", mode="w", encoding="utf-8") as results:
        results.write("\n".join(script_lines))


def save_pem_file(pem_data: str) -> None:
    """
    Save PEM-encoded data to a file after processing.

    Args:
        pem_data (str): The PEM-encoded data to be processed and saved.
    """
    pem_data = pem_data.replace("-----BEGIN RSA PRIVATE KEY-----", "")
    pem_data = pem_data.replace("-----END RSA PRIVATE KEY-----", "")
    pem_data = pem_data.replace(" ", "\n")
    pem_data = (
        "-----BEGIN RSA PRIVATE KEY-----" + pem_data + "-----END RSA PRIVATE KEY-----"
    )

    with open("benchmarks.redislabs.pem", mode="w", encoding="utf-8") as pem_file:
        pem_file.write(pem_data)
