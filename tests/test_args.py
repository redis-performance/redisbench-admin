import argparse

from redisbench_admin.run_local.args import create_run_local_arguments
from redisbench_admin.run_remote.args import create_run_remote_arguments


def test_create_run_remote_arguments():
    parser = argparse.ArgumentParser(
        description="test",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser = create_run_remote_arguments(parser)
    args = parser.parse_args(args=["--github_actor", "--module_path", "mymodule.so"])
    assert args.github_actor == ""
    args = parser.parse_args(args=["--github_branch", "--module_path", "mymodule.so"])
    assert args.github_branch == ""
    args = parser.parse_args(args=["--github_sha", "--module_path", "mymodule.so"])
    assert args.github_sha == ""
    args = parser.parse_args(
        args=["--github_actor", "gh.user", "--module_path", "mymodule.so"]
    )
    assert args.github_actor == "gh.user"


def test_create_run_local_arguments():
    parser = argparse.ArgumentParser(
        description="test",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser = create_run_local_arguments(parser)
    args = parser.parse_args(
        args=["--module_path", "mymodule.so", "--test", "test1.yml"]
    )
    assert args.test == "test1.yml"
