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


def test_db_dirname_argument_default():
    """Test that --db-dirname has default value of '/tmp'"""
    # Test run-local
    parser = argparse.ArgumentParser(
        description="test",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser = create_run_local_arguments(parser)
    args = parser.parse_args(
        args=["--module_path", "mymodule.so", "--test", "test1.yml"]
    )
    assert args.db_dirname == "/tmp"

    # Test run-remote
    parser = argparse.ArgumentParser(
        description="test",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser = create_run_remote_arguments(parser)
    args = parser.parse_args(args=["--module_path", "mymodule.so"])
    assert args.db_dirname == "/tmp"


def test_db_dirname_argument_custom():
    """Test that --db-dirname accepts custom values"""
    # Test run-local
    parser = argparse.ArgumentParser(
        description="test",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser = create_run_local_arguments(parser)
    args = parser.parse_args(
        args=[
            "--module_path",
            "mymodule.so",
            "--test",
            "test1.yml",
            "--db-dirname",
            "/custom/db/path",
        ]
    )
    assert args.db_dirname == "/custom/db/path"

    # Test run-remote
    parser = argparse.ArgumentParser(
        description="test",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser = create_run_remote_arguments(parser)
    args = parser.parse_args(
        args=["--module_path", "mymodule.so", "--db-dirname", "./data/db"]
    )
    assert args.db_dirname == "./data/db"
