import redis
from redistimeseries.client import Client

from redisbench_admin.utils.remote import (
    extract_git_vars,
    fetch_remote_setup_from_config, push_data_to_redistimeseries,
)


def test_extract_git_vars():
    (
        github_org_name,
        github_repo_name,
        github_sha,
        github_actor,
        github_branch,
        github_branch_detached,
    ) = extract_git_vars(".")
    assert github_org_name == "RedisLabsModules"
    assert github_repo_name == "redisbench-admin"
    assert github_sha != None and github_branch != ""
    if github_branch_detached is False:
        assert github_actor != None and github_branch != ""
        assert github_branch != None and github_branch != ""


def test_extract_git_vars_passing_repo():
    (
        github_org_name,
        github_repo_name,
        github_sha,
        github_actor,
        github_branch,
        github_branch_detached,
    ) = extract_git_vars(
        ".", github_url="https://github.com/RedisLabsModules/redisbench-admin"
    )
    assert github_org_name == "RedisLabsModules"
    assert github_repo_name == "redisbench-admin"
    assert github_sha != None and github_branch != ""
    if github_branch_detached is False:
        assert github_actor != None and github_branch != ""
        assert github_branch != None and github_branch != ""


def test_extract_git_vars_passing_repo2():
    (
        github_org_name,
        github_repo_name,
        github_sha,
        github_actor,
        github_branch,
        github_branch_detached,
    ) = extract_git_vars(
        ".", github_url="https://github.com/RedisLabsModules/redisbench-admin/"
    )
    assert github_org_name == "RedisLabsModules"
    assert github_repo_name == "redisbench-admin"
    assert github_sha != None and github_branch != ""
    if github_branch_detached is False:
        assert github_actor != None and github_branch != ""
        assert github_branch != None and github_branch != ""


def test_extract_git_vars_passing_repo3():
    (
        github_org_name,
        github_repo_name,
        github_sha,
        github_actor,
        github_branch,
        github_branch_detached,
    ) = extract_git_vars(
        ".", github_url="git@github.com:RedisLabsModules/redisbench-admin.git"
    )
    assert github_org_name == "RedisLabsModules"
    assert github_repo_name == "redisbench-admin"
    assert github_sha != None and github_branch != ""
    if github_branch_detached is False:
        assert github_actor != None and github_branch != ""
        assert github_branch != None and github_branch != ""


def test_fetch_remote_setup_from_config():
    terraform_working_dir, type, _ = fetch_remote_setup_from_config(
        [{"type": "oss-standalone"}, {"setup": "redistimeseries-m5d"}]
    )
    assert type == "oss-standalone"


def test_push_data_to_redistimeseries():
    time_series_dict = {}
    try:
        rts = Client()
        rts.redis.ping()
    except redis.exceptions.ConnectionError:
        pass
    finally:
        datapoint_errors, datapoint_inserts = push_data_to_redistimeseries(rts, time_series_dict)
        assert datapoint_errors == 0
        assert datapoint_inserts == 0
