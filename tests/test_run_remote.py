#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
import argparse
import os

import pytest
import redis
import yaml

from redisbench_admin.run.metrics import collect_redis_metrics
from redisbench_admin.run_remote.args import create_run_remote_arguments
from redisbench_admin.run_remote.run_remote import (
    export_redis_metrics,
    run_remote_command_logic,
    save_env_for_cross_type_reuse,
    tear_down_previous_mixed_env_if_needed,
)
from redisbench_admin.utils.remote import check_ec2_env


def test_export_redis_metrics():
    end_time_ms = 1
    overall_end_time_metrics = {}
    setup_name = "setup_name"
    test_name = "test1"
    tf_github_branch = None
    tf_github_org = "org"
    tf_github_repo = "repo"
    tf_triggering_env = "env"
    setup_type = "oss-standalone"
    artifact_version = None
    try:
        rts_host = os.environ.get("RTS_DATASINK_HOST", None)
        rts_port = os.environ.get("RTS_PORT", None)
        # Skip test if required environment variables are not set
        if rts_port is None or rts_host is None:
            pytest.skip(
                "RTS_PORT and RTS_DATASINK_HOST environment variables are required for this test"
            )
        rts = redis.Redis(port=rts_port, host=rts_host)
        rts.ping()
        datapoint_errors, datapoint_inserts = export_redis_metrics(
            artifact_version,
            end_time_ms,
            overall_end_time_metrics,
            rts,
            setup_name,
            setup_type,
            test_name,
            tf_github_branch,
            tf_github_org,
            tf_github_repo,
            tf_triggering_env,
        )
        assert datapoint_errors == 0
        assert datapoint_inserts == 0
        tf_github_branch = ""
        artifact_version = ""
        datapoint_errors, datapoint_inserts = export_redis_metrics(
            artifact_version,
            end_time_ms,
            overall_end_time_metrics,
            rts,
            setup_name,
            setup_type,
            test_name,
            tf_github_branch,
            tf_github_org,
            tf_github_repo,
            tf_triggering_env,
        )
        assert datapoint_errors == 0
        assert datapoint_inserts == 0

        time_ms, _, overall_end_time_metrics = collect_redis_metrics([rts])
        artifact_version = "6.2.3"
        tf_github_branch = "master"
        datapoint_errors, datapoint_inserts = export_redis_metrics(
            artifact_version,
            time_ms,
            overall_end_time_metrics,
            rts,
            setup_name,
            setup_type,
            test_name,
            tf_github_branch,
            tf_github_org,
            tf_github_repo,
            tf_triggering_env,
            {"metric-type": "test-tag"},
        )
        labels_rts_cmdstats = (
            rts.ts()
            .info(
                "ci.benchmarks.redislabs/env/org/repo/test1/by.version/6.2.3/benchmark_end/setup_name/commandstats_cmdstat_ping_calls"
            )
            .labels
        )
        assert labels_rts_cmdstats["metric-type"] == "test-tag"
        assert labels_rts_cmdstats["command"] == "ping"
        assert labels_rts_cmdstats["command_and_setup"] == "ping - setup_name"
        assert (
            labels_rts_cmdstats["command_and_metric_and_setup"]
            == "ping - calls - setup_name"
        )
        assert (
            labels_rts_cmdstats["command_and_metric_and_setup_and_version"]
            == "ping - calls - setup_name - 6.2.3"
        )
        assert labels_rts_cmdstats["metric"] == "calls"
        assert labels_rts_cmdstats["shard"] == "1"
        assert labels_rts_cmdstats["metric_and_shard"] == "calls"

        labels_rts_cmdstats = (
            rts.ts()
            .info(
                "ci.benchmarks.redislabs/env/org/repo/test1/by.version/6.2.3/benchmark_end/setup_name/commandstats_cmdstat_ping_calls"
            )
            .labels
        )
        assert labels_rts_cmdstats["metric-type"] == "test-tag"
        assert labels_rts_cmdstats["command"] == "ping"
        assert labels_rts_cmdstats["command_and_setup"] == "ping - setup_name"
        assert (
            labels_rts_cmdstats["command_and_metric_and_setup"]
            == "ping - calls - setup_name"
        )
        assert (
            labels_rts_cmdstats["command_and_metric_and_setup_and_version"]
            == "ping - calls - setup_name - 6.2.3"
        )
        assert labels_rts_cmdstats["metric"] == "calls"
        assert labels_rts_cmdstats["shard"] == "1"
        assert labels_rts_cmdstats["metric_and_shard"] == "calls"

        # by branch
        labels_rts_cmdstats = (
            rts.ts()
            .info(
                "ci.benchmarks.redislabs/env/org/repo/test1/by.branch/master/benchmark_end/setup_name/commandstats_cmdstat_ping_calls"
            )
            .labels
        )
        assert labels_rts_cmdstats["metric-type"] == "test-tag"
        assert labels_rts_cmdstats["command"] == "ping"
        assert labels_rts_cmdstats["command_and_setup"] == "ping - setup_name"
        assert (
            labels_rts_cmdstats["command_and_metric_and_setup"]
            == "ping - calls - setup_name"
        )
        assert (
            labels_rts_cmdstats["command_and_metric_and_setup_and_branch"]
            == "ping - calls - setup_name - master"
        )
        assert labels_rts_cmdstats["metric"] == "calls"
        assert labels_rts_cmdstats["shard"] == "1"
        assert labels_rts_cmdstats["metric_and_shard"] == "calls"

        #
        assert "ci.benchmarks.redislabs/env/org/repo/test1/by.version/6.2.3/benchmark_end/setup_name/commandstats_cmdstat_ping_calls" in rts.ts().queryindex(
            ["metric-type=test-tag"]
        )
        assert "ci.benchmarks.redislabs/env/org/repo/test1/by.version/6.2.3/benchmark_end/setup_name/commandstats_cmdstat_ping_calls" in rts.ts().queryindex(
            ["command=ping"]
        )
        assert "ci.benchmarks.redislabs/env/org/repo/test1/by.version/6.2.3/benchmark_end/setup_name/latencystats_latency_percentiles_usec_ping_p50" in rts.ts().queryindex(
            ["command=ping"]
        )
        labels_rts_latencystats = (
            rts.ts()
            .info(
                "ci.benchmarks.redislabs/env/org/repo/test1/by.version/6.2.3/benchmark_end/setup_name/latencystats_latency_percentiles_usec_ping_p50"
            )
            .labels
        )
        assert labels_rts_latencystats["metric-type"] == "test-tag"
        assert labels_rts_latencystats["command"] == "ping"
        assert labels_rts_latencystats["command_and_setup"] == "ping - setup_name"
        assert labels_rts_latencystats["metric"] == "p50"
        assert labels_rts_latencystats["shard"] == "1"
        assert labels_rts_latencystats["metric_and_shard"] == "p50"
        assert datapoint_errors == 0
        assert datapoint_inserts == (2 * len(list(overall_end_time_metrics.keys())))
        tf_github_branch = "master"
        datapoint_errors, datapoint_inserts = export_redis_metrics(
            artifact_version,
            time_ms,
            overall_end_time_metrics,
            rts,
            setup_name,
            setup_type,
            test_name,
            tf_github_branch,
            tf_github_org,
            tf_github_repo,
            tf_triggering_env,
        )
        assert datapoint_errors == 0
        assert datapoint_inserts == (2 * len(list(overall_end_time_metrics.keys())))

    except redis.exceptions.ConnectionError:
        pass


def test_run_remote_mixed_env_no_leak():
    """Regression test for the `mixed -> mixed` env-leak bug, in remote mode.

    Plan:
      - mixed-load.yml   (benchmark_type=mixed,     dataset=mixed-env-leak)
      - mixed-load2.yml  (benchmark_type=mixed,     dataset=mixed-env-leak)
      - read-query.yml   (benchmark_type=read-only, dataset=mixed-env-leak)

    Pre-fix, the second mixed benchmark inherited the populated
    `setup_details["env"]` from the first and was routed through
    `ro_benchmark_reuse`, which asserts `benchmark_type == "read-only"`.
    Post-fix, each mixed test spins up fresh; the read-only test inherits the
    env from the last mixed test via `shared_env`.

    Same infrastructure requirements as test_run_remote_dataset_reuse_memtier:
    RUN_REMOTE_TESTS=1 plus either AWS credentials or pre-deployed inventory.
    """
    if os.getenv("RUN_REMOTE_TESTS", "0") != "1":
        pytest.skip("Remote tests disabled. Set RUN_REMOTE_TESTS=1 to enable.")

    db_server_ip = os.getenv("DB_SERVER_HOST", None)
    client_server_ip = os.getenv("CLIENT_SERVER_HOST", None)
    private_key_path = os.getenv(
        "EC2_PRIVATE_PEM", "./tests/test_data/test-ssh/tox_rsa"
    )

    has_inventory = db_server_ip is not None and client_server_ip is not None
    has_aws_credentials, _ = check_ec2_env()

    if not has_inventory and not has_aws_credentials:
        pytest.skip(
            "This test requires either pre-deployed inventory "
            "(DB_SERVER_HOST, CLIENT_SERVER_HOST) or AWS credentials "
            "(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION)"
        )

    parser = argparse.ArgumentParser(
        description="test",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser = create_run_remote_arguments(parser)

    args_list = [
        "--test-glob",
        "./tests/test_data/mixed_env_leak/*.yml",
        "--skip-env-vars-verify",
    ]

    if has_inventory:
        args_list.extend(
            [
                "--inventory",
                f"server_private_ip={db_server_ip},server_public_ip={db_server_ip},client_public_ip={client_server_ip}",
                "--private_key",
                private_key_path,
            ]
        )

    args = parser.parse_args(args=args_list)

    try:
        run_remote_command_logic(args, "tool", "v0")
    except SystemExit as e:
        assert e.code == 0, (
            f"run_remote_command_logic exited with code {e.code} — the "
            "mixed -> mixed env leak likely tripped the "
            "ro_benchmark_reuse `assert benchmark_type == 'read-only'` "
            "invariant."
        )


def test_run_remote_dataset_reuse_memtier():
    """
    Test that benchmarks with the same dataset_name are grouped together
    for dataset reuse optimization when running remotely.

    - vanilla-memtier-load.yml: has dataset_name in clientconfig (produces dataset)
    - vanilla-memtier-query.yml: has dataset_name in dbconfig (uses dataset)

    When run together, both are grouped under the same dataset_name "vanilla-memtier",
    enabling the query benchmark to reuse the dataset loaded by the load benchmark.

    This test also verifies:
    - Redis PIDs are the same when reusing the environment (ensuring same Redis instance)
    - Keyspace checks are confirmed when reusing the dataset

    This test requires:
    1. RUN_REMOTE_TESTS=1 environment variable to be set
    2. Either AWS credentials for terraform deployment OR pre-deployed inventory
       (DB_SERVER_HOST, CLIENT_SERVER_HOST)
    """
    # Only run when explicitly enabled
    if os.getenv("RUN_REMOTE_TESTS", "0") != "1":
        pytest.skip("Remote tests disabled. Set RUN_REMOTE_TESTS=1 to enable.")

    # Check for pre-deployed inventory first (faster if available)
    db_server_ip = os.getenv("DB_SERVER_HOST", None)
    client_server_ip = os.getenv("CLIENT_SERVER_HOST", None)
    private_key_path = os.getenv(
        "EC2_PRIVATE_PEM", "./tests/test_data/test-ssh/tox_rsa"
    )

    has_inventory = db_server_ip is not None and client_server_ip is not None

    # Check for AWS credentials for terraform deployment
    has_aws_credentials, _ = check_ec2_env()

    if not has_inventory and not has_aws_credentials:
        pytest.skip(
            "This test requires either pre-deployed inventory "
            "(DB_SERVER_HOST, CLIENT_SERVER_HOST) or AWS credentials "
            "(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION)"
        )

    parser = argparse.ArgumentParser(
        description="test",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser = create_run_remote_arguments(parser)

    # Build args list
    args_list = [
        "--test-glob",
        "./tests/test_data/vanilla-memtier-*.yml",
        "--skip-env-vars-verify",
    ]

    # Add inventory if we have pre-deployed servers
    if has_inventory:
        args_list.extend(
            [
                "--inventory",
                f"server_private_ip={db_server_ip},server_public_ip={db_server_ip},client_public_ip={client_server_ip}",
                "--private_key",
                private_key_path,
            ]
        )

    args = parser.parse_args(args=args_list)

    try:
        run_remote_command_logic(args, "tool", "v0")
    except SystemExit as e:
        assert e.code == 0, f"run_remote_command_logic exited with code {e.code}"


def test_save_env_for_cross_type_reuse_publishes_to_shared_env():
    """`save_env_for_cross_type_reuse` publishes a mixed benchmark's spun-up
    env to `shared_env` so a subsequent read-only group on the same
    `(setup, dataset)` can inherit it via the cross-type handoff."""
    fake_env = {"redis_pids": [1234], "server_plaintext_port": 6379}
    setup_details = {"env": fake_env}
    shared_env = {}

    published = save_env_for_cross_type_reuse(
        benchmark_type="mixed",
        reuse_mixed=True,
        dataset_name="ds1",
        setup_name="oss-standalone",
        setup_details=setup_details,
        shared_env=shared_env,
    )

    assert published is True
    assert shared_env[("ds1", "oss-standalone")] is fake_env
    # The env stays in setup_details too — the dispatcher reads it on the
    # next iteration to decide spin-up vs reuse.
    assert setup_details["env"] is fake_env


def test_save_env_for_cross_type_reuse_read_only_is_noop():
    """Read-only benchmarks do not publish via this path in run_remote: env is
    kept in `setup_details["env"]` so the next read-only test in the group can
    reuse it through the dispatcher's reuse branch."""
    fake_env = {"redis_pids": [1234]}
    setup_details = {"env": fake_env}
    shared_env = {}

    published = save_env_for_cross_type_reuse(
        benchmark_type="read-only",
        reuse_mixed=True,
        dataset_name="ds1",
        setup_name="oss-standalone",
        setup_details=setup_details,
        shared_env=shared_env,
    )

    assert published is False
    assert setup_details["env"] is fake_env
    assert shared_env == {}


def test_save_env_for_cross_type_reuse_mixed_without_reuse_mixed_is_noop():
    """If `reuse_mixed` is False, the cross-type handoff is disabled — neither
    publish happens. Each mixed test is then expected to be torn down between
    iterations via the existing teardown logic (no inter-group reuse to
    preserve)."""
    fake_env = {"redis_pids": [1234]}
    setup_details = {"env": fake_env}
    shared_env = {}

    published = save_env_for_cross_type_reuse(
        benchmark_type="mixed",
        reuse_mixed=False,
        dataset_name="ds1",
        setup_name="oss-standalone",
        setup_details=setup_details,
        shared_env=shared_env,
    )

    assert published is False
    assert setup_details["env"] is fake_env
    assert shared_env == {}


def test_tear_down_previous_mixed_env_clears_and_removes_from_shared_env(monkeypatch):
    """Regression for the `mixed -> mixed` env-leak bug.

    Between two mixed tests sharing `(setup, dataset)`, the previous test's
    remote Redis env must be torn down and `setup_details["env"]` cleared so
    the dispatcher takes the spin-up path for the next mixed test (rather
    than the reuse branch, which asserts `benchmark_type == "read-only"`).
    The env must also be removed from `shared_env` since the Redis it
    references no longer exists."""
    from redisbench_admin.run_remote import run_remote as run_remote_mod

    teardown_calls = []
    monkeypatch.setattr(
        run_remote_mod,
        "shutdown_remote_redis",
        lambda conns, tunnel: teardown_calls.append((conns, tunnel)),
    )

    fake_tunnel = object()
    fake_env = {"redis_conns": ["c1"], "ssh_tunnel": fake_tunnel}
    setup_details = {"env": fake_env}
    env_key = ("ds1", "oss-standalone")
    shared_env = {env_key: fake_env}

    tore_down = run_remote_mod.tear_down_previous_mixed_env_if_needed(
        benchmark_type="mixed",
        reuse_mixed=True,
        setup_details=setup_details,
        shared_env=shared_env,
        env_key=env_key,
        keep_env_and_topo=False,
        skip_remote_db_setup=False,
    )

    assert tore_down is True
    assert teardown_calls == [(["c1"], fake_tunnel)]
    assert setup_details["env"] is None
    assert shared_env == {}


def test_tear_down_previous_mixed_env_noop_on_first_iteration(monkeypatch):
    """First iteration in a mixed group: `setup_details["env"]` is None — no
    previous env to tear down. Helper must be a no-op."""
    from redisbench_admin.run_remote import run_remote as run_remote_mod

    teardown_calls = []
    monkeypatch.setattr(
        run_remote_mod,
        "shutdown_remote_redis",
        lambda conns, tunnel: teardown_calls.append((conns, tunnel)),
    )

    setup_details = {"env": None}
    shared_env = {}

    tore_down = run_remote_mod.tear_down_previous_mixed_env_if_needed(
        benchmark_type="mixed",
        reuse_mixed=True,
        setup_details=setup_details,
        shared_env=shared_env,
        env_key=("ds1", "oss-standalone"),
        keep_env_and_topo=False,
        skip_remote_db_setup=False,
    )

    assert tore_down is False
    assert teardown_calls == []


def test_tear_down_previous_mixed_env_noop_for_read_only(monkeypatch):
    """Read-only types intentionally reuse env within a group (that's the
    original RO optimization); the helper must not interfere."""
    from redisbench_admin.run_remote import run_remote as run_remote_mod

    teardown_calls = []
    monkeypatch.setattr(
        run_remote_mod,
        "shutdown_remote_redis",
        lambda conns, tunnel: teardown_calls.append((conns, tunnel)),
    )

    fake_env = {"redis_conns": ["c1"], "ssh_tunnel": object()}
    setup_details = {"env": fake_env}
    env_key = ("ds1", "oss-standalone")
    shared_env = {env_key: fake_env}

    tore_down = run_remote_mod.tear_down_previous_mixed_env_if_needed(
        benchmark_type="read-only",
        reuse_mixed=True,
        setup_details=setup_details,
        shared_env=shared_env,
        env_key=env_key,
        keep_env_and_topo=False,
        skip_remote_db_setup=False,
    )

    assert tore_down is False
    assert teardown_calls == []
    assert setup_details["env"] is fake_env
    assert shared_env[env_key] is fake_env


def test_tear_down_previous_mixed_env_noop_when_keep_env_and_topo(monkeypatch):
    """`--keep-env-and-topo` is the user explicitly opting out of teardown —
    the helper must respect that even for mixed groups."""
    from redisbench_admin.run_remote import run_remote as run_remote_mod

    teardown_calls = []
    monkeypatch.setattr(
        run_remote_mod,
        "shutdown_remote_redis",
        lambda conns, tunnel: teardown_calls.append((conns, tunnel)),
    )

    fake_env = {"redis_conns": ["c1"], "ssh_tunnel": object()}
    setup_details = {"env": fake_env}
    shared_env = {}

    tore_down = run_remote_mod.tear_down_previous_mixed_env_if_needed(
        benchmark_type="mixed",
        reuse_mixed=True,
        setup_details=setup_details,
        shared_env=shared_env,
        env_key=("ds1", "oss-standalone"),
        keep_env_and_topo=True,
        skip_remote_db_setup=False,
    )

    assert tore_down is False
    assert teardown_calls == []
    assert setup_details["env"] is fake_env
