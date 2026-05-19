import os

import argparse
import redis
import yaml

from redisbench_admin.profilers.pprof import process_pprof_text_to_tabular
from redisbench_admin.run.args import REDIS_7
from redisbench_admin.run_local.args import create_run_local_arguments
from redisbench_admin.run_local.local_helpers import (
    check_benchmark_binaries_local_requirements,
)
from redisbench_admin.profilers.profilers_schema import get_profilers_rts_key_prefix
from redisbench_admin.run_local.run_local import (
    run_local_command_logic,
    save_env_for_cross_type_reuse,
    tear_down_previous_mixed_env_if_needed,
)
from redisbench_admin.run.redistimeseries import datasink_profile_tabular_data
from redisbench_admin.utils.local import get_local_run_full_filename


def test_get_local_run_full_filename_sanitizes_branch_slashes():
    result = get_local_run_full_filename(
        start_time_str="2026-04-07-10-00-00",
        github_branch="feat/memtier/fix",
        test_name="memtier-1",
        setup_name="oss-standalone",
    )
    assert "/" not in result
    assert result == (
        "oss-standalone-2026-04-07-10-00-00-feat-memtier-fix-memtier-1.json"
    )


def test_check_benchmark_binaries_local_requirements():
    filename = "ycsb-redisearch-binding-0.18.0-SNAPSHOT.tar.gz"
    inner_foldername = "ycsb-redisearch-binding-0.18.0-SNAPSHOT"
    binaries_localtemp_dir = "./binaries"
    with open("./tests/test_data/ycsb-config.yml", "r") as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        (
            benchmark_tool,
            which_benchmark_tool,
            benchmark_tool_workdir,
        ) = check_benchmark_binaries_local_requirements(
            benchmark_config, "ycsb", binaries_localtemp_dir
        )
        assert which_benchmark_tool == os.path.abspath(
            "./binaries/ycsb-redisearch-binding-0.18.0-SNAPSHOT/bin/ycsb"
        )
        assert benchmark_tool_workdir == os.path.abspath(
            "./binaries/ycsb-redisearch-binding-0.18.0-SNAPSHOT"
        )
        assert benchmark_tool == "ycsb"


def test_datasink_profile_tabular_data():
    tabular_map = {}
    tabular_map["text"] = process_pprof_text_to_tabular(
        "./tests/test_data/results/profile_oss-standalone__primary-1-of-1__tsbs-scale100_lastpoint_perf:record_2021-09-07-15-13-02.out.pprof.txt",
        "text",
    )

    tabular_map["text-lines"] = process_pprof_text_to_tabular(
        "./tests/test_data/results/profile_oss-standalone__primary-1-of-1__tsbs-scale100_cpu-max-all-1_perf:record_2021-09-07-16-52-16.out.pprof.LOC.txt",
        "text-lines",
    )
    try:
        start_time_str = "2021-09-09"
        test_name = "test1"
        setup_type = "oss-standalone"
        tf_triggering_env = "ci"
        github_branch = "branch-1"
        github_hash = "hash-11312213"
        rts = redis.Redis()
        rts.ping()
        rts.flushall()
        datasink_profile_tabular_data(
            github_branch,
            "org",
            "repo",
            github_hash,
            tabular_map,
            rts,
            setup_type,
            1000,
            start_time_str,
            test_name,
            tf_triggering_env,
        )
        zset_profiles_key_name = get_profilers_rts_key_prefix(
            tf_triggering_env,
            "org",
            "repo",
        )
        #
        assert rts.exists(zset_profiles_key_name)
        assert rts.zcard(zset_profiles_key_name) == 1

        profile_test_suffix = "{start_time_str}:{test_name}/{setup_type}/{github_branch}/{github_hash}".format(
            start_time_str=start_time_str,
            test_name=test_name,
            setup_type=setup_type,
            github_branch=github_branch,
            github_hash=github_hash,
        )
        for pprof_format in ["text", "text-lines"]:
            table_columns_text_key = "{}:{}:columns:text".format(
                pprof_format, profile_test_suffix
            )
            assert rts.exists(table_columns_text_key)
        # assert redis_conn.exists(testcases_setname)
        # assert redis_conn.exists(running_platforms_setname)
        # assert redis_conn.exists(build_variant_setname)

    except redis.exceptions.ConnectionError:
        pass


def test_run_local_command_logic_redis_benchmark():
    parser = argparse.ArgumentParser(
        description="test",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser = create_run_local_arguments(parser)
    args = parser.parse_args(
        args=[
            "--test",
            "./tests/test_data/redis-benchmark-json.yml",
            "--keep_env_and_topo",
        ]
    )
    try:
        run_local_command_logic(args, "tool", "v0")
    except SystemExit as e:
        assert e.code == 0
    finally:
        r = redis.Redis()
        r.ping()
        total_keys = r.info("keyspace")["db0"]["keys"]
        r.shutdown(nosave=True)
        assert total_keys == 1000


def test_run_local_command_logic():
    parser = argparse.ArgumentParser(
        description="test",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser = create_run_local_arguments(parser)
    args = parser.parse_args(
        args=[
            "--test",
            "./tests/test_data/redis-benchmark-vanilla.yml",
            "--redis-7",
            "{}".format(REDIS_7),
        ]
    )
    try:
        run_local_command_logic(args, "tool", "v0")
    except SystemExit as e:
        assert e.code == 0

    ## specify the default properties to load
    ## and limit the allowed envs to oss-standalone
    parser = argparse.ArgumentParser(
        description="test",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser = create_run_local_arguments(parser)
    args = parser.parse_args(
        args=[
            "--test",
            "./tests/test_data/redis-benchmark-vanilla.yml",
            "--defaults_filename",
            "./tests/test_data/common-properties-v0.5.yml",
            "--allowed-envs",
            "oss-standalone",
            "--redis-7",
            "{}".format(REDIS_7),
        ]
    )
    try:
        run_local_command_logic(args, "tool", "v0")
    except SystemExit as e:
        assert e.code == 0

    ## expected to fail on not allowed tool
    parser = argparse.ArgumentParser(
        description="test",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser = create_run_local_arguments(parser)
    args = parser.parse_args(
        args=[
            "--test",
            "./tests/test_data/redis-benchmark-vanilla.yml",
            "--allowed-tools",
            "ftsb_redisearch",
            "--redis-7",
            "{}".format(REDIS_7),
        ]
    )
    exit_raised = False
    try:
        run_local_command_logic(args, "tool", "v0")
    except SystemExit as e:
        exit_raised = True
        assert e.code == 1

    # If SystemExit was not raised, the test should fail
    assert (
        exit_raised
    ), "Expected SystemExit to be raised when tool is not in allowed list"

    ## run while pushing results to redis_conn
    # Check if we have the test DB to store results - if not, skip this test section
    rts_port = os.environ.get("RTS_PORT", None)
    rts_host = os.getenv("RTS_DATASINK_HOST", None)
    rts_pass = ""
    if rts_host is None or rts_port is None:
        return
    rts = redis.Redis(port=rts_port, host=rts_host)
    rts.ping()
    rts.flushall()
    parser = argparse.ArgumentParser(
        description="test",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser = create_run_local_arguments(parser)
    args = parser.parse_args(
        args=[
            "--test",
            "./tests/test_data/redis-benchmark-vanilla.yml",
            "--redistimeseries_host",
            rts_host,
            "--redistimeseries_port",
            "{}".format(rts_port),
            "--push_results_redistimeseries",
            "--redis-7",
            "{}".format(REDIS_7),
        ]
    )
    try:
        run_local_command_logic(args, "tool", "v0")
    except SystemExit as e:
        assert e.code == 0


def test_run_local_mixed_env_no_leak():
    """Regression test: two `mixed` benchmarks sharing (setup, dataset_name)
    plus one `read-only` benchmark must complete without crashing.

    The plan is:
      - mixed-load.yml   (benchmark_type=mixed,     dataset=mixed-env-leak)
      - mixed-load2.yml  (benchmark_type=mixed,     dataset=mixed-env-leak)
      - read-query.yml   (benchmark_type=read-only, dataset=mixed-env-leak)

    The presence of both mixed and read-only flips `reuse_mixed` to True, and
    the two mixed benchmarks land in the same (setup, dataset) group. Pre-fix,
    the second mixed test inherited the first one's populated `setup_details
    ["env"]` and got routed through `ro_benchmark_reuse`, which asserts
    `benchmark_type == "read-only"` and crashed. Post-fix, each mixed test
    spins up its own fresh env; the read-only test reuses the env from the
    last mixed test via `shared_env`.
    """
    parser = argparse.ArgumentParser(
        description="test",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser = create_run_local_arguments(parser)
    args = parser.parse_args(
        args=[
            "--test-glob",
            "./tests/test_data/mixed_env_leak/*.yml",
        ]
    )
    try:
        run_local_command_logic(args, "tool", "v0")
    except SystemExit as e:
        assert e.code == 0, (
            f"run_local_command_logic exited with code {e.code} — the "
            "mixed -> mixed env leak likely tripped the "
            "ro_benchmark_reuse `assert benchmark_type == 'read-only'` "
            "invariant."
        )


def test_run_local_dataset_reuse_memtier():
    """
    Test that benchmarks with the same dataset_name are grouped together
    for dataset reuse optimization.

    - vanilla-memtier-load.yml: has dataset_name in clientconfig (produces dataset)
    - vanilla-memtier-query.yml: has dataset_name in dbconfig (uses dataset)

    When run together, both are grouped under the same dataset_name "vanilla-memtier",
    enabling the query benchmark to reuse the dataset loaded by the load benchmark.
    """
    parser = argparse.ArgumentParser(
        description="test",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser = create_run_local_arguments(parser)
    args = parser.parse_args(
        args=[
            "--test-glob",
            "./tests/test_data/vanilla-memtier-*.yml",
        ]
    )
    try:
        run_local_command_logic(args, "tool", "v0")
    except SystemExit as e:
        assert e.code == 0


def test_run_local_dataset_reuse_not_possible_memtier():
    """
    Test that benchmarks with the same dataset_name are grouped together
    for dataset reuse optimization.

    - vanilla-memtier-load.yml: has dataset_name in clientconfig (produces dataset)
    - vanilla-memtier-query.yml: has dataset_name in dbconfig (uses dataset)

    When run together, both are grouped under the same dataset_name "vanilla-memtier",
    enabling the query benchmark to reuse the dataset loaded by the load benchmark.
    """
    parser = argparse.ArgumentParser(
        description="test",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser = create_run_local_arguments(parser)
    args = parser.parse_args(
        args=[
            "--test-glob",
            "./tests/test_data/vanilla-memtier-query*.yml",
            "--keep_env_and_topo",
        ]
    )
    try:
        run_local_command_logic(args, "tool", "v0")
    except SystemExit as e:
        assert e.code == 0
    finally:
        r = redis.Redis()
        r.ping()
        # After both benchmarks run, we should have 10000 keys loaded by the load benchmark
        # The query benchmark reuses this dataset
        total_keys = r.info("keyspace")["db0"]["keys"]
        r.shutdown(nosave=True)
        assert total_keys == 10000


def test_run_local_dataset_reuse_not_possible_ftsb():
    import glob
    import tempfile

    parser = argparse.ArgumentParser(
        description="test",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser = create_run_local_arguments(parser)
    args = parser.parse_args(
        args=[
            "--test-glob",
            "./tests/test_data/vanilla-ftsb-query*.yml",
            "--keep_env_and_topo",
        ]
    )
    try:
        run_local_command_logic(args, "tool", "v0")
    except SystemExit as e:
        assert e.code == 0
    finally:
        r = redis.Redis()
        r.ping()
        # After both benchmarks run, we should have 10000 keys loaded by the load benchmark
        # The query benchmark reuses this dataset
        total_keys = r.info("keyspace")["db0"]["keys"]
        r.shutdown(nosave=True)
        assert total_keys == 100

    # Verify that ftsb log files were created in temp directory
    temp_dir = tempfile.gettempdir()
    log_files = glob.glob(os.path.join(temp_dir, "**/load-data.log"), recursive=True)
    assert len(log_files) > 0, "Expected ftsb log files to be created"
    # Verify log file is not empty
    for log_file in log_files:
        assert os.path.getsize(log_file) > 0, f"Log file {log_file} should not be empty"


def test_run_local_dataset_reuse_ftsb():
    parser = argparse.ArgumentParser(
        description="test",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser = create_run_local_arguments(parser)
    args = parser.parse_args(
        args=[
            "--test-glob",
            "./tests/test_data/vanilla-ftsb-*.yml",
        ]
    )
    try:
        run_local_command_logic(args, "tool", "v0")
    except SystemExit as e:
        assert e.code == 0


def test_save_env_for_cross_type_reuse_publishes_to_shared_env():
    """`save_env_for_cross_type_reuse` publishes the spun-up env to `shared_env`
    when `reuse_mixed=True`, so a subsequent group on the same
    `(setup, dataset)` can inherit it via the cross-type handoff."""
    fake_env = {"redis_pids": [1234], "redis_conns": []}
    setup_details = {"env": fake_env}
    shared_env = {}
    env_key = ("ds1", "oss-standalone")

    published = save_env_for_cross_type_reuse(
        reuse_mixed=True,
        env_key=env_key,
        setup_details=setup_details,
        shared_env=shared_env,
    )

    assert published is True
    assert shared_env[env_key] is fake_env
    # The env stays in setup_details too — the dispatcher reads it on the
    # next iteration to decide spin-up vs reuse.
    assert setup_details["env"] is fake_env


def test_save_env_for_cross_type_reuse_disabled_is_noop():
    """When `reuse_mixed` is False, no publish to shared_env happens."""
    fake_env = {"redis_pids": [1234], "redis_conns": []}
    setup_details = {"env": fake_env}
    shared_env = {}
    env_key = ("ds1", "oss-standalone")

    published = save_env_for_cross_type_reuse(
        reuse_mixed=False,
        env_key=env_key,
        setup_details=setup_details,
        shared_env=shared_env,
    )

    assert published is False
    assert setup_details["env"] is fake_env
    assert shared_env == {}


def test_tear_down_previous_mixed_env_clears_and_removes_from_shared_env(monkeypatch):
    """Regression for the `mixed -> mixed` env-leak bug.

    Between two mixed tests sharing `(setup, dataset)`, the previous test's
    Redis env must be torn down and `setup_details["env"]` cleared so the
    dispatcher takes the spin-up path for the next mixed test (rather than
    the reuse branch, which asserts `benchmark_type == "read-only"`). The
    env must also be removed from `shared_env` since the Redis it references
    no longer exists."""
    from redisbench_admin.run_local import run_local as run_local_mod

    teardown_calls = []
    monkeypatch.setattr(
        run_local_mod,
        "teardown_local_setup",
        lambda conns, procs, name: teardown_calls.append((conns, procs, name)),
    )

    fake_env = {"redis_pids": [1234], "redis_conns": ["c1"], "redis_processes": ["p1"]}
    setup_details = {"env": fake_env}
    env_key = ("ds1", "oss-standalone")
    shared_env = {env_key: fake_env}

    tore_down = run_local_mod.tear_down_previous_mixed_env_if_needed(
        benchmark_type="mixed",
        reuse_mixed=True,
        setup_details=setup_details,
        shared_env=shared_env,
        env_key=env_key,
        keep_env_and_topo=False,
    )

    assert tore_down is True
    assert teardown_calls == [(["c1"], ["p1"], "previous-mixed")]
    assert setup_details["env"] is None
    assert shared_env == {}


def test_tear_down_previous_mixed_env_noop_on_first_iteration(monkeypatch):
    """First iteration in a mixed group: `setup_details["env"]` is None — no
    previous env to tear down. Helper must be a no-op."""
    from redisbench_admin.run_local import run_local as run_local_mod

    teardown_calls = []
    monkeypatch.setattr(
        run_local_mod,
        "teardown_local_setup",
        lambda conns, procs, name: teardown_calls.append((conns, procs, name)),
    )

    setup_details = {"env": None}
    shared_env = {}

    tore_down = run_local_mod.tear_down_previous_mixed_env_if_needed(
        benchmark_type="mixed",
        reuse_mixed=True,
        setup_details=setup_details,
        shared_env=shared_env,
        env_key=("ds1", "oss-standalone"),
        keep_env_and_topo=False,
    )

    assert tore_down is False
    assert teardown_calls == []


def test_tear_down_previous_mixed_env_noop_for_read_only(monkeypatch):
    """Read-only types intentionally reuse env within a group (that's the
    original RO optimization); the helper must not interfere."""
    from redisbench_admin.run_local import run_local as run_local_mod

    teardown_calls = []
    monkeypatch.setattr(
        run_local_mod,
        "teardown_local_setup",
        lambda conns, procs, name: teardown_calls.append((conns, procs, name)),
    )

    fake_env = {"redis_pids": [1234], "redis_conns": ["c1"], "redis_processes": ["p1"]}
    setup_details = {"env": fake_env}
    env_key = ("ds1", "oss-standalone")
    shared_env = {env_key: fake_env}

    tore_down = run_local_mod.tear_down_previous_mixed_env_if_needed(
        benchmark_type="read-only",
        reuse_mixed=True,
        setup_details=setup_details,
        shared_env=shared_env,
        env_key=env_key,
        keep_env_and_topo=False,
    )

    assert tore_down is False
    assert teardown_calls == []
    assert setup_details["env"] is fake_env
    assert shared_env[env_key] is fake_env


def test_tear_down_previous_mixed_env_noop_when_keep_env_and_topo(monkeypatch):
    """`--keep-env-and-topo` is the user explicitly opting out of teardown —
    the helper must respect that even for mixed groups."""
    from redisbench_admin.run_local import run_local as run_local_mod

    teardown_calls = []
    monkeypatch.setattr(
        run_local_mod,
        "teardown_local_setup",
        lambda conns, procs, name: teardown_calls.append((conns, procs, name)),
    )

    fake_env = {"redis_pids": [1234], "redis_conns": ["c1"], "redis_processes": ["p1"]}
    setup_details = {"env": fake_env}
    shared_env = {}

    tore_down = run_local_mod.tear_down_previous_mixed_env_if_needed(
        benchmark_type="mixed",
        reuse_mixed=True,
        setup_details=setup_details,
        shared_env=shared_env,
        env_key=("ds1", "oss-standalone"),
        keep_env_and_topo=True,
    )

    assert tore_down is False
    assert teardown_calls == []
    assert setup_details["env"] is fake_env
