import argparse


from redisbench_admin.run_local.args import create_run_local_arguments
from redisbench_admin.run_local.run_local import (
    run_local_command_logic,
)


def test_run_local_command_logic_oss_cluster():
    ## specify the default properties to load
    ## sping a 3 primaries cluster and test it
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
            "oss-cluster",
            "--port",
            "12000",
        ]
    )
    try:
        run_local_command_logic(args, "tool", "v0")
    except SystemExit as e:
        assert e.code == 0
