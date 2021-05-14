import yaml

from redisbench_admin.run.tsbs_run_queries_redistimeseries.tsbs_run_queries_redistimeseries import (
    prepare_tsbs_benchmark_command,
)


def test_prepare_tsbs_benchmark_command():
    with open("./tests/test_data/tsbs-scale100-cpu-max-all-1.yml", "r") as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        is_remote = False
        command_arr, command_str = prepare_tsbs_benchmark_command(
            "tsbs_load_redistimeseries",
            "localhost",
            6379,
            benchmark_config,
            ".",
            "/tmp/result.json",
            "/tmp/data.json",
            is_remote,
        )
        assert (
            command_str
            == "tsbs_load_redistimeseries --host localhost:6379 --results-file /tmp/result.json"
        )
        is_remote = True
        for k in benchmark_config["clientconfig"]:
            if "parameters" in k:
                command_arr, command_str = prepare_tsbs_benchmark_command(
                    "tsbs_load_redistimeseries",
                    "localhost",
                    6379,
                    k,
                    ".",
                    "/tmp/result.json",
                    "/tmp/data.json",
                    is_remote,
                )
                assert (
                    command_str
                    == "tsbs_load_redistimeseries --host localhost:6379 --workers 64 --file /tmp/data.json --results-file /tmp/result.json"
                )
