import shutil

import yaml

from redisbench_admin.run.redis_benchmark.redis_benchmark import (
    prepare_redis_benchmark_command,
    redis_benchmark_ensure_min_version_local,
    redis_benchmark_from_stdout_csv_to_json,
)


def test_prepare_redis_benchmark_command():
    with open("./tests/test_data/redisgraph-benchmark-go.yml", "r") as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        for k in benchmark_config["clientconfig"]:
            if "parameters" in k:
                command_arr, command_str = prepare_redis_benchmark_command(
                    "redis-benchmark", "localhost", "6380", k
                )
                assert (
                    command_str
                    == "redis-benchmark -h localhost -p 6380 --csv -e -c 32 --threads 4 -n 1000000"
                )


def test_prepare_redis_benchmark_command_x():
    with open("./tests/test_data/redis-benchmark-json.yml", "r") as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        for k in benchmark_config["clientconfig"]:
            if "parameters" in k:
                command_arr, command_str = prepare_redis_benchmark_command(
                    "redis-benchmark",
                    "localhost",
                    "6380",
                    k,
                    False,
                    ".",
                )
                assert command_str.startswith(
                    "redis-benchmark -h localhost -p 6380 --csv -e -c 16 -n 50000 --threads 2 -P 1 -r 1000 SET __rand_int__ '"
                )


def test_prepare_redis_benchmark_command():
    with open("./tests/test_data/redis-benchmark2.yml", "r") as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        for k in benchmark_config["clientconfig"]:
            if "parameters" in k:
                command_arr, command_str = prepare_redis_benchmark_command(
                    "redis-benchmark", "localhost", "6380", k, False, None
                )
                assert (
                    command_str
                    == "redis-benchmark -h localhost -p 6380 --csv -e -c 16 -n 1000000000 --threads 2 -P 1 -r 100000 -d 128 bf.add test __rand_int__"
                )


def test_redis_benchmark_ensure_min_version_local():
    redis_benchmark_bin = shutil.which("redis-benchmark")
    if redis_benchmark_bin:
        try:
            redis_benchmark_ensure_min_version_local(
                redis_benchmark_bin, "6.2.0", "6", "2", "0"
            )
        except Exception:
            pass


def test_redis_benchmark_from_stdout_csv_to_json():
    with open("./tests/test_data/redis-benchmark-6.2.0-csv.out", "r") as csv_file:
        csv_data = csv_file.read()
        results_dict = redis_benchmark_from_stdout_csv_to_json(csv_data, 1, "1")
        assert "SET" in results_dict["Tests"]
        assert "GET" in results_dict["Tests"]
