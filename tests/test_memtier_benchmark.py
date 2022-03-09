#  BSD 3-Clause License
#
#  Copyright (c) 2022., Redis Labs Modules
#  All rights reserved.
#
import yaml

from redisbench_admin.run.memtier_benchmark.memtier_benchmark import (
    prepare_memtier_benchmark_command,
)


def test_prepare_memtier_benchmark_command():
    with open("./tests/test_data/vecsim-memtier.yml", "r") as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        command_arr, command_str = prepare_memtier_benchmark_command(
            "memtier_benchmark",
            "localhost",
            "6380",
            benchmark_config["clientconfig"],
            False,
            "result.json",
        )
        assert (
            command_str
            == "memtier_benchmark -s localhost -p 6380 --hide-histogram --json-out-file result.json --command \"FT.SEARCH idx 'text0=>[KNN $k @hnsw_vector $BLOB]' PARAMS 4 k 10 BLOB aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\" --test-time 180 -c 8 -t 2 --hide-histogram"
        )
        assert (
            command_arr[9]
            == "FT.SEARCH idx 'text0=>[KNN $k @hnsw_vector $BLOB]' PARAMS 4 k 10 BLOB aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        )
        assert len(command_arr) == 17
