import yaml

from redisbench_admin.run.redisgraph_benchmark_go.redisgraph_benchmark_go import (
    prepare_redisgraph_benchmark_go_command,
)


def test_prepare_redis_graph_benchmark_go_command():
    with open("./tests/test_data/redisgraph-benchmark-go.yml", "r") as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        for k in benchmark_config["clientconfig"]:
            if "parameters" in k:
                command_arr, command_str = prepare_redisgraph_benchmark_go_command(
                    "redisgraph-benchmark-go", "localhost", "6380", k, "result.txt"
                )
                assert (
                    command_str
                    == "redisgraph-benchmark-go -graph-key g -rps 0 -c 32 -n 1000000 -query MATCH (n) WHERE ID(n) = 0 SET n.v = n.v + 1 -query-ratio 1 -h localhost -p 6380 -json-out-file result.txt"
                )


def test_prepare_redis_graph_benchmark_go_new_command():
    with open("./tests/test_data/redisgraph-benchmark-go_2.yml", "r") as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        for k in benchmark_config["clientconfig"]:
            if "parameters" in k:
                command_arr, command_str = prepare_redisgraph_benchmark_go_command(
                    "redisgraph-benchmark-go", "localhost", "6380", k, "result.txt"
                )
                assert (
                    command_str
                    == "redisgraph-benchmark-go -graph-key graph500-scale18-ef16 -rps 0 -c 32 -n 10000 -random-int-max 262016 -random-seed 12345 -query MATCH (n)-[:IS_CONNECTED]->(z) WHERE ID(n) = __rand_int__ RETURN ID(n), count(z) -query-ratio 0.75 -query CYPHER Id1=__rand_int__ Id2=__rand_int__ MATCH (n1:Node {id: $Id1}) MATCH (n1:Node {id: $Id2}) MERGE (n1)-[rel:IS_CONNECTED]->(n2) -query-ratio 0.25 -h localhost -p 6380 -json-out-file result.txt"
                )
