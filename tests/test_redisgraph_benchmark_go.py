import yaml

from redisbench_admin.run.redisgraph_benchmark_go.redisgraph_benchmark_go import prepareRedisGraphBenchmarkGoCommand


def test_prepare_redis_graph_benchmark_go_command():
    with open("./tests/test_data/redisgraph-benchmark-go.yml", "r") as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        for k in benchmark_config["clientconfig"]:
            if 'parameters' in k:
                command_arr, command_str = prepareRedisGraphBenchmarkGoCommand("redisgraph-benchmark-go", "localhost", "6380", k, "result.txt")
                assert command_str == "redisgraph-benchmark-go -graph-key g -rps 0 -c 32 -n 1000000 -query MATCH (n) WHERE ID(n) = 0 SET n.v = n.v + 1 -query-ratio 1 -h localhost -p 6380 -json-out-file result.txt"
