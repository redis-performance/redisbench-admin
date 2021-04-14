import yaml

from redisbench_admin.run_local.run_local import checkBenchmarkBinariesLocalRequirements

def test_check_benchmark_binaries_local_requirements():
    filename = "ycsb-redisearch-binding-0.18.0-SNAPSHOT.tar.gz"
    inner_foldername = "ycsb-redisearch-binding-0.18.0-SNAPSHOT"
    binaries_localtemp_dir = "./binaries"
    with open("./tests/test_data/ycsb-config.yml", "r") as yml_file:
        benchmark_config = yaml.safe_load(yml_file)
        benchmark_tool,which_benchmark_tool = checkBenchmarkBinariesLocalRequirements(benchmark_config, "ycsb", binaries_localtemp_dir)
        assert which_benchmark_tool == "./binaries/ycsb-redisearch-binding-0.18.0-SNAPSHOT/bin/ycsb"
        assert benchmark_tool == "ycsb"