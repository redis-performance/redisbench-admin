[![codecov](https://codecov.io/gh/RedisLabsModules/redisbench-admin/branch/master/graph/badge.svg)](https://codecov.io/gh/RedisLabsModules/redisbench-admin)
![Actions](https://github.com/RedisLabsModules/redisbench-admin/workflows/Run%20Tests/badge.svg?branch=master)
![Actions](https://badge.fury.io/py/redisbench-admin.svg)

# [redisbench-admin](https://github.com/RedisLabsModules/redisbench-admin)

Redis benchmark run helper can help you with the following tasks:

- Setup abd teardown of benchmarking infrastructure specified
  on [RedisLabsModules/testing-infrastructure](https://github.com/RedisLabsModules/testing-infrastructure)
- Setup and teardown of an Redis and Redis Modules DBs for benchmarking
- Management of benchmark data and specifications across different setups
- Running benchmarks and recording results
- Exporting performance results in several formats (CSV, RedisTimeSeries, JSON)
- Finding on-cpu, off-cpu, io, and threading performance problems by attaching profiling tools/probers ( perf (a.k.a. perf_events), bpf tooling, vtune )
- **[SOON]** Finding performance problems by attaching telemetry probes

Current supported benchmark tools:

- [redis-benchmark](https://github.com/redis/redis)
- [memtier_benchmark](https://github.com/RedisLabs/memtier_benchmark)
- [redis-benchmark-go](https://github.com/filipecosta90/redis-benchmark-go)
- [YCSB](https://github.com/RediSearch/YCSB)
- [tsbs](https://github.com/RedisTimeSeries/tsbs)
- [redisgraph-benchmark-go](https://github.com/RedisGraph/redisgraph-benchmark-go)
- [ftsb_redisearch](https://github.com/RediSearch/ftsb)
- [SOON][aibench](https://github.com/RedisAI/aibench)

## Installation

Installation is done using pip, the package installer for Python, in the following manner:

```bash
python3 -m pip install redisbench-admin
```

## Development

1. Install [pypoetry](https://python-poetry.org/) to manage your dependencies and trigger tooling.
```sh
pip install poetry
```

2. Installing dependencies from lock file

```
poetry install
```

### Running formaters

```sh
poetry run black .
```


### Running linters

```sh
poetry run flake8
```


### Running tests

A simple test suite is provided, and can be run with:

```sh
$ poetry run pytest
```

## License

redisbench-admin is distributed under the BSD3 license - see [LICENSE](LICENSE)
