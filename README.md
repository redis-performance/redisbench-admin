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
- [SOON] Comparing performance results
- [SOON] Finding performance problems by attaching telemetry probes

Current supported benchmark tools:

- [redisgraph-benchmark-go](https://github.com/RedisGraph/redisgraph-benchmark-go)
- [ftsb_redisearch](https://github.com/RediSearch/ftsb)
- [redis-benchmark](https://github.com/redis/redis)
- [YCSB](https://github.com/RediSearch/YCSB)
- [SOON][memtier_benchmark](https://github.com/RedisLabs/memtier_benchmark)
- [SOON][aibench](https://github.com/RedisAI/aibench)
- [SOON][redis-benchmark-go](https://github.com/filipecosta90/redis-benchmark-go)

## Installation

Installation is done using pip, the package installer for Python, in the following manner:

```bash
python3 -m pip install redisbench-admin
```

## Development

### Running tests

A simple test suite is provided, and can be run with:

```sh
$ poetry run pytest
```

## License

redisbench-admin is distributed under the BSD3 license - see [LICENSE](LICENSE)
