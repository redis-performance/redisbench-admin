[![codecov](https://codecov.io/gh/filipecosta90/redisbench-admin/branch/master/graph/badge.svg)](https://codecov.io/gh/filipecosta90/redisbench-admin)
![Actions](https://github.com/filipecosta90/redisbench-admin/workflows/Run%20Tests/badge.svg?branch=master)
![Actions](https://badge.fury.io/py/redisbench-admin.svg)

# redisbench-admin

Redis benchmark run helper can help you with the following tasks:

- Setup and teardown of an Redis and Redis Modules DBs for benchmarking
- Management of benchmark data and specifications across different setups
- Running benchmarks and recording results
- Comparing performance results
- Exporting performance results in several formats (CSV, RedisTimeSeries, JSON)
- [SOON] Finding performance problems by attaching telemetry probes

Current supported benchmark tools:
- [redisgraph-database-benchmark](https://github.com/RedisGraph/graph-database-benchmark/tree/master/benchmark/redisgraph)
- [ftsb_redisearch](https://github.com/RediSearch/ftsb)

** future versions will also support redis-benchmark and memtier_benchmark.

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
