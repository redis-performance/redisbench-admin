[![codecov](https://codecov.io/gh/filipecosta90/redisbench-admin/branch/master/graph/badge.svg)](https://codecov.io/gh/filipecosta90/redisbench-admin)
![Actions](https://github.com/filipecosta90/redisbench-admin/workflows/Run%20Tests/badge.svg?branch=master)
![Actions](https://badge.fury.io/py/redisbench-admin.svg)

# redisbench-admin
Redis benchmark run helper. An automation wrapper around:
- [redisgraph-database-benchmark](https://github.com/RedisGraph/graph-database-benchmark/tree/master/benchmark/redisgraph)
- [ftsb_redisearch](https://github.com/RediSearch/ftsb)

** future versions will also support redis-benchmark and memtier_benchmark.

## Installation

Installation is done using pip, the package installer for Python, in the following manner:

```bash
python3 -m pip install redisbench-admin
```

## Overview

TBD

### Running tests

A simple test suite is provided, and can be run with:

```sh
$ poetry run pytest
```

## License

redisbench-admin is distributed under the BSD3 license - see [LICENSE](LICENSE)
