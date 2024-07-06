[![codecov](https://codecov.io/gh/redis-performance/redisbench-admin/branch/master/graph/badge.svg)](https://codecov.io/gh/redis-performance/redisbench-admin)
![Actions](https://github.com/redis-performance/redisbench-admin/workflows/Run%20Tests/badge.svg?branch=master)
![Actions](https://badge.fury.io/py/redisbench-admin.svg)

# [redisbench-admin](https://github.com/redis-performance/redisbench-admin)

Redis benchmark run helper can help you with the following tasks:

- Setup abd teardown of benchmarking infrastructure specified
  on [redis-performance/testing-infrastructure](https://github.com/redis-performance/testing-infrastructure)
- Setup and teardown of an Redis and Redis Modules DBs for benchmarking
- Management of benchmark data and specifications across different setups
- Running benchmarks and recording results
- Exporting performance results in several formats (CSV, RedisTimeSeries, JSON)
- Finding on-cpu, off-cpu, io, and threading performance problems by attaching profiling tools/probers ( perf (a.k.a. perf_events), bpf tooling, vtune )
- **[SOON]** Finding performance problems by attaching telemetry probes

Current supported benchmark tools:

- [redis-benchmark](https://github.com/redis/redis)
- [memtier_benchmark](https://github.com/RedisLabs/memtier_benchmark)
- [redis-benchmark-go](https://github.com/redis-performance/redis-benchmark-go)
- [YCSB](https://github.com/RediSearch/YCSB)
- [tsbs](https://github.com/RedisTimeSeries/tsbs)
- [redisgraph-benchmark-go](https://github.com/RedisGraph/redisgraph-benchmark-go)
- [ftsb_redisearch](https://github.com/RediSearch/ftsb)
- [ann-benchmarks](https://github.com/RedisAI/ann-benchmarks)

## Installation

Installation is done using pip, the package installer for Python, in the following manner:

```bash
python3 -m pip install redisbench-admin
```

## Profiler daemon

You can use the profiler daemon by itself in the following manner. 
On the target machine do as follow:

```bash
pip3 install --upgrade pip
pip3 install redisbench-admin --ignore-installed PyYAML

# install perf
apt install linux-tools-common linux-tools-generic linux-tools-`uname -r` -y

# ensure perf is working
perf --version

# install awscli
snap install aws-cli --classic


# configure aws
aws configure

# start the perf-daemon
perf-daemon start
WARNING:root:Unable to detected github_actor. caught the following error: No section: 'user'
Writting log to /tmp/perf-daemon.log
Starting perf-daemon. PID file /tmp/perfdaemon.pid. Daemon workdir: /root/RedisGraph

# check daemon is working appropriatelly
curl localhost:5000/ping

# start a profile
curl -X POST localhost:5000/profiler/perf/start/<pid to profile>

# stop a profile
curl -X POST -d '{"aws_access_key_id":$AWS_ACCESS_KEY_ID,"aws_secret_access_key":$AWS_SECRET_ACCESS_KEY}' localhost:5000/profiler/perf/stop/<pid to profile>
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

A test suite is provided, and can be run with:

```sh
$ tox
```

To run a specific test:
```sh
$ tox -- tests/test_redistimeseries.py
```

To run a specific test with verbose logging:

```sh
# tox -- -vv --log-cli-level=INFO tests/test_run.py
```

## License

redisbench-admin is distributed under the BSD3 license - see [LICENSE](LICENSE)
