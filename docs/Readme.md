
# Context

The automated benchmark definitions provides a framework for evaluating and comparing feature branches and catching regressions prior letting them into the master branch.

To be able to run local benchmarks you need `redisbench_admin>=0.1.64` [[tool repo for full details](https://github.com/RedisLabsModules/redisbench-admin)] and the benchmark tool specified on each configuration file . You can install redisbench-admin via PyPi as any other package. 
```
pip3 install redisbench_admin>=0.1.64
```

**Note:** to be able to run remote benchmarks triggered from your machine you need `terraform`.

Bellow, we dive deeper into the benchmark definition file.

# Benchmark definition

Each benchmark requires a benchmark definition yaml file to present on the current directory. 
A benchmark definition will then consist of:

- optional db configuration (`dbconfig`) with the proper dataset definition. If no db config is passed then no dataset is loaded during the system setup. You can specify both local ( path to rdb ) and remote rdb files ( example: ` "https://s3.amazonaws.com/benchmarks.redislabs/redistimeseries/tsbs/datasets/devops/functional/scale-100-redistimeseries_data.rdb"`. As soon as you run the benchmark one on your machine the remote URL is translated to a local path `./datasets/<filename>` within the benchmarks folder. **Please do not push large RDB files to git!!!.**

- mandatory client configuration (`clientconfig`) specifing the parameters to pass to the benchmark tool tool. The properties allowed here are: `tool`, `min-tool-version`, `tool_source`, `parameters`. If you don't have the required tools and the `tool_source` property is specified then the benchmark client will be downloaded once to a local path `./binaries/<tool>`. 

- optional ci remote definition (`remote`), with the proper terraform deployment configurations definition. The properties allowed here are `type` and `setup`. Both properties are used to find the proper benchmark specification folder within [RedisLabsModules/testing-infrastructure](https://github.com/RedisLabsModules/testing-infrastructure). As an example, if you specify ` - type: oss-standalone` and `- setup: redistimeseries-m5` the used terraform setup will be described by the setup at [`testing-infrastructure/tree/terraform/oss-standalone-redistimeseries-m5`](https://github.com/RedisLabsModules/testing-infrastructure/tree/master/terraform/oss-standalone-redistimeseries-m5)

- optional KPIs definition (`kpis`), specifying the target upper or lower bounds for each relevant performance metric. If specified the KPIs definitions constraints the tests passing/failing. 

- optional metric exporters definition ( `exporter`: currently only `redistimeseries`), specifying which metrics to parse after each benchmark run and push to remote stores.

Sample benchmark definition:
```yml
version: 0.2
name: "json_get_sub_doc_pass_100_json"
description: "JSON.GET pass-100 sub_doc.sclr || {Full document: pass-100.json} https://oss.redislabs.com/redisjson/performance/"
remote:
 - type: oss-standalone
 - setup: redisearch-m5d
dbconfig:
  - dataset: "https://s3.amazonaws.com/benchmarks.redislabs/redisjson/performance.docs/performance.docs.rdb"
clientconfig:
  - tool: redis-benchmark
  - min-tool-version: "6.2.0"
  - parameters:
    - clients: 16
    - requests: 2000000
    - threads: 2
    - pipeline: 1
    - command: 'JSON.GET pass-100 sub_doc.sclr'
exporter:
  redistimeseries:
    break_by:
      - version
      - commit
    timemetric: "$.StartTime"
    metrics:
      - "$.Tests.Overall.rps"
      - "$.Tests.Overall.avg_latency_ms"
      - "$.Tests.Overall.p50_latency_ms"
      - "$.Tests.Overall.p95_latency_ms"
      - "$.Tests.Overall.p99_latency_ms"
      - "$.Tests.Overall.max_latency_ms"
      - "$.Tests.Overall.min_latency_ms"

```

# Running benchmarks

The benchmark automation currently allows running benchmarks in various environments:

- completely locally, if the framework is supported on the local system.

- on AWS, distributing the tasks to multiple EC2 instances as defined on each benchmark specification. To run a benchmark on AWS you additionally need to have a configured AWS account. The application is using the boto3 Python package to exchange files through S3 and create EC2 instances. Triggering of this type of benchmarks can be done from a local machine or via CI on each push to the repo. The results visualization utilities and credentials should have been provide to each team member.

## Run benchmarks locally 

To run a benchmark locally call the `make benchmark` rule.
The `redisbench-admin` tool will detect if all requirements are set and if not will download the required benchmark utilities. 

## Run benchmarks remotely on steady stable VMs with sustained performance

To run a benchmark remotely call  `make benchmark REMOTE=1`. 

Some considerations:
- To run a benchmark on AWS you additionally need to have a configured AWS account. You can easily configure it by having the `AWS_ACCESS_KEY_ID`, `AWS_DEFAULT_REGION`, `AWS_SECRET_ACCESS_KEY` variables set.
- You are required to have EC2 instances private key used to connect to the created EC2 instances set via the `EC2_PRIVATE_PEM` environment variable. 
- The git sha, git actor, git org, git repo, and git branch information are required to properly deploy the required EC2 instances. By default that information will be automatically retrieved and can be override by passing the corresponding arguments. 
- Apart from relying on a configured AWS account, the remote benchmarks require terraform to be installed on your local system. Within `./remote/install_deps.sh` you find automation to easily install terraform on linux systems.
- Optionally, at the end of each remote benchmark you push the results json file to the `ci.benchmarks.redislabs` S3 bucket. The pushed results will have a public read ACL. 
- Optionally, at the end of each remote benchmark you can chose the export the key metrics of the benchmark definition to a remote storage like RedisTimeSeries. To do so, you will need the following env variables defined (`PERFORMANCE_RTS_AUTH`, `PERFORMANCE_RTS_HOST`, `PERFORMANCE_RTS_PORT`) or to pass the corresponding arguments.
- By default all benchmark definitions will be run.
- Each benchmark definition will spawn one or multiple EC2 instances as defined on each benchmark specification 
a standalone redis-server, copy the dataset and module files to the DB VM and make usage of the tool to run the query variations. 
- After each benchmark the defined KPIs limits are checked and will influence the exit code of the runner script. Even if we fail a benchmark variation, all other benchmark definitions are run.
- At the end of each benchmark an output json file is stored with this benchmarks folder and will be named like `<start time>-<deployment type>-<git org>-<git repo>-<git branch>-<test name>-<git sha>.json`
- In the case of a uncaught exception after we've deployed the environment the benchmark script will always try to teardown the created environment. 