from redisbench_admin.utils.benchmark_config import (
    prepare_benchmark_definitions,
    process_benchmark_definitions_remote_timeouts,
)


class BenchmarkClass:
    def __init__(
        self,
        benchmark_defs_result=None,
        benchmark_definitions=None,
        default_metrics=None,
        exporter_timemetric_path=None,
        default_specs=None,
        clusterconfig=None,
        remote_envs_timeout=None,
        benchmark_runs_plan=None,
    ):
        self.benchmark_defs_result = benchmark_defs_result
        self.benchmark_definitions = benchmark_definitions
        self.default_metrics = default_metrics
        self.exporter_timemetric_path = exporter_timemetric_path
        self.default_specs = default_specs
        self.clusterconfig = clusterconfig
        self.benchmark_artifacts_table_headers = [
            "Setup",
            "Test-case",
            "Artifact",
            "link",
        ]
        self.benchmark_artifacts_table_name = "Benchmark client artifacts"
        self.benchmark_runs_plan = benchmark_runs_plan

        self.remote_envs_timeout = remote_envs_timeout

    def prepare_benchmark_definitions(self, args):
        (
            self.benchmark_defs_result,
            self.benchmark_definitions,
            self.default_metrics,
            self.exporter_timemetric_path,
            self.default_specs,
            self.clusterconfig,
        ) = prepare_benchmark_definitions(args)

    def populate_remote_envs_timeout(self):
        self.remote_envs_timeout = process_benchmark_definitions_remote_timeouts(
            self.benchmark_definitions
        )
