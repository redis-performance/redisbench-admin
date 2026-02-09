#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#

from importlib.resources import files

# ALLOWED_PROFILERS = "perf:record,ebpf:oncpu,ebpf:offcpu,vtune"
ALLOWED_PROFILERS = "perf:record,vtune"
PROFILERS_DEFAULT = "perf:record"
PROFILE_FREQ_DEFAULT = "99"

STACKCOLLAPSE_PATH = str(
    files("redisbench_admin").joinpath("profilers/stackcollapse-perf.pl")
)

FLAMEGRAPH_PATH = str(files("redisbench_admin").joinpath("profilers/flamegraph.pl"))
