#  Apache License Version 2.0
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#

import pkg_resources

ALLOWED_PROFILERS = "perf:record,ebpf:oncpu,ebpf:offcpu"
PROFILERS_DEFAULT = "perf:record"
PROFILE_FREQ_DEFAULT = "99"

STACKCOLLAPSE_PATH = pkg_resources.resource_filename(
    "redisbench_admin", "profilers/stackcollapse-perf.pl"
)

FLAMEGRAPH_PATH = pkg_resources.resource_filename(
    "redisbench_admin", "profilers/flamegraph.pl"
)
