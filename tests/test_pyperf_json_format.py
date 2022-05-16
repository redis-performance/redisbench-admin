#  BSD 3-Clause License
#
#  Copyright (c) 2022., Redis Labs Modules
#  All rights reserved.
#
import json

from redisbench_admin.export.pyperf.pyperf_json_format import (
    generate_summary_json_pyperf,
)


def test_fill_avg_stddev_pyperf():
    with open(
        "./tests/test_data/results/pyperf.json",
        "r",
    ) as json_file:
        results_dict = json.load(json_file)
        summary_json = generate_summary_json_pyperf(results_dict)
        assert "BlockingConnectionPool_get_connection" in summary_json
