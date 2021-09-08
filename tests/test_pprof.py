#  BSD 3-Clause License
#
#  Copyright (c) 2021., Redis Labs Modules
#  All rights reserved.
#
from redisbench_admin.profilers.pprof import process_pprof_text_to_tabular


def test_process_pprof_text_to_tabular():
    tabular_data = process_pprof_text_to_tabular(
        "./tests/test_data/results/profile_oss-standalone__primary-1-of-1__tsbs-scale100_lastpoint_perf:record_2021-09-07-15-13-02.out.pprof.txt",
        "text-lines",
    )
    assert tabular_data["columns:text"] == ["self%", "cum%", "entry"]
    assert len(tabular_data["rows:self%"]) == 10
    assert len(tabular_data["rows:cum%"]) == 10
    assert len(tabular_data["rows:entry"]) == 10
    assert tabular_data["type"] == "text-lines"
    assert tabular_data["rows:entry"][3] == "generate_digits (inline)"
    assert tabular_data["rows:cum%"][3] == "3.03"
    assert tabular_data["rows:self%"][3] == "3.03"
    assert tabular_data["rows:entry"][0] == "[redistimeseries.so]"
    assert tabular_data["rows:cum%"][0] == "92.59"
    assert tabular_data["rows:self%"][0] == "75.45"

    tabular_data = process_pprof_text_to_tabular(
        "./tests/test_data/results/profile_oss-standalone__primary-1-of-1__tsbs-scale100_cpu-max-all-1_perf:record_2021-09-07-16-52-16.out.pprof.LOC.txt",
        "text-lines",
    )
    assert tabular_data["columns:text"] == ["self%", "cum%", "entry"]
    assert len(tabular_data["rows:self%"]) == 43
    assert len(tabular_data["rows:cum%"]) == 43
    assert len(tabular_data["rows:entry"]) == 43
    assert tabular_data["type"] == "text-lines"
    assert tabular_data["rows:entry"][0] == "<unknown>"
    assert tabular_data["rows:cum%"][0] == "12.29"
    assert tabular_data["rows:self%"][0] == "12.29"
    assert tabular_data["rows:entry"][1] == "[redistimeseries.so]"
    assert tabular_data["rows:cum%"][1] == "87.08"
    assert tabular_data["rows:self%"][1] == "8.33"
    assert (
        tabular_data["rows:entry"][4]
        == "SeriesIteratorGetNext /home/fco/redislabs/RedisTimeSeries/src/series_iterator.c:160"
    )
    assert tabular_data["rows:cum%"][4] == "5.88"
    assert tabular_data["rows:self%"][4] == "5.88"
