from unittest import TestCase

from redisbench_admin.export.common.common import (
    add_datapoint,
    split_tags_string,
    get_or_None,
)
from redisbench_admin.run.common import get_start_time_vars


class Test(TestCase):
    def test_add_datapoint(self):
        time_series_dict = {}
        broader_ts_name = "ts"
        tags_array = []
        add_datapoint(time_series_dict, broader_ts_name, 1, 5.0, tags_array)
        add_datapoint(time_series_dict, broader_ts_name, 4, 10.0, tags_array)
        add_datapoint(time_series_dict, broader_ts_name, 60, 10.0, tags_array)
        assert time_series_dict == {
            "ts": {
                "data": [5.0, 10.0, 10.0],
                "index": [1, 4, 60],
                "tags": {},
                "tags-array": [],
            }
        }

    def test_split_tags_string(self):
        result = split_tags_string("k1=v1,k2=v2")
        assert result == [{"k1": "v1"}, {"k2": "v2"}]

    def test_get_or_none(self):
        res = get_or_None({}, "k")
        assert res == None
        res = get_or_None({"k": "v"}, "k")
        assert res == "v"


def test_get_start_time_vars():
    start_time, start_time_ms, start_time_str = get_start_time_vars()
    assert type(start_time_ms) == int
    assert start_time_ms > 0
    assert type(start_time_str) == str
