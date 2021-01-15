from unittest import TestCase

from redisbench_admin.export.common.common import get_timeserie_name


class Test(TestCase):
    def test_get_timeserie_name(self):
        kv_array = [{"deployment-type": "docker-oss"},
                    {"metric-name": "Overall Updates and Aggregates query q50 latency"}]
        metric_name = get_timeserie_name(kv_array)
        expected_metric_name = "deployment-type=docker-oss:metric-name=overall_updates_and_aggregates_query_q50_latency"
        self.assertEqual(metric_name, expected_metric_name)
