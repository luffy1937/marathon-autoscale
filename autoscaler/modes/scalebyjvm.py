# encoding: utf-8

"""
@author: 'liuyuefeng'
@file: scalebyjvm.py
@time: 2020/11/23 15:04
"""
from autoscaler.modes.abstractmode import AbstractMode
import requests

class ScaleByJvm(AbstractMode):
    PROMETHEUS_QUERY_URI = '/api/v1/query?query=sum(agent_stats_jvm_gc{application="app_name",name="heap_used"}) / sum(agent_stats_jvm_gc{application="app_name",name="heap_max"})'
    def __init__(self, api_client=None, agent_stats=None, prometheus_host=None, app=None,
                 dimension=None):
        super().__init__(api_client, agent_stats, prometheus_host, app, dimension)

    def get_value(self):
        try:
            # Jvm heap usage
            jvm_heap_usage = self.get_jvm_heap_usage(self.app.app_name)
        except ValueError:
            raise
        self.log.info("Current average jvm utilization for app %s = %s",
                      self.app.app_name, jvm_heap_usage)
        return jvm_heap_usage

    def scale_direction(self):

        try:
            value = self.get_value()
            return super().scale_direction(value)
        except ValueError:
            raise

    def get_jvm_heap_usage(self, app_name):
        """Calculate jvm heap usage for the app
        """

        response = requests.get(self.prometheus_host + self.PROMETHEUS_QUERY_URI.replace('app_name', app_name))
        if response.status_code == 200 :
            jvm_heap_usage = response.json()['data']['result'][0]['value'][1]
        else:
            raise ValueError("failed to get jvm heap usage  from prometheus")

        self.log.debug("jvm heap usage  from prometheus is {}".format(jvm_heap_usage))

        return float(jvm_heap_usage) * 100
