# encoding: utf-8

"""
@author: 'liuyuefeng'
@file: autoscaler.py
@time: 2020/11/13 10:54
"""
import json
import logging

import time
import copy
import math
import datetime
from autoscaler.agent_stats import AgentStats
from autoscaler.app import MarathonApp
from autoscaler.modes.scalebyjvm import ScaleByJvm
from autoscaler.modes.scalemem import ScaleByMemory
ALARM_API_BODY = {
        'request_params':{
            'key':'starship'
        },
        'body':{
            'alarmLevel': 'P0',
            'area': '亦庄测试',
            'cluster': '亦庄测试',
            'detail': '自动扩缩告警',
            'dingAlarm': True,
            'key': 'starship',
            'project': '星舟自动扩缩',
            'source': '服务名',
            'startTime':'2020-11-11T02:52:17.460Z',
            'threshold':'50%',
            'type': 'auto_scale'
        }
    }
#如果有全局key，则使用
ALARM_API_BODY_GLOBALKEY = None
# Dictionary defines the different scaling modes available to autoscaler
MODES = {
    'mem': ScaleByMemory,
    'jvm': ScaleByJvm
}

class Autoscaler:
    """Marathon autoscaler upon initialization, it reads a list of
    command line parameters or env variables. Then it logs in to DCOS
    and starts querying metrics relevant to the scaling objective
    (cpu, mem, sqs, and, or). Scaling can happen by cpu, mem,
    or sqs. The checks are performed on a configurable interval.
    """

    MARATHON_APPS_URI = '/service/marathon/v2/apps'
    PROMETHEUS_QUERY_URI = '/api/v1/query'

    def __init__(self, dcos_tenant, prometheus_host, app_id, trigger_mode, autoscale_multiplier, min_instances, max_instances, cool_down_factor
                 , scale_up_factor, min_range, max_range, interval, log_level, api_client, alarm_key):
        self.scale_up = 0
        self.cool_down = 0
        self.trigger_mode = trigger_mode
        self.autoscale_multiplier = float(autoscale_multiplier)
        self.min_instances = int(min_instances)
        self.max_instances = int(max_instances)
        self.cool_down_factor = int(cool_down_factor)
        self.scale_up_factor = int(scale_up_factor)
        self.interval = interval
        self.log_level = log_level
        self.alarm_key = alarm_key
        self.dcos_tenant = dcos_tenant
        self.prometheus_host = prometheus_host
        self.app_id = app_id
        self.min_range = min_range
        self.max_range = max_range
        self.marathon_apps_uri = Autoscaler.MARATHON_APPS_URI.replace('marathon', dcos_tenant)
        #多线程时的终止条件
        self.active = True

        # Start logging
        self.log = logging.getLogger(self.dcos_tenant + self.app_id)
        if self.log_level == 'DEBUG':
            self.log.setLevel(logging.DEBUG)
        else:
            self.log.setLevel(logging.INFO)

        # Initialize marathon client for auth requests
        self.api_client = api_client

        # Initialize agent statistics fetcher and keeper
        self.agent_stats = AgentStats(self.api_client)

        # Instantiate the Marathon app class
        app_id = app_id
        if not app_id.startswith('/'):
            app_id = '/' + app_id
        self.marathon_app = MarathonApp(
            app_id=app_id,
            api_client=self.api_client,
            dcos_tenant=self.dcos_tenant
        )

        # Instantiate the scaling mode class
        min = [float(i) for i in min_range]
        max = [float(i) for i in max_range]

        dimension = {"min": min, "max": max}

        self.scaling_mode = MODES[self.trigger_mode](
            api_client=self.api_client,
            agent_stats=self.agent_stats,
            prometheus_host = self.prometheus_host,
            app=self.marathon_app,
            dimension=dimension,
        )
    def terminal(self):
        self.active = False
    def timer(self):
        """Simple timer function"""
        self.log.debug("Successfully completed a cycle, sleeping for %s seconds",
                       self.interval)
        time.sleep(self.interval)

    def autoscale(self, direction):
        """ Determine if scaling mode direction is below or above scaling
        factor. If scale_up/cool_down cycle count exceeds scaling
        factor, autoscale (up/down) will be triggered.
        """

        if direction == 1:
            self.scale_up += 1
            self.cool_down = 0
            if self.scale_up >= self.scale_up_factor:
                self.log.info("Auto-scale triggered based on %s exceeding threshold" % self.trigger_mode)
                self.scale_app(True)
                self.scale_up = 0
            else:
                self.log.info("%s above thresholds, but waiting to exceed scale-up factor. "
                              "Consecutive cycles = %s, Scale-up factor = %s" %
                              (self.trigger_mode, self.scale_up, self.scale_up_factor))
        elif direction == -1:
            self.cool_down += 1
            self.scale_up = 0
            if self.cool_down >= self.cool_down_factor:
                self.log.info("Auto-scale triggered based on %s below the threshold" % self.trigger_mode)
                self.scale_app(False)
                self.cool_down = 0
            else:
                self.log.info("%s below thresholds, but waiting to exceed cool-down factor. "
                              "Consecutive cycles = %s, Cool-down factor = %s" %
                              (self.trigger_mode, self.cool_down, self.cool_down_factor))
        else:
            self.log.info("%s within thresholds" % self.trigger_mode)
            self.scale_up = 0
            self.cool_down = 0
    def alarm(self, detail):
        msg = copy.deepcopy(ALARM_API_BODY)
        if ALARM_API_BODY_GLOBALKEY != None:
            msg['request_params']['key'] = ALARM_API_BODY_GLOBALKEY
        else:
            msg['request_params']['key'] = self.alarm_key
        msg['body']['detail'] = detail
        msg['body']['source'] = self.dcos_tenant + self.app_id
        msg['body']['startTime'] = datetime.datetime.now().isoformat()
        msg['body']['threshold'] = '扩缩策略:{},扩容阈值:{}%,缩容阈值:{}%,最小实例数:{},最大实例数:{}'\
            .format(self.trigger_mode, str(self.max_range), str(self.min_range), str(self.min_instances), str(self.max_instances))
        self.log.warning(json.dumps(msg,ensure_ascii=False))
    def scale_app(self, is_up):
        """Scale marathon_app up or down
        Args:
            is_up(bool): Scale up if True, scale down if False
        """
        # get the number of instances running
        app_instances = self.marathon_app.get_app_instances()

        if is_up:
            target_instances = math.ceil(app_instances * self.autoscale_multiplier)
            if target_instances > self.max_instances:
                self.log.warning("Reached the set maximum of instances %s", self.max_instances)
                target_instances = self.max_instances
            if target_instances > app_instances:
                detail = "当前实例数为{}，将扩容至实例数{}".format(app_instances, target_instances)
            else:
                detail = "当前实例数为{}，已达到最大实例数".format(app_instances)
            self.alarm(detail)
        else:
            # target_instances = math.floor(app_instances / self.autoscale_multiplier)
            # if target_instances < self.min_instances:
            #     self.log.info("Reached the set minimum of instances %s", self.min_instances)
            #     target_instances = self.min_instances
            #缩容动作动作不执行，日志告警
            target_instances = app_instances
            self.log.warning('scale down trigger off')

        self.log.debug("scale_app: app_instances %s target_instances %s",
                       app_instances, target_instances)

        if app_instances != target_instances:
            data = {'instances': target_instances}
            json_data = json.dumps(data)
            response = self.api_client.dcos_rest(
                "put",
                self.marathon_apps_uri + self.marathon_app.app_id,
                data=json_data
            )
            self.log.debug("scale_app response: %s", response)

    def run(self):
        """Main function
        """
        self.cool_down = 0
        self.scale_up = 0

        while self.active:
            try:
                self.agent_stats.reset()

                # Test for apps existence in Marathon
                if not self.marathon_app.app_exists():
                    self.log.error("Could not find %s in list of apps.",
                                   self.marathon_app.app_id)
                    continue

                # Get the mode scaling direction
                direction = self.scaling_mode.scale_direction()
                self.log.debug("scaling mode direction = %s", direction)

                # Evaluate whether to auto-scale
                self.autoscale(direction)

            except Exception as e:
                self.log.exception(e)
            finally:
                self.timer()
        self.log.info('termination')