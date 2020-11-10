import logging
import os
import json
import sys
import time
import math
import urllib3
import threading
import requests
import socket
from autoscaler.agent_stats import AgentStats
from autoscaler.api_client import APIClient
from autoscaler.app import MarathonApp
from autoscaler.modes.scalecpu import ScaleByCPU
from autoscaler.modes.scalesqs import ScaleBySQS
from autoscaler.modes.scalemem import ScaleByMemory
from autoscaler.modes.scalecpuandmem import ScaleByCPUAndMemory
from autoscaler.modes.scalebycpuormem import ScaleByCPUOrMemory
from logging.handlers import RotatingFileHandler

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
LOGGING_FORMAT = '%(asctime)s - %(threadName)s - %(thread)s - %(pathname)s:%(lineno)d - %(levelname)s - %(message)s'

class Autoscaler:
    """Marathon autoscaler upon initialization, it reads a list of
    command line parameters or env variables. Then it logs in to DCOS
    and starts querying metrics relevant to the scaling objective
    (cpu, mem, sqs, and, or). Scaling can happen by cpu, mem,
    or sqs. The checks are performed on a configurable interval.
    """

    MARATHON_APPS_URI = '/service/marathon/v2/apps'

    # Dictionary defines the different scaling modes available to autoscaler
    MODES = {
        'sqs': ScaleBySQS,
        'cpu': ScaleByCPU,
        'mem': ScaleByMemory,
        'and': ScaleByCPUAndMemory,
        'or': ScaleByCPUOrMemory
    }


    def __init__(self, dcos_tenant, marathon_app, trigger_mode, autoscale_multiplier, min_instances, max_instances, cool_down_factor
                 , scale_up_factor, min_range, max_range, interval, log_level, api_client):
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
        self.MARATHON_APPS_URI = self.MARATHON_APPS_URI.replace('marathon', dcos_tenant)
        #多线程时的终止条件
        self.active = True

        # Start logging
        self.log = logging.getLogger(dcos_tenant + marathon_app)
        if self.log_level == 'DEBUG':
            self.log.setLevel(logging.DEBUG)
        else:
            self.log.setLevel(logging.INFO)

        # Initialize marathon client for auth requests
        self.api_client = api_client

        # Initialize agent statistics fetcher and keeper
        self.agent_stats = AgentStats(self.api_client)

        # Instantiate the Marathon app class
        app_name = marathon_app
        if not app_name.startswith('/'):
            app_name = '/' + app_name
        self.marathon_app = MarathonApp(
            app_name=app_name,
            api_client=self.api_client,
            dcos_tenant=dcos_tenant
        )

        # Instantiate the scaling mode class
        if self.MODES.get(self.trigger_mode, None) is None:
            self.log.error("Scale mode is not found.")
            sys.exit(1)

        min = [float(i) for i in min_range]
        max = [float(i) for i in max_range]

        dimension = {"min": min, "max": max}

        self.scaling_mode = self.MODES[self.trigger_mode](
            api_client=self.api_client,
            agent_stats=self.agent_stats,
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
                self.log.warning("Auto-scale triggered based on %s exceeding threshold" % self.trigger_mode)
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
                self.log.warning("Auto-scale triggered based on %s below the threshold" % self.trigger_mode)
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
                self.MARATHON_APPS_URI + self.marathon_app.app_name,
                data=json_data
            )
            self.log.debug("scale_app response: %s", response)

    def run(self):
        """Main function
        """
        self.cool_down = 0
        self.scale_up = 0

        while True:
            if self.active is not True:
                self.log.info("termination")
                return
            try:
                self.agent_stats.reset()

                # Test for apps existence in Marathon
                if not self.marathon_app.app_exists():
                    self.log.error("Could not find %s in list of apps.",
                                   self.marathon_app.app_name)
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

if __name__ == "__main__":
    logging.basicConfig(
        format=LOGGING_FORMAT,
        level=logging.INFO
    )
    rh = RotatingFileHandler('marathon-autoscale-info-' + socket.gethostname() + '.log', maxBytes=1024 * 1024 * 1024, backupCount=10)
    rh.setLevel(level=logging.INFO)
    rh.setFormatter(logging.Formatter(LOGGING_FORMAT))
    rhe = RotatingFileHandler('marathon-autoscale-warn-' + socket.gethostname() + '.log', maxBytes=1024 * 1024 * 1024, backupCount=10)
    rhe.setLevel(level=logging.WARN)
    rhe.setFormatter(logging.Formatter(LOGGING_FORMAT))
    logging.getLogger().addHandler(rh)
    logging.getLogger().addHandler(rhe)
    log = logging.getLogger('autoscale')

    #获取参数,优先通过请求AUTOSCALE_ARGS_URI获得，如果获取不到,使用AUTOSCALE_ARGS
    use_env_args = False
    argsJson = ''
    args_uri = os.environ.get('AUTOSCALE_ARGS_URI')
    if args_uri is not None and args_uri.strip() != '':
        response = requests.get(args_uri)
        if response.status_code != 200:
            raise SystemError(args_uri + ' is not available\n' + response.content )
        jsonArgs = response.json()
    else:
        argsEnv = os.environ.get('AUTOSCALE_ARGS')
        if argsEnv is None or argsEnv.strip() != '':
            raise SystemError('fail to get args')
        jsonArgs = json.loads(argsEnv)
        use_env_args = True
    #每个app对应一个autoscaler,并运行在单独的线程中
    api_client = APIClient(jsonArgs['dcos_master'])
    interval = jsonArgs['interval']
    current_marathon_apps = jsonArgs['marathon_apps']
    #map的key为app['dcos_tenant'] + app['id']
    threadsMap = {}
    autoScalerMap = {}
    for app in current_marathon_apps:
        autoScaler = Autoscaler(app['dcos_tenant'],
            app['id'],
            app['trigger_mode'],
            app['autoscale_multiplier'],
            app['min_instances'],
            app['max_instances'],
            app['cool_down_factor'],
            app['scale_up_factor'],
            app['min_range'],
            app['max_range'],
            interval,
            app['log_level'],
            api_client)
        t = threading.Thread(target=autoScaler.run,name=app['dcos_tenant'] + app['id'])
        t.start()
        threadsMap[t.getName()] = t
        autoScalerMap[t.getName()] = autoScaler
    #清理缓存
    while True:
        time.sleep(interval)
        #清空api_client.dcos_rest_get()的缓存
        log.info(' '.join(['current cache_info:', str(api_client.dcos_rest_get.cache_info()), '\n cache cleared']))
        api_client.dcos_rest_get.cache_clear()

        #如果是env_args方式，则不执行以下逻辑
        if use_env_args: continue

        #访问服务扩缩信息全量查询接口，更新策略
        log.info('Polling Update Autoscaler Threads Begin')
        response = requests.get(args_uri)
        if response.status_code is not 200:
            log.error("request for autoscale api error:" + response.content)
            continue
        else:
            #当前app key set
            currentAppsMap = {}
            for app in current_marathon_apps:
                currentAppsMap[app['dcos_tenant'] + app['id']] = app

            currentAppKeySet = set(currentAppsMap.keys())
            #接口返回app信息
            expectedApps = response.json()['marathon_apps']
            expectedAppsMap = {}
            for app in expectedApps:
                expectedAppsMap[app['dcos_tenant'] + app['id']] = app
            #接口返回的app key set
            expectedAppKeySet = set(expectedAppsMap.keys())
            #新增key
            newAppKeySet = expectedAppKeySet - currentAppKeySet
            log.info('new app:' + str(newAppKeySet))
            #移除的key
            removedKeySet = currentAppKeySet - expectedAppKeySet
            log.info('removed app:' + str(removedKeySet))
            #保留的key
            reservedAppKeySet = expectedAppKeySet & currentAppKeySet
            #保留key中，value有更新的key
            modifiedKeySet = set()
            for key in reservedAppKeySet:
                if currentAppsMap.get(key) != expectedAppsMap.get(key):
                    modifiedKeySet.add(key)
                    log.info('app:{} modified from: \n{}\nto:\n{}'.format(key, str(currentAppsMap.get(key)), str(expectedAppsMap.get(key))))
            log.info('modified app:' + str(modifiedKeySet))

            #新增app，根据参数创建新的autoscaler线程
            for key in newAppKeySet:
                app = expectedAppsMap.get(key)
                autoScaler = Autoscaler(app['dcos_tenant'],
                                        app['id'],
                                        app['trigger_mode'],
                                        app['autoscale_multiplier'],
                                        app['min_instances'],
                                        app['max_instances'],
                                        app['cool_down_factor'],
                                        app['scale_up_factor'],
                                        app['min_range'],
                                        app['max_range'],
                                        interval,
                                        app['log_level'],
                                        api_client)
                t = threading.Thread(target=autoScaler.run, name=app['dcos_tenant'] + app['id'])
                t.start()
                autoScalerMap[t.getName()] = autoScaler
                threadsMap[t.getName()] = t
            #移除app，调用autoscaler的ternimal方法，让线程终止
            for key in removedKeySet:
                autoScalerMap.get(key).terminal()
                autoScalerMap.pop(key)
                threadsMap.pop(key)
            #修改app,先移除app,再根据新参数创建autoscaler线程
            for key in modifiedKeySet:
                autoScalerMap.get(key).terminal()
                autoScalerMap.pop(key)
                threadsMap.pop(key)
                app = expectedAppsMap.get(key)
                autoScaler = Autoscaler(app['dcos_tenant'],
                                        app['id'],
                                        app['trigger_mode'],
                                        app['autoscale_multiplier'],
                                        app['min_instances'],
                                        app['max_instances'],
                                        app['cool_down_factor'],
                                        app['scale_up_factor'],
                                        app['min_range'],
                                        app['max_range'],
                                        interval,
                                        app['log_level'],
                                        api_client)
                t = threading.Thread(target=autoScaler.run, name=app['dcos_tenant'] + app['id'])
                t.start()
                threadsMap[t.getName()] = t
                autoScalerMap[t.getName()] = autoScaler
            current_marathon_apps = expectedApps
            log.info('Polling Update Autoscaler Threads End')
