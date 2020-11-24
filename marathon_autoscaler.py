import logging
import json
import os
import time
import urllib3
import threading
import requests
import socket
from autoscaler.api_client import APIClient
from logging.handlers import RotatingFileHandler
from autoscaler.autoscaler import Autoscaler
from autoscaler import autoscaler
from autoscaler.http_handler import MyHttpHandler, AlarmFilter


LOGGING_FORMAT = '%(asctime)s - %(threadName)s - %(thread)s - %(pathname)s:%(lineno)d - %(levelname)s - %(message)s'

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

JSON_CONFIG_URL = os.environ.get('JSON_CONFIG_URL')

#判断是否支持mode
supportMode = lambda app: False if autoscaler.MODES.get(app['trigger_mode'], None) is None else True

if __name__ == "__main__":
    #从配置中心加载配置
    #配置格式
    '''
    dcos_master: http://leader.mesos
    prometheus_host: http://1.1.1.1:8888
    internal: 20
    log_level: INFO
    alarm_api:
      host: marathon-lb-skyark.sae-skyark.dcos.2i.unicom.local
      url: /mser/business/monitor/alarm
      params:
        globalKey: 'starship'
        alarmLevel: 'P0'
        area: '亦庄测试'
        cluster: '亦庄测试'
        dingAlarm: True
        project: '星舟自动扩缩'
        type: 'auto_scale'
        key: 'starship'
    scale_api_url: http://marathon-lb-skyark.sae-skyark.dcos.2i.unicom.locall/mser/scale/configs/effective
    '''
    response = requests.get(JSON_CONFIG_URL)
    if response.status_code != 200:
        raise SystemError('获取配置失败')
    config = json.loads(response.content)
    dcos_master = config['dcos_master']
    prometheus_host = config['prometheus_host']
    interval = config['internal']
    #log_level = config['log_level']
    alarm_host = config['alarm_api']['host']
    alarm_url = config['alarm_api']['url']
    scale_api_url = config['scale_api_url']

    #填充告警接口报文
    autoscaler.ALARM_API_BODY_GLOBALKEY = config['alarm_api'].get('globalKey')
    autoscaler.ALARM_API_BODY['body']['area'] = config['alarm_api']['params']['area']
    autoscaler.ALARM_API_BODY['body']['cluster'] = config['alarm_api']['params']['cluster']
    autoscaler.ALARM_API_BODY['body']['dingAlarm'] = config['alarm_api']['params']['dingAlarm']
    autoscaler.ALARM_API_BODY['body']['project'] = config['alarm_api']['params']['project']
    autoscaler.ALARM_API_BODY['body']['type'] = config['alarm_api']['params']['type']
    autoscaler.ALARM_API_BODY['body']['alarmLevel'] = config['alarm_api']['params']['alarmLevel']
    autoscaler.ALARM_API_BODY['body']['key'] = config['alarm_api']['params']['key']

    #root日志设置
    logging.basicConfig(
        format=LOGGING_FORMAT,
        level=logging.INFO
    )
    rootlog = logging.getLogger()
    #info级别及以上日志，输出到info文件
    rh = RotatingFileHandler('marathon-autoscale-info-' + socket.gethostname() + '.log', maxBytes=1024 * 1024 * 100, backupCount=10, encoding='u8')
    rh.setLevel(level=logging.INFO)
    rh.setFormatter(logging.Formatter(LOGGING_FORMAT))
    rootlog.addHandler(rh)
    #warn级别及以上日志，输出到warn文件
    rhe = RotatingFileHandler('marathon-autoscale-warn-' + socket.gethostname() + '.log', maxBytes=1024 * 1024 * 100, backupCount=10, encoding='u8')
    rhe.setLevel(level=logging.WARN)
    rhe.setFormatter(logging.Formatter(LOGGING_FORMAT))
    rootlog.addHandler(rhe)
    #告警类日志还需要调用alarm接口
    httpHandler = MyHttpHandler(alarm_host, alarm_url, method="POST")
    httpHandler.addFilter(AlarmFilter())
    httpHandler.setLevel(level=logging.WARN)
    rootlog.addHandler(httpHandler)

    log = logging.getLogger('autoscale')

    #访问扩缩策略接口
    use_env_args = False
    argsJson = ''
    response = requests.get(scale_api_url)
    if response.status_code != 200:
        raise SystemError(scale_api_url + ' is not available\n' + response.content)
    jsonArgs = response.json()
    #每个app对应一个autoscaler,并运行在单独的线程中
    api_client = APIClient(dcos_master)
    current_marathon_apps = list(filter(supportMode, jsonArgs['data']['marathon_apps']))
    #map的key为app['dcos_tenant'] + app['id']
    #threadsMap = {}
    autoScalerMap = {}
    for app in current_marathon_apps:
        autoScaler = Autoscaler(app['dcos_tenant'],
                                prometheus_host,
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
                                api_client,
                                app['alarm_key'])
        t = threading.Thread(target=autoScaler.run,name=app['dcos_tenant'] + app['id'])
        t.start()
        #threadsMap[t.getName()] = t
        autoScalerMap[t.getName()] = autoScaler
    #定时任务：1.清空api_client.dcos_rest_get()缓存；2.动态更新扩缩策略
    while True:
        try:

            time.sleep(interval)
            #清空api_client.dcos_rest_get()的缓存
            log.info(' '.join(['current cache_info:', str(api_client.dcos_rest_get.cache_info()), '\n cache cleared']))
            api_client.dcos_rest_get.cache_clear()

            #如果是env_args方式，则不执行以下逻辑
            if use_env_args: continue
            #访问服务扩缩信息全量查询接口，更新autoscale
            log.info('Polling Update Autoscaler Threads Begin')
            response = requests.get(scale_api_url)
            if response.status_code != 200:
                log.error("request for autoscale api error:" + response.content)
                continue
            else:
                #当前app key set
                currentAppsMap = {app['dcos_tenant'] + app['id']:app for app in current_marathon_apps}
                currentAppKeySet = set(currentAppsMap.keys())
                #接口返回app信息
                expectedApps = list(filter(supportMode, response.json()['data']['marathon_apps']))
                expectedAppsMap = {app['dcos_tenant'] + app['id']:app for app in expectedApps}
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
                                            prometheus_host,
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
                                            api_client,
                                            app['alarm_key'])
                    t = threading.Thread(target=autoScaler.run, name=app['dcos_tenant'] + app['id'])
                    t.start()
                    autoScalerMap[t.getName()] = autoScaler
                    #threadsMap[t.getName()] = t
                #移除app，调用autoscaler的ternimal方法，让线程终止
                for key in removedKeySet:
                    autoScalerMap.get(key).terminal()
                    autoScalerMap.pop(key)
                    #threadsMap.pop(key)
                #修改app,先移除app,再根据新参数创建autoscaler线程
                for key in modifiedKeySet:
                    autoScalerMap.get(key).terminal()
                    autoScalerMap.pop(key)
                    #threadsMap.pop(key)
                    app = expectedAppsMap.get(key)
                    autoScaler = Autoscaler(app['dcos_tenant'],
                                            prometheus_host,
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
                                            api_client,
                                            app['alarm_key'])
                    t = threading.Thread(target=autoScaler.run, name=app['dcos_tenant'] + app['id'])
                    t.start()
                    #threadsMap[t.getName()] = t
                    autoScalerMap[t.getName()] = autoScaler
                current_marathon_apps = expectedApps
                log.info('Polling Update Autoscaler Threads End')
        except Exception as e:
            log.exception(e)
