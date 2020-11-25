# encoding: utf-8

"""
@author: 'liuyuefeng'
@file: http_handler.py
@time: 2020/11/12 14:58
"""
from logging import Filter
from logging.handlers import HTTPHandler

class AlarmFilter(Filter):
    '''
    根据LogRecord.msg 是否包含‘alarmLevel’字符串来判断是否过滤
    '''
    def filter(self, record):
        if type(record.msg) == str and record.msg.__contains__('alarmLevel'):
            return True
        return False

class MyHttpHandler(HTTPHandler):
    '''
    要求record.__dict__['msg']的结构为：
    {
        'request_params':{
            'name': 'xiaoming'
        },
        'body':{
        }
    }
    '''
    def mapLogRecord(self, record):
        """
        Default implementation of mapping the log record into a dict
        that is sent as the CGI data. Overwrite in your class.
        Contributed by Franz Glasner.
        """
        return record.__dict__['msg']

    def emit(self, record):
        """
        Emit a record.

        Send the record to the Web server as a percent-encoded dictionary
        """
        try:
            import requests, urllib.parse, json
            msg = json.loads(self.mapLogRecord(record))
            #拼装url
            url = self.url
            if (url.find('?') >= 0):
                sep = '&'
            else:
                sep = '?'
            request_params = urllib.parse.urlencode(msg['request_params'])
            url = 'http://' + self.host + url + "%c%s" % (sep, request_params)

            if self.method == "GET":
                requests.get(url)
            else:
                requests.post(url,json=msg['body'])
        except Exception:
            self.handleError(record)