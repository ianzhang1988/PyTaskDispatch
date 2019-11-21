# -*- coding: utf-8 -*-
# @Time    : 2019/11/18 17:42
# @Author  : ZhangYang
# @Email   : ian.zhang.88@outlook.com

from datetime import datetime, timedelta
import json
from .utility import create_new_sequence_node

import traceback, sys

class TimeoutEvent(): # could break into two class

    def __init__(self, zk_client, path):
        self.zk_client = zk_client
        self.path = path

        self.task_path =None
        self.type=None
        self.callback_func=None
        self.parameters=None
        self.time_out=None
        self.interval=None
        self.final_time=None

    def init_time(self, final_time, interval = None, now = None):
        self.final_time = final_time
        if interval:
            self.interval = interval
            self.time_out = now + interval
        else:
            self.time_out = final_time

    def check_time_out(self,now):
        if not self.interval:
            return now > self.final_time

        if now > self.time_out:
            self.time_out += self.interval
            return True

    def check_final(self,now):
        return now > self.final_time

    def sync(self):
        data = {
            "task_path": self.task_path,
            "type": self.type,
            "callback_func": self.callback_func,
            "parameters": self.parameters,
            "time_out":self.time_out.strftime('%Y-%m-%d %H:%M:%S'),
            "interval": self.interval.total_seconds() if self.interval else None,
            "final_time":self.final_time.strftime('%Y-%m-%d %H:%M:%S'),
        }

        data_str = json.dumps(data).encode('utf-8')

        self.zk_client.set(self.path, data_str)

    def load(self):
        data_str = self.zk_client.get(self.path)[0]
        data = json.loads(data_str)

        self.task_path = data['task_path']
        self.type = data['type']
        self.callback_func = data['callback_func']
        self.parameters = data['parameters']
        self.time_out = datetime.strptime(data['time_out'], '%Y-%m-%d %H:%M:%S')
        self.interval = timedelta(seconds=data['interval']) if data['interval'] else None
        self.final_time = datetime.strptime(data['final_time'], '%Y-%m-%d %H:%M:%S')

    def delete(self):
        self.zk_client.delete(self.path)

class TimeoutCallbackRegister():
    def __init__(self):
        pass

class TimeoutManager():
    def __init__(self, zk_client, base_path):
        self.zk_client = zk_client
        self.base_path = base_path
        self.zk_client.ensure_path(self.base_path)

        self._timeout_event_list = []

        self._load_from_server()

    def check(self):
        pass

    def _load_from_server(self):
        all_timeout = self.zk_client.get_children(self.base_path)
        # print('----------- all_timeout', all_timeout)
        # traceback.print_stack(file=sys.stdout)
        for path in all_timeout:
            toe = TimeoutEvent(self.zk_client, self.base_path+'/'+path)
            toe.load()

            self._timeout_event_list.append(toe)

    def add(self, type, task_path, callback_func, parameters, final_time, interval = None):
        timeout_event_path = create_new_sequence_node(self.zk_client,self.base_path,'timeout')
        toe = TimeoutEvent(self.zk_client, timeout_event_path)

        toe.type = type
        toe.task_path = task_path
        toe.callback_func = callback_func
        toe.parameters = parameters
        if interval:
            toe.init_time(final_time, interval, datetime.now())
        else:
            toe.init_time(final_time)

        toe.sync()

        self._timeout_event_list.append(toe)


    def _delete(self, timeout_event):
        timeout_event.delete()




