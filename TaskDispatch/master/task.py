# -*- coding: utf-8 -*-
# @Time    : 2019/9/10 18:30
# @Author  : ZhangYang
# @Email   : ian.zhang.88@outlook.com

from functools import wraps
from datetime import datetime

class Task():
    def __init__(self, zk_client, base_path):
        self.zk_client = zk_client
        self.base_path = base_path

        self.worker_path = self.base_path+'/worker'
        self.data_path = self.base_path + '/data'
        self.start_time_path = self.base_path + '/start_time'
        self.end_time_path = self.base_path + '/end_time'

    def get(path):
        def decorator(func):
            @wraps(func)
            def wrapper(self, *args, **kwargs):
                if not self.zk_client.exists(getattr(self, path)):
                    return None
                return func(self, *args, **kwargs)

            return wrapper
        return decorator

    def set(path):
        def decorator(func):
            @wraps(func)
            def wrapper(self, *args, **kwargs):
                if not self.zk_client.exists(getattr(self, path)):
                    self.zk_client.ensure_path(getattr(self, path))
                return func(self, *args, **kwargs)
            return wrapper
        return decorator

    @get('worker_path')
    def get_worker(self):
        return self.zk_client.get(self.worker_path)[0].decode('utf-8')

    @set('data_path')
    def set_data(self, data):
        self.zk_client.set(self.data_path, data.encode('utf-8'))

    @get('data_path')
    def get_data(self):
        return self.zk_client.get(self.data_path)[0].decode('utf-8')

    @set('start_time_path')
    def set_start_time(self, time_str):
        self.zk_client.set(self.start_time_path, time_str.encode('utf-8'))

    @get('start_time_path')
    def get_start_time(self):
        return self.zk_client.get(self.start_time_path)[0].decode('utf-8')

    @set('end_time_path')
    def set_end_time(self, time_str):
        self.zk_client.set(self.end_time_path, time_str.encode('utf-8'))

    @get('end_time_path')
    def get_end_time(self):
        return self.zk_client.get(self.end_time_path)[0].decode('utf-8')

    def get_runtime(self):
        time = self.get_start_time()
        if time is None:
            return None

        return (datetime.now() - datetime.strptime(time, '%Y-%m-%d %H:%M:%S')).seconds

    def get_runtime_total(self):
        time_start = self.get_start_time()
        time_end = self.get_end_time()
        if time_start is None or time_end is None:
            return None

        return (datetime.strptime(time_end, '%Y-%m-%d %H:%M:%S') - datetime.strptime(time_start, '%Y-%m-%d %H:%M:%S')).seconds