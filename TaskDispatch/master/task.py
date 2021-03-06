# -*- coding: utf-8 -*-
# @Time    : 2019/9/10 18:30
# @Author  : ZhangYang
# @Email   : ian.zhang.88@outlook.com

from functools import wraps
from datetime import datetime

# todo: cache some attribute so that we don't need check with zookeeper every time

class Task():
    def __init__(self, zk_client, base_path):
        self.zk_client = zk_client
        self.base_path = base_path

        self.worker_path = self.base_path+'/worker'
        self.data_path = self.base_path + '/data'
        self.enqueue_time = self.base_path + '/enqueue_time'
        self.dequeue_time = self.base_path + '/dequeue_time'
        self.start_time_path = self.base_path + '/start_time'
        self.end_time_path = self.base_path + '/end_time'
        self.state_path = self.base_path + '/task_state'

        # separate form task_state, avoiding async operation write at same time resulting in error state
        self.state_abnormal_path = self.base_path + '/task_state_abnormal'

        # for client data
        self.client_data_entry = self.base_path + '/client_entry'

        # separate kill flag rather than just use taskstate.kill
        # because server set kill and client set say working same time, could result in working
        # we want state be kill
        self.kill_flag_path = self.base_path + '/kill_flag'


    def task_path(self):
        return self.base_path

    def get(path_variable):
        def decorator(func):
            @wraps(func)
            def wrapper(self, *args, **kwargs):
                if not self.zk_client.exists(getattr(self, path_variable)):
                    return None
                return func(self, *args, **kwargs)

            return wrapper
        return decorator

    def set(path_variable):
        def decorator(func):
            @wraps(func)
            def wrapper(self, *args, **kwargs):
                self.zk_client.ensure_path(getattr(self, path_variable))
                return func(self, *args, **kwargs)
            return wrapper
        return decorator

    def delete(self):
        self.zk_client.delete(self.base_path, recursive=True)

    @get('worker_path')
    def get_worker(self):
        return self.zk_client.get(self.worker_path)[0].decode('utf-8')

    def check_worker(self):
        return True if self.zk_client.exists(self.worker_path) else False

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

    @set('enqueue_time')
    def set_enqueue_time(self, time_str):
        self.zk_client.set(self.enqueue_time, time_str.encode('utf-8'))

    @get('enqueue_time')
    def get_enqueue_time(self):
        return self.zk_client.get(self.enqueue_time)[0].decode('utf-8')

    @set('dequeue_time')
    def set_dequeue_time(self, time_str):
        self.zk_client.set(self.dequeue_time, time_str.encode('utf-8'))

    @get('enqueue_time')
    def get_dequeue_time(self):
        return self.zk_client.get(self.dequeue_time)[0].decode('utf-8')

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

    @set('state_path')
    def set_state(self, data):
        self.zk_client.set(self.state_path, data.encode('utf-8'))

    @get('state_path')
    def get_state(self):
        return self.zk_client.get(self.state_path)[0].decode('utf-8')

    def set_kill_flag(self):
        ret = self.zk_client.create(self.kill_flag_path, ''.encode('utf-8'))

    def clear_kill_flag(self):
        self.zk_client.delete(self.kill_flag_path)