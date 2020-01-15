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
        self.omega_state_path = self.base_path + '/omega_state'

        # for client data, just put key value pair here
        self.client_data_entry = self.base_path + '/client_entry'


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

    @set('omega_state_path')
    def set_omega_state(self, state):
        self.zk_client.set(self.omega_state_path, state.encode('utf-8'))

    @get('omega_state_path')
    def get_omega_state(self):
        return self.zk_client.get(self.omega_state_path)[0].decode('utf-8')

    def reset_omega_state(self):
        if not self.zk_client.exists(self.omega_state_path):
            return True
        self.zk_client.delete(self.omega_state_path)

    @set('client_data_entry')
    def set_client_data(self, key, value):
        path = self.client_data_entry + f'/{key}'
        self.zk_client.ensure_path(path)
        self.zk_client.set(path, value.encode('utf-8'))

    @get('client_data_entry')
    def get_client_data(self, key):
        path = self.client_data_entry + f'/{key}'
        return self.zk_client.get(path)[0].decode('utf-8')

    @get('client_data_entry')
    def get_client_data_all(self):
        key_list = self.zk_client.get_children(self.client_data_entry)
        return {key: self.zk_client.get(self.client_data_entry + f'/{key}')[0].decode('utf-8') for key in key_list}
