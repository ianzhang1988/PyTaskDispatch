# -*- coding: utf-8 -*-
# @Time    : 2019/11/18 17:42
# @Author  : ZhangYang
# @Email   : ian.zhang.88@outlook.com

class TimeoutEvent():
    def __init__(self):
        pass

class TimeoutManager():
    def __init__(self, zk_client):
        self.zk_client = zk_client
        self._timeout_event_list = []
        pass

    def check(self):
        pass

    def _load_from_server(self):
        pass

    def add(self):
        pass

    def _delete(self, timeout_event):
        pass

    def _update(self, timeout_event):
        pass


