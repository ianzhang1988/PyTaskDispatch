# -*- coding: utf-8 -*-
# @Time    : 2019/8/29 18:26
# @Author  : ZhangYang
# @Email   : ian.zhang.88@outlook.com

from kazoo.client import KazooClient

class ZkClientMixin():
    def __init__(self):
        self.zookeeper_host='127.0.0.1:2181'
        self.client = KazooClient(hosts=self.zookeeper_host)
        self.client.start()

    def get_client(self):
        return self.client

    def close(self):
        self.client.stop()
        self.client.close()
