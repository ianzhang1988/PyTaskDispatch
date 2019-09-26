# -*- coding: utf-8 -*-
# @Time    : 2019/9/26 18:45
# @Author  : ZhangYang
# @Email   : ian.zhang.88@outlook.com

from .utility import SetGetMixin

class Cluster(SetGetMixin):
    def __init__(self, zk_client, base_path):
        self.zk_client = zk_client
        self.base_path = base_path
        self.zk_client.ensure_path(self.base_path)

    def add(self, cluster_name):
        if self.zk_client.exists(self.base_path+'/'+cluster_name):
            return
        self.zk_client.create(self.base_path+'/'+cluster_name, makepath=True)

    def get_all(self):
        return self.zk_client.get_children(self.base_path)

    def delete(self, cluster_name):
        if self.zk_client.exists(self.base_path+'/'+cluster_name):
            self.zk_client.delete(self.base_path+'/'+cluster_name)
        return True


