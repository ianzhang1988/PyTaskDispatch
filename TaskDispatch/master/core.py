# -*- coding: utf-8 -*-
# @Time    : 2019/9/17 18:01
# @Author  : ZhangYang
# @Email   : ian.zhang.88@outlook.com

from .utility import create_new_sequence_node
from .job import Job

class Core():
    def __init__(self, zk_client):
        self.zk_client = zk_client

        self.base_path='/TaskDispatch'
        self.jobs_path = self.base_path + '/jobs'
        self.workers_path = self.base_path + '/workers'
        self.masters_path = self.base_path + '/masters'

    def make_sure_tree_structure(self):
        self.zk_client.ensure_path(self.base_path)
        self.zk_client.ensure_path(self.jobs_path)
        self.zk_client.ensure_path(self.workers_path)
        self.zk_client.ensure_path(self.masters_path)

    def add_new_job(self, data):
        new_node_path = create_new_sequence_node(self. zk_client, self.jobs_path, 'job')
        j = Job(self.zk_client, new_node_path)
        ret = j.parse(data)
        return ret, new_node_path
