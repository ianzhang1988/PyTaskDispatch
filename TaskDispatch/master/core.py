# -*- coding: utf-8 -*-
# @Time    : 2019/9/17 18:01
# @Author  : ZhangYang
# @Email   : ian.zhang.88@outlook.com

from .utility import create_new_sequence_node
from .job import Job
from .consts import TaskStateCode

class Core():
    def __init__(self, zk_client):
        self.zk_client = zk_client

        self.base_path='/TaskDispatch'
        self.jobs_path = self.base_path + '/jobs'
        self.workers_path = self.base_path + '/workers'
        self.masters_path = self.base_path + '/masters'
        self.cluster_path = self.base_path + '/clusters'

        self._cluster = {}

        self._cluster_job_upper_threshold=50
        self._cluster_job_lower_threshold=20

    def make_sure_tree_structure(self):
        self.zk_client.ensure_path(self.base_path)
        self.zk_client.ensure_path(self.jobs_path)
        self.zk_client.ensure_path(self.workers_path)
        self.zk_client.ensure_path(self.masters_path)

    def add_new_job(self, data):
        new_node_path = create_new_sequence_node(self.zk_client, self.jobs_path, 'job')
        print('new_node_path:', new_node_path)
        j = Job(self.zk_client, new_node_path)
        ret = j.parse(data)
        j.set_state(TaskStateCode.QUEUE)
        return ret, new_node_path

    def prepare_dequeue_job(self):
        jobs_path = self.zk_client.get_children(self.jobs_path)
        jobs = [Job(self.zk_client, path) for path in jobs_path]

        # check lower threshold

        new_cluster={}

        for j in jobs:
            cluster = j.get_cluster()
            if cluster not in self._cluster:
                new_cluster[cluster]=[]
                new_cluster[cluster].append(j)
            else:
                new_cluster[cluster].append(j)

