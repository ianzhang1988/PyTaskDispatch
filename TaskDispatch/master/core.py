# -*- coding: utf-8 -*-
# @Time    : 2019/9/17 18:01
# @Author  : ZhangYang
# @Email   : ian.zhang.88@outlook.com

from .utility import create_new_sequence_node
from .job import Job
from .consts import TaskStateCode
from .cluster import Cluster

class Core():
    def __init__(self, zk_client):
        self.zk_client = zk_client

        self.base_path='/TaskDispatch'
        self.jobs_path = self.base_path + '/jobs'
        self.workers_path = self.base_path + '/workers'
        self.masters_path = self.base_path + '/masters'
        self.cluster_path = self.base_path + '/clusters'

        self._cluster_job = {}

        self._cluster_job_upper_threshold=50
        self._cluster_job_lower_threshold=20

        self.cluster = Cluster(self.zk_client, self.cluster_path)

    def make_sure_tree_structure(self):
        self.zk_client.ensure_path(self.base_path)
        self.zk_client.ensure_path(self.jobs_path)
        self.zk_client.ensure_path(self.workers_path)
        self.zk_client.ensure_path(self.masters_path)

    def add_cluster(self, name):
        self.cluster.add(name)
        self._cluster_job[name] = []

    def delete_cluster(self, name):
        if not self._cluster_job[name]:
            self.cluster.delete(name)
            return True
        return False


    def add_new_job(self, data):
        new_node_path = create_new_sequence_node(self.zk_client, self.jobs_path, 'job')
        print('new_node_path:', new_node_path)
        j = Job(self.zk_client, new_node_path)
        ret = j.parse(data)
        j.set_state(TaskStateCode.QUEUE)
        return ret, new_node_path

    def prepare_dequeue_job(self):
        jobs_path = self.zk_client.get_children(self.jobs_path)
        jobs = [Job(self.zk_client, self.jobs_path+'/'+path) for path in jobs_path]

        clusters = self.cluster.get_all()

        for c in clusters:
            size = len(self._cluster_job[c])
            if size > self._cluster_job_lower_threshold:
                continue

            jobs_this_cluster = [j for j in jobs if j.get_cluster() == c]
            jobs_this_cluster_sorted = sorted(jobs_this_cluster, key=lambda x: x.get_priority(), reverse=True )

            print('jobs_this_cluster_sorted', list([j.get_priority() for j in jobs_this_cluster_sorted]))

            jobs_needed = jobs_this_cluster_sorted[:self._cluster_job_upper_threshold - size]
            for j in jobs_needed:
                j.set_state(TaskStateCode.DEQUEUE)
            self._cluster_job[c].extend(jobs_needed)
            self._cluster_job[c] = sorted(self._cluster_job[c], key=lambda x: x.get_priority(), reverse=True )

    def get_dequeue_job_task(self, cluster_name, task_type):
        jobs = self._cluster_job[cluster_name]

        job_task_path = None
        task_path = None
        task_data = None

        for j in jobs:
            if j.get_type() != task_type:
                continue
            if j.get_state() == TaskStateCode.DEQUEUE:
                job_task_path = j.job_path()
                task_path = j.task_path()
                task_data = j.get_data()
                j.set_state(TaskStateCode.READY)
                break

        return job_task_path, task_path, task_data


