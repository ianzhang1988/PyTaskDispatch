# -*- coding: utf-8 -*-
# @Time    : 2019/9/17 15:29
# @Author  : ZhangYang
# @Email   : ian.zhang.88@outlook.com

from .task import Task
import json

class Job(Task):
    def __init__(self,zk_client, base_path):
        self.job_base_path = base_path
        self.job_task_base_path = base_path + '/job_task'
        zk_client.create(self.job_task_base_path,''.encode('utf-8'))
        super().__init__(zk_client, self.job_task_base_path)

        self.meta_data_path=self.job_base_path+'/meta_data'
        self.id_path = self.job_base_path+'/id'
        self.cluster_path = self.job_base_path +'/cluster'
        self.type_path = self.job_base_path + '/type'

    def parse(self, data_str):
        data = None
        try:
            data = json.loads(data_str)

        except as e:


    @Task.set('meta_data_path')
    def set_meta_data(self, data):
        self.zk_client.set(self.meta_data_path, data.encode('utf-8'))

    @Task.get('meta_data_path')
    def get_meta_data(self):
        return self.zk_client.get(self.meta_data_path)[0].decode('utf-8')

    @Task.set('id_path')
    def set_id(self, data):
        self.zk_client.set(self.id_path, data.encode('utf-8'))

    @Task.get('id_path')
    def get_id(self):
        return self.zk_client.get(self.id_path)[0].decode('utf-8')

    @Task.set('cluster_path')
    def set_cluster(self, data):
        self.zk_client.set(self.cluster_path, data.encode('utf-8'))

    @Task.get('cluster_path')
    def get_cluster(self):
        return self.zk_client.get(self.cluster_path)[0].decode('utf-8')

    @Task.set('type_path')
    def set_type(self, data):
        self.zk_client.set(self.type_path, data.encode('utf-8'))

    @Task.get('type_path')
    def get_type(self):
        return self.zk_client.get(self.type_path)[0].decode('utf-8')