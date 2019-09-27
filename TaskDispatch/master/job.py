# -*- coding: utf-8 -*-
# @Time    : 2019/9/17 15:29
# @Author  : ZhangYang
# @Email   : ian.zhang.88@outlook.com

from .task import Task
import json
import logging
import traceback

class Job(Task):
    def __init__(self,zk_client, base_path):
        self.job_base_path = base_path
        self.job_task_base_path = base_path + '/job_task'
        zk_client.ensure_path(self.job_task_base_path)
        super().__init__(zk_client, self.job_task_base_path)

        self.priority = 0

        self.priority_path=self.job_base_path+'/priority'
        self.meta_data_path=self.job_base_path+'/meta_data'
        self.id_path = self.job_base_path+'/id'
        self.cluster_path = self.job_base_path +'/cluster'
        self.type_path = self.job_base_path + '/type'

    def job_path(self):
        return self.job_base_path

    def parse(self, data):
        #data = None
        # try:
        #     data = json.loads(data_str)
        #
        # except Exception as e:
        #     logging.error('parse job data failed')
        #     return False

        try:
            self.set_id(data['id'])
            self.set_meta_data(data['meta_data'])
            self.set_cluster(data['cluster'])
            self.set_type(data['type'])
            self.set_priority(str(data['priority']))

            # task
            self.set_data(json.dumps(data['data']))

        except Exception as e:
            logging.error('setup job failed %s', str(e))
            traceback.print_exc()
            return False

        return True

    def delete(self):
        self.zk_client.delete(self.job_base_path, recursive=True)

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


    @Task.set('priority_path')
    def set_priority(self, data):
        self.zk_client.set(self.priority_path, data.encode('utf-8'))

    @Task.get('priority_path')
    def get_priority(self):
        return int(self.zk_client.get(self.priority_path)[0].decode('utf-8'))