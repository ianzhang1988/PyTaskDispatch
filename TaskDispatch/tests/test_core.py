# -*- coding: utf-8 -*-
# @Time    : 2019/9/17 18:13
# @Author  : ZhangYang
# @Email   : ian.zhang.88@outlook.com

import unittest
from .utility import ZkClientMixin
from ..master.core import Core
from ..master.task import Task
from ..master.job import Job
from ..master.cluster import Cluster
from ..master.consts import TaskStateCode
import datetime
import json
import random

class TestTask(unittest.TestCase, ZkClientMixin):

    def setUp(self):
        ZkClientMixin.__init__(self)

        self.zk_client = self.get_client()
        self.core = Core(self.zk_client)


    def test_add_job(self):
        job_data = '''
        {
            "id":"test",
            "meta_data":"",
            "data": {
                "hello":"world"
            },
            "cluster":"test",
            "type":"test",
            "priority": 10
        }
        '''
        data = json.loads(job_data)

        ret, path = self.core.add_new_job(data)
        self.assertEqual(ret, True)
        print('test_add_job:',path)

        j = Job(self.zk_client, path)
        self.assertEqual( j.get_id(), 'test')
        self.assertEqual( j.get_data(), json.dumps(json.loads('{"hello":"world"}')) )
        self.assertEqual( j.get_type(), 'test')
        self.assertEqual( j.get_priority(), 10)
        self.assertEqual( j.get_state(), TaskStateCode.QUEUE)

        j.delete()
        self.assertEqual( None, self.zk_client.exists(path))


    def test_get_dequeue_job_task(self):
        new_jobs=[[] for i in range(2)]
        all_jobs=[]

        for c in range(2):
            for i in range(10):
                job_data_template = """
                {{
                    "id":"test_{cluster}_{id}",
                    "meta_data":"",
                    "data": {{
                        "hello":"world"
                    }},
                    "cluster":"test_{cluster}",
                    "type":"test",
                    "priority": {priority}
                }}
                """
                job_data = job_data_template.format(id=i,cluster=c, priority=i)
                data = json.loads(job_data)
                ret, path = self.core.add_new_job(data)
                self.assertEqual(ret, True)
                j = Job(self.zk_client, path)
                new_jobs[c].append(j)
                all_jobs.append(j)

        self.core._cluster_job_upper_threshold=5
        self.core._cluster_job_lower_threshold=3
        self.core.add_cluster('test_0')
        self.core.add_cluster('test_1')

        self.core.prepare_dequeue_job()

        self.assertEqual(5, len(self.core._cluster_job['test_0']))

        for j in self.core._cluster_job['test_0']:
            self.assertLess(4, j.get_priority())

        job_path, task_path, task_data = self.core.get_dequeue_job_task(cluster_name='test_0', task_type='test')

        print('job_path', job_path )
        print('task_path', task_path)
        self.assertEqual(job_path+'/job_task', task_path)
        dequeue_job_task = Job(self.zk_client, job_path)
        self.assertEqual(9,                         dequeue_job_task.get_priority())
        self.assertEqual(TaskStateCode.READY,       dequeue_job_task.get_state() )
        self.assertEqual(task_data,                 dequeue_job_task.get_data())

        job_path, task_path, task_data = self.core.get_dequeue_job_task(cluster_name='test_0', task_type='test')

        dequeue_job_task = Job(self.zk_client, job_path)
        self.assertEqual(8, dequeue_job_task.get_priority())
        self.assertEqual(TaskStateCode.READY,       dequeue_job_task.get_state())

        for j in all_jobs:
            j.delete()

    def test_task(self):
        job_data_template = """
            {{
                "id":"test_{cluster}_{id}",
                "meta_data":"",
                "data": {{
                    "hello":"world"
                }},
                "cluster":"test_{cluster}",
                "type":"test",
                "priority": {priority}
            }}
            """
        job_data = job_data_template.format(id='001', cluster=1, priority=5)
        data = json.loads(job_data)
        ret, path = self.core.add_new_job(data)
        self.assertEqual(ret, True)
        j = Job(self.zk_client, path)

        self._test_task_add(j)

        self._test_task_dequeue()

        j.delete()

    def _test_task_add(self, j):

        for i in range(10):
            task_data = {
                'data':{
                    'mark': str(i),
                },
            }
            ret, path = self.core.add_new_task(j.job_path(), task_data)
            self.assertEqual(ret, True)

    def _test_task_dequeue(self):

        ret, task_path, task_data = self.core.get_enqueue_task(cluster_name='test_1', task_type='test')
        self.assertEqual(ret, True)

        t = Task(self.zk_client, task_path)
        self.assertTrue('001' in task_path)
        self.assertEqual(task_data, t.get_data)

    # no need for a new class
    def test_cluster(self):
        c = Cluster(self.zk_client, '/test/clusters')
        c.add('test1')
        c.add('test2')

        clusters = c.get_all()

        self.assertEqual(2, len(clusters))
        self.assertTrue('test1' in clusters)
        self.assertTrue('test2' in clusters)

        ret = c.delete('test1')
        self.assertEqual(True, ret)

        clusters = c.get_all()
        self.assertEqual(1, len(clusters))
        self.assertTrue('test2' in clusters)

        ret = c.delete('test1')
        self.assertEqual(True, ret)

        ret = c.delete('test2')
        self.assertEqual(True, ret)

        clusters = c.get_all()
        self.assertEqual(0, len(clusters))

