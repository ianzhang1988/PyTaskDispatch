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


    # def test_get_dequeue_job_task(self):
    #     new_jobs=[[] for i in range(2)]
    #     all_jobs=[]
    #
    #     for c in range(2):
    #         for i in range(10):
    #             job_data_template = '''
    #             {
    #                 "id":"test_{cluster}_{id}",
    #                 "meta_data":"",
    #                 "data": {
    #                     "hello":"world"
    #                 },
    #                 "cluster":"test_{cluster}",
    #                 "type":"test",
    #                 "priority": {priority}
    #             }
    #             '''
    #             job_data = job_data_template.format(id=i,cluster=c, priority=i)
    #             data = json.loads(job_data)
    #             ret, path = self.core.add_new_job(data)
    #             self.assertEqual(ret, True)
    #             j = Job(self.zk_client, path)
    #             new_jobs[c].append(j)
    #             all_jobs.append(j)
    #
    #     self.core._cluster_job_upper_threshold=5
    #     self.core._cluster_job_lower_threshold=3
    #     self.core.add_cluster('test_0')
    #     self.core.add_cluster('test_1')
    #
    #     self.core.prepare_dequeue_job()
    #
    #     self.assertEqual(5, len(self.core._cluster['test_0']))
    #
    #     for j in self.core._cluster['test_0']:
    #         self.assertLess(4, j.get_priority())
    #
    #     job_path = self.core.get_dequque_job_task(cluster='test_0', type='test')
    #
    #     dequeue_job_task = Job(self.zk_client, job_path)
    #     self.assertEqual(9,                      dequeue_job_task.get_priority())
    #     self.assertEqual(TaskStateCode.DEQUEUE , dequeue_job_task.get_state() )
    #
    #     job_path = self.core.get_dequque_job_task(cluster='test_0', type='test')
    #
    #     dequeue_job_task = Job(self.zk_client, job_path)
    #     self.assertEqual(8, dequeue_job_task.get_priority())
    #     self.assertEqual(TaskStateCode.DEQUEUE, dequeue_job_task.get_state())

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

