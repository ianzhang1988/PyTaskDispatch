# -*- coding: utf-8 -*-
# @Time    : 2019/9/10 16:25
# @Author  : ZhangYang
# @Email   : ian.zhang.88@outlook.com

import unittest
from .utility import ZkClientMixin
from ..master.task import Task
from ..master.job import Job
from ..master.utility import create_new_sequence_node
from ..master.consts import TaskStateCode
import datetime
import time


class TestTask(unittest.TestCase, ZkClientMixin):

    def setUp(self):
        ZkClientMixin.__init__(self)

        self.zk_client = self.get_client()

        ### task
        self.base_path='/test/test_task'

        if not self.zk_client.exists(self.base_path):
            self.zk_client.ensure_path(self.base_path)
        else:
            self.zk_client.delete(self.base_path, recursive=True)
            self.zk_client.ensure_path(self.base_path)


        ### job
        self.job_base_path = '/test/test_job'

        if not self.zk_client.exists(self.job_base_path):
            self.zk_client.ensure_path(self.job_base_path)
        else:
            self.zk_client.delete(self.job_base_path, recursive=True)
            self.zk_client.ensure_path(self.job_base_path)



    def test_task(self):
        t = Task(self.zk_client, self.base_path)

        self.zk_client.create(self.base_path + '/task_state/', 'working'.encode('utf-8'))

        self.assertEqual(t.get_worker(), None)
        # worker set worker path
        self.zk_client.create(self.base_path + '/worker/', '/worker/w'.encode('utf-8'))

        self._test_task(t)

    def _test_task(self, t):

        self.assertEqual(t.get_worker(), '/worker/w')

        t.set_data('{"k":"v"}')
        self.assertEqual(t.get_data(), '{"k":"v"}')

        t.set_start_time('2019-09-10 00:00:00')
        self.assertEqual(t.get_start_time(), '2019-09-10 00:00:00')

        sec = t.get_runtime()
        self.assertLessEqual(sec, (datetime.datetime.now() - datetime.datetime.strptime('2019-09-10 00:00:00','%Y-%m-%d %H:%M:%S')).seconds)

        t.set_end_time('2019-09-10 00:02:00')
        sec = t.get_runtime_total()
        self.assertEqual(sec, 120)

        self.assertEqual(t.get_state(), TaskStateCode.WORKING)

        t.set_enqueue_time('2019-09-10 00:00:00')
        self.assertEqual(t.get_enqueue_time(), '2019-09-10 00:00:00')

        t.set_dequeue_time('2019-09-10 00:00:00')
        self.assertEqual(t.get_dequeue_time(), '2019-09-10 00:00:00')

        t.set_omega_state(TaskStateCode.KILL)
        self.assertEqual(TaskStateCode.KILL, t.get_omega_state())

        t.reset_omega_state()
        self.assertEqual(None, t.get_omega_state())

        key='hello'
        value='world'
        key2='ni'
        value2='hao'
        t.set_client_data(key, value)
        t.set_client_data(key2, value2)

        self.assertEqual(t.get_client_data(key), value)
        self.assertEqual(t.get_client_data_all(), {key:value, key2:value2})


    def test_job(self):
        t = Job(self.zk_client, self.job_base_path)
        self.zk_client.create(self.job_base_path + '/job_task/task_state/', 'working'.encode('utf-8'))
        self.assertEqual(t.get_worker(), None)
        self.zk_client.create(self.job_base_path + '/job_task/worker/', '/worker/w'.encode('utf-8'))

        # prove job still a valid task
        self._test_task(t)

        t.set_meta_data('{"k":"v"}')
        self.assertEqual(t.get_meta_data(), '{"k":"v"}')

        t.set_id('123')
        self.assertEqual(t.get_id(), '123')

        t.set_cluster('test')
        self.assertEqual(t.get_cluster(), 'test')

        t.set_type('universal')
        self.assertEqual(t.get_type(), 'universal')


    def test_job_get_tasks(self):
        j = Job(self.zk_client, self.job_base_path)

        new_tasks_path=set()

        for i in range(3):

            new_task_path = create_new_sequence_node(self.zk_client, j.task_base_path, 'task')

            t = Task(self.zk_client, new_task_path)
            t.set_data('test_%s'%i)
            t.set_state(TaskStateCode.QUEUE)

            new_tasks_path.add(new_task_path)

        self.assertEqual(new_tasks_path, set(j.get_tasks_path()))


    def test_check_worker_state(self):
        t = Task(self.zk_client, self.base_path)

        self.zk_client.create(self.base_path+'/worker','/worker/work0001'.encode('utf-8'))

        self.assertEqual(t.check_worker(), True)

        self.zk_client.delete(self.base_path+'/worker')

        self.assertEqual(t.check_worker(), False)


