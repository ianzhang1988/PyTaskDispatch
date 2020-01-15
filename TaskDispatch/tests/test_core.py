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
        self.zk_client.delete('/testTaskDispatch',recursive=True)
        self.core = Core(self.zk_client, '/testTaskDispatch')

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
        self.core.add_cluster('test_1')
        ret, path = self.core.add_new_job(data)
        self.assertEqual(ret, True)
        self.core.prepare_dequeue_job()

        j = Job(self.zk_client, path)

        # simulate job_task working
        j.set_state(TaskStateCode.WORKING)

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
            task_data=json.dumps(task_data)
            ret, path = self.core.add_new_task(j.job_path(), task_data)
            self.assertEqual(ret, True)

    def _test_task_dequeue(self):

        tesk_path_set = set()

        for i in range(10):
            ret, task_path = self.core.get_queue_task(cluster_name='test_1', task_type='test')
            print(task_path)
            self.assertEqual(ret, True)


            t = Task(self.zk_client, task_path)
            tesk_path_set.add(task_path)

            self.assertTrue('mark' in t.get_data())

        self.assertEqual(10, len(tesk_path_set))


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

    def test_update_task_state(self):

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
        self.core.add_cluster('test_1')
        ret, path = self.core.add_new_job(data)
        self.assertEqual(ret, True)

        j = Job(self.zk_client, path)

        self.core.update_task_state(path+'/job_task', TaskStateCode.WORKING)

        self.assertEqual(j.get_state(), TaskStateCode.WORKING)

        j.delete()

    def test_clean_finished_job(self):
        new_jobs = [[] for i in range(2)]
        all_jobs = []

        self.core._cluster_job={}

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
                job_data = job_data_template.format(id=i, cluster=c, priority=i)
                data = json.loads(job_data)
                ret, path = self.core.add_new_job(data)
                self.assertEqual(ret, True)
                j = Job(self.zk_client, path)
                new_jobs[c].append(j)
                all_jobs.append(j)

        self.core._cluster_job_upper_threshold=30
        self.core._cluster_job_lower_threshold=20
        self.core.add_cluster('test_0')
        self.core.add_cluster('test_1')

        self.core.prepare_dequeue_job()

        for i,j in enumerate(new_jobs[0]):
            if i % 2 == 0:
                j.set_state(TaskStateCode.FINISHED)

        for i,j in enumerate(new_jobs[1]):
            if i % 2 != 0:
                j.set_state(TaskStateCode.FINISHED)

        self.core.clean_finished_job()

        self.assertEqual(len(self.core._cluster_job['test_0']), 5)
        self.assertEqual(len(self.core._cluster_job['test_1']), 5)

        self.assertEqual(self.core._cluster_job['test_0'][1].get_id(), "test_0_7")
        self.assertEqual(self.core._cluster_job['test_1'][1].get_id(), "test_1_6")

        for j in all_jobs:
            j.delete()

    def test_check_abnormal(self):
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
        self.core.add_cluster('test_1')
        ret, path = self.core.add_new_job(data)
        self.assertEqual(ret, True)
        self.core.prepare_dequeue_job()

        j = Job(self.zk_client, path)

        task_data = {
            'data':{
                'mark': '1',
            },
        }
        task_data=json.dumps(task_data)
        ret, task_path = self.core.add_new_task(j.job_path(), task_data)
        self.assertEqual(ret, True)

        t = Task(self.zk_client, task_path)

        self.zk_client.create(path+'/job_task/worker','/job_worker'.encode('utf-8'))
        self.zk_client.create(task_path + '/worker', '/task_worker'.encode('utf-8'))

        # simulate job_task working
        j.set_state(TaskStateCode.WORKING)
        t.set_state(TaskStateCode.WORKING)

        self.core.check_abnormal()

        self.assertTrue(t.check_worker() == True)
        self.assertTrue(j.check_worker() == True)

        # when worker lost
        self.zk_client.delete(task_path + '/worker')

        self.core.check_abnormal()

        self.assertTrue(t.check_worker() == False)
        self.assertTrue(t.get_state() == TaskStateCode.QUEUE)

        self.zk_client.delete(path + '/job_task/worker')

        self.core.check_abnormal()

        self.assertTrue(t.get_omega_state() == TaskStateCode.KILL)
        self.assertTrue(j.check_worker() == False)
        self.assertTrue(j.get_state() == TaskStateCode.QUEUE)

        self.zk_client.create(path+'/job_task/worker','/job_worker'.encode('utf-8'))
        self.zk_client.create(task_path + '/worker', '/task_worker'.encode('utf-8'))

        j.set_state(TaskStateCode.WORKING)
        t.set_state(TaskStateCode.WORKING)

        self.core.check_abnormal()

        self.assertTrue(t.check_worker() == True)
        self.assertTrue(j.check_worker() == True)

        # test time out

        j.delete()

    def test_timeout(self):
        self.core.job_timeout = datetime.timedelta(hours=1)
        self.core.task_timeout = datetime.timedelta(minutes=40)

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
        self.core.add_cluster('test_1')
        ret, path = self.core.add_new_job(data)
        self.assertEqual(ret, True)
        self.core.prepare_dequeue_job()

        # set job time out 1h task time out 40min
        # task time out, reschedule, job time out

        now = datetime.datetime.now()

        job_path, task_path, task_data = self.core.get_dequeue_job_task('test_1', 'test')

        self.assertEqual(path, job_path)

        j = Job(self.zk_client, job_path)

        j.set_state(TaskStateCode.WORKING)

        task_data = {
            'data': {
                'mark': '1',
            },
        }
        task_data = json.dumps(task_data)
        ret, task_path_tmp = self.core.add_new_task(j.job_path(), task_data)
        self.assertEqual(ret, True)

        ret, task_path = self.core.get_queue_task('test_1', 'test')

        self.assertEqual(True, ret, task_path)
        self.assertEqual(task_path, task_path_tmp)

        t = Task(self.zk_client, task_path)

        self.core.timeout_manager.check( now + datetime.timedelta(minutes=41) )

        self.assertEqual(TaskStateCode.TIMEOUT,t.get_omega_state())

        # dispatch again
        self.zk_client.create(path+'/job_task/worker','/job_worker'.encode('utf-8'))
        self.zk_client.create(task_path + '/worker', '/task_worker'.encode('utf-8'))
        self.core.check_abnormal()

        self.assertEqual(TaskStateCode.QUEUE, t.get_state())

        ret, task_path = self.core.get_queue_task('test_1', 'test')

        self.assertEqual(True, ret)
        self.assertEqual(task_path, task_path_tmp)

        self.assertEqual(TaskStateCode.READY, t.get_state())

        self.core.timeout_manager.check(now + datetime.timedelta(minutes=80))

        self.assertEqual(TaskStateCode.TIMEOUT, j.get_omega_state())
        # job time out set task to kill or some step set all time out job's task kill ?
        self.assertEqual(TaskStateCode.KILL, t.get_omega_state())

        # clean
        j.delete()