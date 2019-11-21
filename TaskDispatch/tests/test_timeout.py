# -*- coding: utf-8 -*-
# @Time    : 2019/11/18 17:53
# @Author  : ZhangYang
# @Email   : ian.zhang.88@outlook.com

import unittest
from .utility import ZkClientMixin
from ..master.core import Core
# from ..master.task import Task
# from ..master.job import Job
# from ..master.cluster import Cluster
from ..master.consts import TaskStateCode, TimeoutType
from ..master.timeout import TimeoutManager, TimeoutEvent
import datetime
import json
import random
from datetime import datetime, timedelta


class TimeoutCallbackRegisterTest():
    def __init__(self):
        pass

class TestTimeout(unittest.TestCase, ZkClientMixin):

    def setUp(self):
        ZkClientMixin.__init__(self)

        self.zk_client = self.get_client()
        self.zk_client.delete('/testTimeout',recursive=True)
        self.zk_client.create('/testTimeout', ''.encode('utf-8'))

        #self.core = Core(self.zk_client, '/testTimeout/core')
        self.timeout_manager = TimeoutManager(self.zk_client, '/testTimeout/timeout')
        #register = TimeoutCallbackRegister(self.core)
        #register = TimeoutCallbackRegisterTest()

        # self.timeout_manager.set_callback_register(register)、

    def test_timeout_event(self):
        self.zk_client.create('/testTimeout/timeout_test1', ''.encode('utf-8'))
        self.zk_client.create('/testTimeout/timeout_test2', ''.encode('utf-8'))

        to1 = TimeoutEvent(self.zk_client, '/testTimeout/timeout_test1/')
        now = datetime.strptime('2018-01-01 00:00:00','%Y-%m-%d %H:%M:%S')
        final_time = datetime.strptime('2018-01-01 00:05:00','%Y-%m-%d %H:%M:%S')
        interval = timedelta(seconds=30)

        to1.init_time(final_time, interval, now)
        to1.type = TimeoutType.Job
        to1.task_path= 'test_task'
        to1.callback_func='test'
        to1.parameters={"key":"value"}

        to1.sync()

        to1_clone = TimeoutEvent(self.zk_client, '/testTimeout/timeout_test1/')
        to1_clone.load()

        self.assertEqual(to1.type, to1_clone.type)
        self.assertEqual(to1.task_path, to1_clone.task_path)
        self.assertEqual(to1.callback_func, to1_clone.callback_func)
        self.assertEqual(to1.parameters, to1_clone.parameters)
        self.assertEqual(to1.time_out, to1_clone.time_out)
        self.assertEqual(to1.interval, to1_clone.interval)
        self.assertEqual(to1.final_time, to1_clone.final_time)

        now_tmp = now
        timeout_count = 0
        for _ in range(5*60+1):
            now_tmp+=timedelta(seconds=1)

            if to1.check_time_out(now_tmp):
                timeout_count+=1

        self.assertEqual(timeout_count, 10)
        self.assertTrue(to1.check_final(now_tmp))

        to2 = TimeoutEvent(self.zk_client, '/testTimeout/timeout_test2/')
        final_time = datetime.strptime('2018-01-01 00:05:00', '%Y-%m-%d %H:%M:%S')

        to2.init_time(final_time)
        to2.type = TimeoutType.Task
        to2.callback_func = 'test'
        to2.parameters = {"key": "value"}

        now = datetime.strptime('2018-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
        self.assertFalse(to2.check_time_out(now))
        self.assertFalse(to2.check_final(now))
        now = datetime.strptime('2018-01-01 00:05:01', '%Y-%m-%d %H:%M:%S')
        self.assertTrue(to2.check_time_out(now))
        self.assertTrue(to2.check_final(now))

    def test_add(self):
        self._add_time_out()

    def _add_time_out(self):
        self.zk_client.create('testTimeout/task01',''.encode('utf-8'))
        self.zk_client.create('testTimeout/task02', ''.encode('utf-8'))
        self.zk_client.create('testTimeout/task03', ''.encode('utf-8'))

        final_time = datetime.strptime('2018-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
        interval = timedelta(seconds=300)

        self.timeout_manager.add(TimeoutType.Task, 'testTimeout/task01', 'test_print', {'content': 'hello1'},
                                 final_time, interval)
        self.timeout_manager.add(TimeoutType.Task, 'testTimeout/task02', 'test_print', {'content': 'hello2'},
                                 final_time)
        self.timeout_manager.add(TimeoutType.Task, 'testTimeout/task03', 'test_print', {'content': 'hello3'},
                                 final_time)

        # test load
        timeout_manager_tmp = TimeoutManager(self.zk_client, '/testTimeout/timeout')
        self.assertEqual(len(timeout_manager_tmp._timeout_event_list),3)
        for i in self.timeout_manager._timeout_event_list:
            self.assertEqual(i.callback_func, 'test_print')

        # here only test task01
        found = False
        for i in self.timeout_manager._timeout_event_list:
            if '01' not in i.task_path:
                continue

            found = True
            data = self.zk_client.get(i.path)[0]

            dict_data = json.loads(data)

            self.assertEqual('test_print', dict_data['callback_func'])
            self.assertEqual({'content': 'hello1'}, dict_data['parameters'])
            self.assertEqual(interval, timedelta(seconds= dict_data['interval']))

        self.assertTrue(found)

    def test_for_nothing(self):
        self.assertTrue(True)