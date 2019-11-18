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
from ..master.timeout import TimeoutManager
import datetime
import json
import random

class TestTask(unittest.TestCase, ZkClientMixin):

    def setUp(self):
        ZkClientMixin.__init__(self)

        self.zk_client = self.get_client()
        self.zk_client.delete('/testTimeout',recursive=True)

        #self.core = Core(self.zk_client, '/testTimeout/core')
        self.timeout_manager = TimeoutManager(self.zk_client, '/testTimeout/timeout')
        #register = TimeoutCallbackRegister(self.core)
        register = TimeoutCallbackRegisterTest()

        self.timeout_manager.set_callback_register(register)

    def test_add(self):
        self._add_time_out()

    def _add_time_out(self):
        self.zk_client.create('testTimeout/task01',''.encode('utf-8'))
        self.zk_client.create('testTimeout/task02', ''.encode('utf-8'))
        self.zk_client.create('testTimeout/task03', ''.encode('utf-8'))

        self.timeout_manager.add(TimeoutType.Task, 'testTimeout/task01', 'test_print', {'content': 'hello1'},
                                 '2018-01-01 00:00:00', 300)
        self.timeout_manager.add(TimeoutType.Task, 'testTimeout/task02', 'test_print', {'content': 'hello2'},
                                 '2018-01-01 00:00:00')
        self.timeout_manager.add(TimeoutType.Task, 'testTimeout/task03', 'test_print', {'content': 'hello3'},
                                 '2018-01-01 00:00:00')

        # here only test task01
        found = False
        for i in self.timeout_manager._timeout_event_list:
            if i.path != 'testTimeout/task01':
                continue

            found = True
            timeout_event_path = i.base_path

            data = self.zk_client.get('timeout_event_path')

            dict_data = json.loads(data)

            self.assertEqual('testTimeout/task01', dict_data['path'])
            self.assertEqual('test_print', dict_data['test_print'])
            self.assertEqual({'content': 'hello1'}, dict_data['param'])
            self.assertEqual(300, dict_data['interval'])


        self.assertTrue(found)
