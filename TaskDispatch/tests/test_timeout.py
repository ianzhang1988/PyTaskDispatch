# -*- coding: utf-8 -*-
# @Time    : 2019/11/18 17:53
# @Author  : ZhangYang
# @Email   : ian.zhang.88@outlook.com

import unittest
from .utility import ZkClientMixin
from ..master.consts import TaskStateCode, TimeoutType
from ..master.timeout import TimeoutManager, TimeoutEvent, TimeoutCallbackRegister
import datetime
import json
import random
from datetime import datetime, timedelta


class CallbackTest():
    def __init__(self, test_case):
        self.test_case = test_case
        self.test_input = []

    def test(self,base_path,  content):
        self.test_input.append(content)

        self.test_case.assertTrue('testTimeout' in base_path)
        self.test_case.assertTrue('hello' in content)

    def test2(self, base_path, v1, v2):

        self.test_case.assertTrue('testTimeout' in base_path)
        self.test_case.assertTrue('fall' in v1)
        self.test_case.assertTrue('out' in v2)


class TestTimeout(unittest.TestCase, ZkClientMixin):

    def setUp(self):
        ZkClientMixin.__init__(self)

        self.zk_client = self.get_client()
        self.zk_client.delete('/testTimeout',recursive=True)
        self.zk_client.create('/testTimeout', ''.encode('utf-8'))

        #self.core = Core(self.zk_client, '/testTimeout/core')
        self.timeout_manager = TimeoutManager(self.zk_client, '/testTimeout/timeout')

        self.fake_funcs = CallbackTest(self)
        self.callback_register = TimeoutCallbackRegister()
        self.callback_register.reg('test', self.fake_funcs.test)
        self.callback_register.reg('test2', self.fake_funcs.test2)

        self.timeout_manager.set_callback_register(self.callback_register)

    def test_timeout_event(self):
        self.zk_client.create('/testTimeout/timeout_test1', ''.encode('utf-8'))
        self.zk_client.create('/testTimeout/timeout_test2', ''.encode('utf-8'))

        to1 = TimeoutEvent(self.zk_client, '/testTimeout/timeout_test1/')
        now = datetime.strptime('2018-01-01 00:00:00','%Y-%m-%d %H:%M:%S')
        final_time = datetime.strptime('2018-01-01 00:10:00','%Y-%m-%d %H:%M:%S')
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
        for _ in range(5*60):
            now_tmp+=timedelta(seconds=1)

            if to1.check_time_out(now_tmp):
                timeout_count+=1

        self.assertEqual(timeout_count, 10)

        now_tmp+=timedelta(seconds=190)
        self.assertTrue(to1.check_time_out(now_tmp))
        self.assertTrue(to1.time_out == now_tmp+timedelta(seconds=20))

        now_tmp += timedelta(seconds=2000)
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
        # do we need to check the node in timeout manager?
        # if check here, we could remove some invalid timeout event, when job or task dead
        #
        # but its better that timeout manager don't know those things, just let owner of
        # callback funcs discard invalid calls
        #
        # self.zk_client.create('testTimeout/task01',''.encode('utf-8'))
        # self.zk_client.create('testTimeout/task02', ''.encode('utf-8'))
        # self.zk_client.create('testTimeout/task03', ''.encode('utf-8'))

        now = datetime.strptime('2018-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
        final_time = datetime.strptime('2018-01-01 01:00:00', '%Y-%m-%d %H:%M:%S')
        interval = timedelta(seconds=30)

        self.timeout_manager.add(TimeoutType.Task, '/testTimeout/task01', 'test', {'content': 'hello1'},
                                 final_time, interval, now)
        self.timeout_manager.add(TimeoutType.Task, '/testTimeout/task02', 'test', {'content': 'hello2'},
                                 final_time + timedelta(seconds=30))
        self.timeout_manager.add(TimeoutType.Task, '/testTimeout/task03', 'test', {'content': 'hello3'},
                                 final_time + timedelta(seconds=60))

        # test load
        timeout_manager_tmp = TimeoutManager(self.zk_client, '/testTimeout/timeout')
        self.assertEqual(len(timeout_manager_tmp._timeout_event_list),3)
        for i in self.timeout_manager._timeout_event_list:
            self.assertEqual(i.callback_func, 'test')

        # here only test task01
        found = False
        for i in self.timeout_manager._timeout_event_list:
            if '01' not in i.task_path:
                continue

            found = True
            data = self.zk_client.get(i.path)[0]

            dict_data = json.loads(data)

            self.assertEqual('test', dict_data['callback_func'])
            self.assertEqual({'content': 'hello1'}, dict_data['parameters'])
            self.assertEqual(interval, timedelta(seconds= dict_data['interval']))

        self.assertTrue(found)

    def test_callback_register(self):

        self.callback_register.call('test', '/testTimeout/task01', {'content': 'hello1'})
        self.callback_register.call('test', '/testTimeout/task01' ,{'content': 'hello2'})

        self.callback_register.call('test2', '/testTimeout/task01', {'v1': 'fall', 'v2':'out'})

        to1 = TimeoutEvent(self.zk_client, '/testTimeout/timeout_test1/')
        now = datetime.strptime('2018-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
        final_time = datetime.strptime('2018-01-01 00:05:00', '%Y-%m-%d %H:%M:%S')
        interval = timedelta(seconds=30)

        to1.init_time(final_time, interval, now)
        to1.type = TimeoutType.Job
        to1.task_path = '/testTimeout/task01'
        to1.callback_func = 'test'
        to1.parameters = {"content": "hello1"}

        self.callback_register.call_by_event(to1)

    def test_manager_check(self):
        self._add_time_out()

        now = datetime.strptime('2018-01-01 00:00:20', '%Y-%m-%d %H:%M:%S')

        self.timeout_manager.check(now)

        self.assertEqual(3, len(self.timeout_manager._timeout_event_list))
        self.assertEqual(0, len(self.fake_funcs.test_input))

        now = datetime.strptime('2018-01-01 00:00:30', '%Y-%m-%d %H:%M:%S')
        self.timeout_manager.check(now)

        self.assertEqual(3, len(self.timeout_manager._timeout_event_list))
        self.assertEqual(1, len(self.fake_funcs.test_input))

        now = datetime.strptime('2018-01-01 00:01:05', '%Y-%m-%d %H:%M:%S')
        self.timeout_manager.check(now)

        self.assertEqual(3, len(self.timeout_manager._timeout_event_list))
        self.assertEqual(2, len(self.fake_funcs.test_input))

        now = datetime.strptime('2018-01-01 00:01:30', '%Y-%m-%d %H:%M:%S')
        self.timeout_manager.check(now)

        self.assertEqual(3, len(self.timeout_manager._timeout_event_list))
        self.assertEqual(3, len(self.fake_funcs.test_input))

        now = datetime.strptime('2018-01-01 00:30:00', '%Y-%m-%d %H:%M:%S')
        self.timeout_manager.check(now)

        self.assertEqual(3, len(self.timeout_manager._timeout_event_list))
        self.assertEqual(4, len(self.fake_funcs.test_input))

        now = datetime.strptime('2018-01-01 01:00:00', '%Y-%m-%d %H:%M:%S')
        self.timeout_manager.check(now)

        self.assertEqual(2, len(self.timeout_manager._timeout_event_list))
        self.assertEqual(5, len(self.fake_funcs.test_input))

        self.timeout_manager.delete('/testTimeout/task02')

        now = datetime.strptime('2018-01-01 01:00:30', '%Y-%m-%d %H:%M:%S')
        self.timeout_manager.check(now)

        self.assertEqual(1, len(self.timeout_manager._timeout_event_list))
        self.assertEqual(5, len(self.fake_funcs.test_input))

        self.timeout_manager.add(TimeoutType.Task, '/testTimeout/task04', 'test', {'content': 'hello4'},
                                 now + timedelta(seconds=60))  # 01:01:30

        self.assertEqual(2, len(self.timeout_manager._timeout_event_list))
        self.assertEqual(5, len(self.fake_funcs.test_input))

        now = datetime.strptime('2018-01-01 01:01:00', '%Y-%m-%d %H:%M:%S')
        self.timeout_manager.check(now)

        self.assertEqual(1, len(self.timeout_manager._timeout_event_list))
        self.assertEqual(6, len(self.fake_funcs.test_input))

        now = datetime.strptime('2018-01-01 01:01:30', '%Y-%m-%d %H:%M:%S')
        self.timeout_manager.check(now)

        self.assertEqual(0, len(self.timeout_manager._timeout_event_list))
        self.assertEqual(7, len(self.fake_funcs.test_input))

        now = datetime.strptime('2018-01-01 01:02:00', '%Y-%m-%d %H:%M:%S')
        self.timeout_manager.check(now)

        self.assertEqual(0, len(self.timeout_manager._timeout_event_list))
        self.assertEqual(7, len(self.fake_funcs.test_input))
