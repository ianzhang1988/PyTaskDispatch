# -*- coding: utf-8 -*-
# @Time    : 2019/9/10 16:25
# @Author  : ZhangYang
# @Email   : ian.zhang.88@outlook.com

import unittest
from .utility import ZkClientMixin
from ..master.task import Task
import datetime


class TestTask(unittest.TestCase, ZkClientMixin):

    def setUp(self):
        ZkClientMixin.__init__(self)

        self.zk_client = self.get_client()
        self.base_path='/test/test_task'


        if not self.zk_client.exists(self.base_path):
            self.zk_client.ensure_path(self.base_path)
        else:
            self.zk_client.delete(self.base_path, recursive=True)
            self.zk_client.ensure_path(self.base_path)


    def test_task(self):
        t = Task(self.zk_client, self.base_path)

        self.assertEqual(t.get_worker(), None)

        # worker set worker path
        self.zk_client.create(self.base_path+'/worker/', '/worker/w'.encode('utf-8'))
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
