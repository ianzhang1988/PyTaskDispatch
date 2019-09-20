# -*- coding: utf-8 -*-
# @Time    : 2019/9/17 18:13
# @Author  : ZhangYang
# @Email   : ian.zhang.88@outlook.com

import unittest
from .utility import ZkClientMixin
from ..master.core import Core
from ..master.task import Task
from ..master.job import Job
from ..master.consts import TaskStateCode
import datetime
import json

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
            "type":"test"
        }
        '''
        data = json.loads(job_data)

        ret, path = self.core.add_new_job(data)
        self.assertEqual(ret, True)

        j = Job(self.client, path)
        self.assertEqual( j.get_id(), 'test')
        self.assertEqual( j.get_data(), json.dumps(json.loads('{"hello":"world"}')) )
        self.assertEqual( j.get_type(), 'test')

