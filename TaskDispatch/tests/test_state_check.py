# -*- coding: utf-8 -*-
# @Time    : 2019/12/24 16:35
# @Author  : ZhangYang
# @Email   : ian.zhang.88@outlook.com

import unittest
from ..master.consts import TaskStateCode
from ..master.state_check import StateGraph

class TestStateCheck(unittest.TestCase):
    def setUp(self):
        pass

    def test_state_change(self):
        sg = StateGraph()

        ret = sg.check_next(TaskStateCode.QUEUE, TaskStateCode.FINISHED)
        self.assertEqual(ret, False)

        ret = sg.check_next(TaskStateCode.READY, TaskStateCode.KILL)
        self.assertEqual(ret, True)

        ret = sg.check_next(TaskStateCode.WORKING, TaskStateCode.FINISHED)
        self.assertEqual(ret, True)

        ret = sg.check_next(TaskStateCode.DEQUEUE, TaskStateCode.TIMEOUT)
        self.assertEqual(ret, False)

        ret = sg.check_next(TaskStateCode.WORKING, TaskStateCode.DEQUEUE)
        self.assertEqual(ret, False)