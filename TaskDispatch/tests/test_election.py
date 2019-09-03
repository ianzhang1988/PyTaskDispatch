# -*- coding: utf-8 -*-
# @Time    : 2019/8/29 18:23
# @Author  : ZhangYang
# @Email   : ian.zhang.88@outlook.com

import unittest
import time

from .utility import ZkClientMixin
from ..Utility.leader_election import LeaderElection

class TestElection(unittest.TestCase, ZkClientMixin):

    def setUp(self):
        ZkClientMixin.__init__(self)

        self.client = self.get_client()
        self.contender_num = 3
        self.contender_list = []

    def tearDown(self):
        print('tearDown')
        for i in self.contender_list:
            i.quit()
            i.join()

        self.close()


    def _leader_callback(self, election):
        if election.is_leader:
            print('%s I am leader' % election.name)

        else:
            print('%s: quit', election.name)


    def test_election(self):

        for i in range(self.contender_num):
            self.contender_list.append(LeaderElection(self.client, '/test/election', 'contender%s'%i))

        for i in  self.contender_list:
            i.set_callback(self._leader_callback)
            i.start()

        time.sleep(1)

        self.assertTrue(any( [i.is_leader for i in self.contender_list] ))
        self.assertEqual(1, len([i for i in self.contender_list if i.is_leader]))







