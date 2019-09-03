# -*- coding: utf-8 -*-
# @Time    : 2019/8/29 18:07
# @Author  : ZhangYang
# @Email   : ian.zhang.88@outlook.com
import threading, time
import uuid
import logging

class LeaderElection(threading.Thread):
    def __init__(self, client, path, name=''):
        super(LeaderElection,self).__init__()
        self.client = client
        self.name =name
        self.path = path
        self.is_leader = False
        self.callback = None
        self.work_flag=True
        self.election = None


    def set_callback(self, callback):
        self.callback = callback

    def quit(self):
        self.work_flag = False
        if self.election:
            self.election.cancel()

        if self.is_leader:
            self.is_leader = False
            self.callback(self)


    def _elected(self, name):
        self.is_leader = True
        self.callback(self)

    def _handle_exception(self):
        if not self.is_leader:
            return

        self.is_leader = False
        self.callback(self)


    def _run_election(self):
        try:
            if not self.name:
                self.name = "c" + uuid.uuid4().hex

            self.election = self.client.Election(self.path, self.name)

            print('_run_election 1')
            self.election.run(self._elected, self.name)
            print('_run_election 2')
        except Exception as e:
            logging.error('election exception %s', str(e))
            self._handle_exception()
            return False

    def run(self):
        while self.work_flag:
            if not self._run_election():
                time.sleep(1)



