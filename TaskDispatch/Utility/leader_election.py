# -*- coding: utf-8 -*-
# @Time    : 2019/8/29 18:07
# @Author  : ZhangYang
# @Email   : ian.zhang.88@outlook.com
import threading, time
import uuid
import logging
from kazoo.exceptions import NodeExistsError

"""
class Election's callback, it's life time is just inside a Lock, when callback return Lock will be released.
so either we put all leader's work in inside callback which is not a good idea, or we use another approach.
What we will do in callback is, we put election's name in /path/leader, and after read it back signal main
thread got leadership. And we continuously check /path/leader, if exception occur signal main thread quit,
rerun election if possible or quit the whole program.
"""

class LeaderElection(threading.Thread):
    def __init__(self, client, path, name=''):
        super(LeaderElection,self).__init__()
        self.client = client
        self.name =name
        self.election_path = path+'/election'
        self.leader_path = path + '/leader'
        self.is_leader = False
        self.callback = None
        self.work_flag=True
        self.election = None


    def set_callback(self, callback):
        self.callback = callback

    def quit(self):
        self.work_flag = False

        try:
            if self.election:
                self.election.cancel()

            if self.is_leader:
                self.client.delete(self.leader_path)
        except Exception as e:
            # if exception, probably lost connection, ephemeral node that created before would be delete automatically
            logging.warning('quit election zookeeper error: %s', str(e))


        if self.is_leader:
            self.is_leader = False
            self.callback(False, self)



    def _elected(self, name):

        try:
            self.client.create(self.leader_path, self.name.encode('utf-8'), ephemeral=True)

        except NodeExistsError as e:
            return

        # print( '---------',self.name, 'is leader' )

        self.is_leader = True
        self.callback(True, self)

    def _handle_exception(self):
        if not self.is_leader:
            return

        self.is_leader = False
        self.callback(False, self)

    def _need_run_election(self):
        try:
            leader_exist = self.client.exists(self.leader_path)

            if self.is_leader:

                if not leader_exist:
                    self._handle_exception()
                    return True
                else:
                    leader_name ,Znode_status= self.client.get(self.leader_path)
                    leader_name = leader_name.decode('utf-8')
                    # print('-------- leader_name', leader_name)
                    # print('-------- self.name', self.name)
                    if leader_name != self.name:
                        self._handle_exception()
                        return True

                    return False

            else:

                if leader_exist:
                    return False
                else:
                    return True


        except Exception as e:
            logging.error('_need_run_election error %s', str(e))
            self._handle_exception()
            return False


    def _run_election(self):
        try:

            if not self.name:
                self.name = "c" + uuid.uuid4().hex

            self.election = self.client.Election(self.election_path, self.name)

            self.election.run(self._elected, self.name)

        except Exception as e:
            logging.error('election exception: %s', str(e))
            self._handle_exception()
            return False

    def run(self):
        while self.work_flag:
            if self._need_run_election():
                self._run_election()

            time.sleep(1)



