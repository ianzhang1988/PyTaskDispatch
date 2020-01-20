# -*- coding: utf-8 -*-
# @Time    : 2019/10/24 18:08
# @Author  : ZhangYang
# @Email   : ian.zhang.88@outlook.com

import sys
sys.path.append('../')
from kazoo.client import KazooClient
from threading import Thread, Lock
from TaskDispatch.master.core import Core
from TaskDispatch.Utility.task import Task
from TaskDispatch.master.consts import TaskStateCode
import time, json
import random
import logging

class Simulation():
    def __init__(self):
        self.zookeeper_host='127.0.0.1:2181'
        self.zk_client = KazooClient(hosts=self.zookeeper_host)
        self.zk_client.start()
        self.core = Core(self.zk_client, '/SimTaskDispatch')

        self.server_thread = None
        self.normal_client = []
        self.global_lock = Lock()

        self.work_flag=True


    def start_sim(self):
        self.core.add_cluster('test_0')
        self.core.add_cluster('test_1')

        self.add_job()

        self.server_thread = Thread(target=self._server_sim)
        self.server_thread.start()

        for i in range(3):
            t = Thread(target=self._normal_client_job_task_sim)
            t.start()
            self.normal_client.append(t)
            t = Thread(target=self._normal_client_task_sim)
            t.start()
            self.normal_client.append(t)

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt as e:
            self.work_flag=False

        self.server_thread.join()
        for t in self.normal_client:
            t.join()

    def add_job(self):
        for c in range(2):
            for i in range(10):
                random1 = random.randrange(1,5)
                random2 = random.randrange(1, 5)
                job_data_template = """
                {{
                    "id":"test_{cluster}_{id}",
                    "meta_data":"",
                    "data": {{
                        "lhv":"{v_1}",
                        "rhv":"{v_2}",
                        "operator":"+"
                    }},
                    "cluster":"test_{cluster}",
                    "type":"test",
                    "priority": {priority}
                }}
                """
                job_data = job_data_template.format(id=i,cluster=c, priority=i, v_1=random1,v_2=random2)
                data = json.loads(job_data)
                ret, path = self.core.add_new_job(data)


    def _server_sim(self):
        self.core._cluster_job_upper_threshold=6
        self.core._cluster_job_lower_threshold=4

        self.core.make_sure_tree_structure()

        while self.work_flag:
            with self.global_lock:
                # print('-------- loop ping')
                self.core.clean_finished_job()
                self.core.prepare_dequeue_job()
            time.sleep(5)

    def _normal_client_job_task_sim(self):
        while self.work_flag:

            cluster_name = random.sample(['test_0', 'test_1'], 1)[0]

            with self.global_lock:
                job_path, task_path, task_data = self.core.get_dequeue_job_task(cluster_name,'test')

            logging.info('job task worker, new job %s',job_path)

            if not job_path:
                time.sleep(1)
                continue

            with self.global_lock:
                self.core.update_task_state(task_path, TaskStateCode.WORKING)

            new_tasks = []

            with self.global_lock:
                status, new_task_path = self.core.add_new_task(job_path, task_data)

            print('add_new_task',status, new_task_path)
            if not status or not new_task_path:
                logging.warning('add new task error')
                continue

            new_tasks.append(Task(self.zk_client,new_task_path))

            # watch task status
            # improvement: use zookeeper watch
            while True:
                finished = True
                for t in new_tasks:
                    finished = finished and t.get_state() == TaskStateCode.FINISHED
                if finished:
                    break
                time.sleep(1)

            with self.global_lock:
                self.core.update_task_state(task_path, TaskStateCode.FINISHED)

    def _normal_client_task_sim(self):
        def add(l, r):
            return l+r

        def minus(l, r):
            return l-r

        while self.work_flag:
            cluster_name = random.sample(['test_0', 'test_1'], 1)[0]

            with self.global_lock:
                status, task_path = self.core.get_queue_task(cluster_name, 'test')

            logging.info('get task %s %s', status, task_path)

            if not status or not task_path:
                time.sleep(1)
                continue

            print('-----debug %s'% sys._getframe().f_lineno)

            with self.global_lock:
                self.core.update_task_state(task_path, TaskStateCode.WORKING)

            t = Task(self.zk_client, task_path)

            data = t.get_data()


            task_data = json.loads(data)


            lhv = int(task_data['lhv'])
            rhv = int(task_data['rhv'])
            operator = add
            if task_data['operator'] != '+':
                operator = minus


            result = operator(lhv, rhv)

            print('-------- work result: %s %s %s = %s ' %(lhv, task_data['operator'], rhv,  result) )

            self.zk_client.create(task_path+'/result', str(result).encode('utf-8'))

            with self.global_lock:
                self.core.update_task_state(task_path, TaskStateCode.FINISHED)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    s = Simulation()
    s.start_sim()