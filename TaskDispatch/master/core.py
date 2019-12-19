# -*- coding: utf-8 -*-
# @Time    : 2019/9/17 18:01
# @Author  : ZhangYang
# @Email   : ian.zhang.88@outlook.com

from .utility import create_new_sequence_node
from .job import Job
from .task import Task
from .consts import TaskStateCode
from .cluster import Cluster
from .timeout import TimeoutManager

class Core():
    def __init__(self, zk_client, base_path='/TaskDispatch'):
        self.zk_client = zk_client

        self.base_path=base_path
        self.jobs_path = self.base_path + '/jobs'
        self.workers_path = self.base_path + '/workers'
        self.masters_path = self.base_path + '/masters'
        self.cluster_path = self.base_path + '/clusters'
        self.timeout_path = self.base_path + '/timeout'

        self._cluster_job = {}
        self._all_job={}

        self._cluster_job_upper_threshold=50
        self._cluster_job_lower_threshold=20

        self.cluster = Cluster(self.zk_client, self.cluster_path)

    def make_sure_tree_structure(self):
        self.zk_client.ensure_path(self.base_path)
        self.zk_client.ensure_path(self.jobs_path)
        self.zk_client.ensure_path(self.workers_path)
        self.zk_client.ensure_path(self.masters_path)
        self.zk_client.ensure_path(self.timeout_path)

    def add_cluster(self, name):
        self.cluster.add(name)
        self._cluster_job[name] = []

    def delete_cluster(self, name):
        if not self._cluster_job[name]:
            self.cluster.delete(name)
            return True
        return False

    def add_new_job(self, data):
        new_node_path = create_new_sequence_node(self.zk_client, self.jobs_path, 'job')
        print('new_node_path:', new_node_path)
        j = Job(self.zk_client, new_node_path)
        ret = j.parse(data)
        j.set_state(TaskStateCode.QUEUE)
        # self._all_job[new_node_path]=j
        return ret, new_node_path


    def prepare_dequeue_job(self):
        jobs_path = self.zk_client.get_children(self.jobs_path)
        jobs = [Job(self.zk_client, self.jobs_path+'/'+path) for path in jobs_path]
        jobs = [ j for j in jobs if j.get_state() == TaskStateCode.QUEUE]

        clusters = self.cluster.get_all()

        for c in clusters:
            size = len(self._cluster_job[c])
            if size > self._cluster_job_lower_threshold:
                continue

            jobs_this_cluster = [j for j in jobs if j.get_cluster() == c]
            jobs_this_cluster_sorted = sorted(jobs_this_cluster, key=lambda x: x.get_priority(), reverse=True )

            print('jobs_this_cluster_sorted', list([j.get_priority() for j in jobs_this_cluster_sorted]))

            jobs_needed = jobs_this_cluster_sorted[:self._cluster_job_upper_threshold - size]
            for j in jobs_needed:
                j.set_state(TaskStateCode.DEQUEUE)
            self._cluster_job[c].extend(jobs_needed)
            self._cluster_job[c] = sorted(self._cluster_job[c], key=lambda x: x.get_priority(), reverse=True )

            self._all_job.update({ j.job_path():j for j in jobs_needed})

    def get_dequeue_job_task(self, cluster_name, task_type):
        jobs = self._cluster_job[cluster_name]

        job_path = None
        task_path = None
        task_data = None

        for j in jobs:
            if j.get_type() != task_type:
                continue
            if j.get_state() == TaskStateCode.DEQUEUE:
                job_path = j.job_path()
                task_path = j.task_path()
                task_data = j.get_data()
                j.set_state(TaskStateCode.READY)
                break

        return job_path, task_path, task_data

    def add_new_task(self, job_path, task_data):

        if job_path not in self._all_job:
            return False, 'job not exists'

        j = self._all_job[job_path]

        new_task_path = create_new_sequence_node(self.zk_client, j.task_base_path, 'task')

        t = Task(self.zk_client, new_task_path)
        t.set_data(task_data)
        t.set_state(TaskStateCode.QUEUE)

        return True, new_task_path

    # alternative: prepare in main loop, save in mem, send here, instead of search every time
    def get_queue_task(self, cluster_name, task_type):

        if cluster_name not in self._cluster_job:
            return False, 'cluster not exists'

        dequeue_task_path = None

        # jobs_in_cluster sorted in priority, so this cover priority
        jobs_in_cluster = self._cluster_job[cluster_name]

        for job in jobs_in_cluster:
            if job.get_type() != task_type:
                continue

            if job.get_state() != TaskStateCode.WORKING:
                continue

            tasks = job.get_tasks_path()

            if not tasks:
                continue

            for t_path in tasks:
                t = Task(self.zk_client, t_path)
                if t.get_state() != TaskStateCode.QUEUE:
                    continue

                dequeue_task_path = t_path
                t.set_state( TaskStateCode.READY )

                return True, dequeue_task_path

        return False, 'no task found'

    def update_task_state(self, task_path, state):
        if not self.zk_client.exists(task_path):
            return False

        t = Task(self.zk_client, task_path)
        t.set_state(state)

        # todo: update time

        return True

    def clean_finished_job(self):
        clusters = self.cluster.get_all()

        for c in clusters:
            finished_job = []
            finished_job_paths = []

            cluster_jobs = self._cluster_job[c]

            print('-------- clean_finished_job', len(cluster_jobs))

            for j in cluster_jobs:
                if j.get_state() == TaskStateCode.FINISHED:
                    finished_job.append(j)
                    finished_job_paths.append(j.job_path())

            for p in finished_job_paths:
                del self._all_job[p]

            for j in finished_job:
                self._cluster_job[c].remove(j)
                j.delete()

        # clean lost job if any (like when master crash, will not reenter _cluster_job)

    def check_abnormal(self):

        for k in self._all_job:
            job = self._all_job[k]

            if not job.check_worker():
                job.set_state(TaskStateCode.QUEUE)

                tasks_path = job.get_tasks_path()

                for p in tasks_path:
                    t = Task(self.zk_client, p)
                    t.set_state(TaskStateCode.KILL)
                continue

            tasks_path = job.get_tasks_path()

            for p in tasks_path:
                t = Task(self.zk_client, p)
                if t.check_worker():
                    continue
                else:
                    t.set_state(TaskStateCode.QUEUE)

