# -*- coding: utf-8 -*-
# @Time    : 2019/12/19 17:03
# @Author  : ZhangYang
# @Email   : ian.zhang.88@outlook.com

# check if state change valid

from .consts import TaskStateCode


IDLE = TaskStateCode.IDLE
QUEUE = TaskStateCode.QUEUE
DEQUEUE = TaskStateCode.DEQUEUE
READY = TaskStateCode.READY
WORKING = TaskStateCode.WORKING
FINISHED = TaskStateCode.FINISHED
FAILED = TaskStateCode.FAILED
TIMEOUT = TaskStateCode.TIMEOUT
KILL = TaskStateCode.KILL


class StateGraph():
    graph={
        IDLE:[QUEUE,],
        QUEUE:[DEQUEUE,],
        DEQUEUE:[READY,],
        READY:[WORKING, KILL,TIMEOUT],
        WORKING:[FINISHED, KILL,TIMEOUT],
        FINISHED:[],
        FAILED:[KILL,DEQUEUE],
        TIMEOUT:[KILL,DEQUEUE],
        KILL:[],
    }

    def __init__(self):
        pass

    def check_next(self, current, next):
        if current not in StateGraph.graph:
            raise Exception('%s not in state graph' % current)

        return next in StateGraph.graph[current]