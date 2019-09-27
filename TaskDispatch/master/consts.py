# -*- coding: utf-8 -*-
# @Time    : 2019/9/17 15:14
# @Author  : ZhangYang
# @Email   : ian.zhang.88@outlook.com

class TaskStateCode():
    IDLE='idle'
    QUEUE= 'queue'
    DEQUEUE='dequeue'
    READY='ready'
    WORKING='working'       # set by job_task
    FINISHED='finished'     # set by job_task

class TaskTypeCode():
    UNIVERSAL='universal'
