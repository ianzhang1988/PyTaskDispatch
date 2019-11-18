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
    FAILED='failed'         # set by job_task


    ERROR = 'error'         #

    TIMEOUT='timeout'

    KILL = 'kill'           # maybe put this in a separate path

class TaskTypeCode():
    UNIVERSAL='universal'

class TimeoutType():
    Task='task'
    Job='job'
