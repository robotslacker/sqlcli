# -*- coding: utf-8 -*-
from .sqlclijob import JOB


class SQLCliGlobalSharedMemory(object):
    # ================================================
    # SQLCli的所有进程共享此结构对象

    # 当前的JOB流水号
    JobID = 1

    # 所有的JOB信息
    Jobs = dict()
    # ================================================

    def __init__(self):
        pass

    # 返回当前的JOBID信息，并给JobID加1
    def getJobID(self):
        self.JobID = self.JobID + 1
        return self.JobID

    # 返回所有的后台JOB信息
    def Get_Jobs(self):
        return self.Jobs

    # 返回指定的JOB信息，根据JobName来判断
    def Get_Job(self, p_szJobName):
        # 返回指定的Job信息，如果找不到，返回None
        if p_szJobName in self.Jobs.keys():
            return self.Jobs[p_szJobName]
        else:
            return None

    # 添加或更新JOB信息
    def Update_Job(self, p_JobName: str, p_Job: JOB):
        # 将JOB加入到数组中
        self.Jobs[p_JobName] = p_Job
