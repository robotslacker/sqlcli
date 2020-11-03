# -*- coding: utf-8 -*-
import copy
import time

from .sqlclijob import JOB

class SQLCli_GlobalSharedMemory(object):
    # ================================================
    # SQLCli的所有进程共享此结构对象

    # 当前的JOB流水号
    JobID = 1

    # 所有的JOB信息
    Jobs = dict()
    # ================================================

    def __init__(self):
        pass

    # 返回所有的后台JOB信息
    def Get_Jobs(self):
        return self.Jobs

    # 返回指定的JOB信息，根据JobName来判断
    def Get_Job(self, p_szJObName):        
        # 返回指定的Job信息，如果找不到，返回None
        if self.Jobs.has_key(p_szJObName):
            return self.Jobs[p_szJObName]
        else:
            return None

    # 添加或更新JOB信息
    def Update_Job(self, p_JobName: str, p_Job: JOB):
        # 复制一个新的JOB对象
        m_Job = copy.copy(p_Job)

        # 将JOB加入到数组中
        self.Jobs[p_JobName] = m_Job

