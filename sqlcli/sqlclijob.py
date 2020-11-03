# -*- coding: utf-8 -*-
from .sqlcliexception import SQLCliException

class JOB:
    def __init__(self):
        self.id = 0
        self.parallel = 10
        self.starter_maxprocess = 2
        self.loop = 100
        self.script = None
        self.think_time = 0
        self.timeout = 3600 # 秒
        self.shutdown_mode = "CLOSE"
        self.fail_mode = "EXIT"
        self.submit_time = None
        self.blowout_threshold_percent = 100
        self.blowout_threshold_count = 9999

    # 设置JOBID
    def setJobID(self, p_nJobID):
        self.id = p_nJobID

    # 设置提交时间
    def setSubmitTime(self, p_szTime):
        self.submit_time = p_szTime

    def start_job(self):
        # TODO
        pass

    def close_job(self):
        # TODO
        pass

    def shutdown_job(self):
        # TODO
        pass

    def abort_job(self):
        # TODO
        pass


