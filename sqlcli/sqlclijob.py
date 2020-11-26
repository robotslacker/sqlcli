# -*- coding: utf-8 -*-
class Transaction:
    def __init__(self):
        self.max_transaction_time = 0
        self.avg_transaction_time = 0
        self.min__transaction_time = 0
        self.transaction_standard_eviation = 0


class Task:
    def __init__(self):
        self.ProcessInfo = None
        self.start_time = None
        self.end_time = None


class JOB:
    def __init__(self):
        self.id = 0

        # 进程启动的时候会利用starter的机制，即每次只启动部分进程数量，不断累积达到最高峰
        # 在进程数量已经达到最大并发数后，不再有这个限制，剩下的进程只要有空闲就会启动
        self.starter_maxprocess = 9999            # starter 最多每次启动的数量
        self.starter_interval = 0                 # starter 每次启动的时间间隔
        self.starter_last_active_time = None      # starter 最后启动脚本的时间，unix的时间戳
        self.starter_started_process = 0          # starter 已经启动的进程数
        self.parallel = 10                        # 程序并发度

        self.loop = 1                             # 需要循环的次数
        self.failed_jobs = 0                      # 已经失败的次数
        self.finished_jobs = 0                    # 已经完成的次数
        self.started_jobs = 0                     # 已经启动的次数

        self.error_message = None                 # 错误失败原因

        self.script = None
        self.script_fullname = None
        self.think_time = 0
        self.timeout = 3600  # 秒
        self.shutdown_mode = "CLOSE"
        self.fail_mode = "EXIT"
        self.submit_time = None
        self.start_time = None
        self.end_time = None
        self.blowout_threshold_percent = 100
        self.blowout_threshold_count = 9999
        self.status = "Submitted"
        self.tasks = []
        self.transactions = []

    # 返回JOB的编号信息
    def getJobID(self):
        return self.id

    # 设置JOBID
    def setJobID(self, p_nJobID):
        self.id = p_nJobID

    # 返回任务提交的时间
    def getSubmitTime(self):
        return self.submit_time

    # 设置提交时间
    def setSubmitTime(self, p_szTime):
        self.submit_time = p_szTime

    # 返回进程当前的状态
    def getStatus(self):
        return self.status

    # 设置进程当前的状态
    def setStatus(self, p_Status: str):
        self.status = p_Status

    # 返回当前正在运行的JOB数量
    def getActiveJobs(self):
        return len(self.tasks)

    # 返回当前已经失败的JOB数量
    def setFailedJobs(self, p_FailedJobs):
        self.failed_jobs = p_FailedJobs

    # 返回当前已经失败的JOB数量
    def getFailedJobs(self):
        return self.failed_jobs

    # 设置已经完成的JOB数量
    def setStartedJobs(self, p_StartedJobs):
        self.started_jobs = p_StartedJobs

    # 返回已经完成的JOB数量
    def getStartedJobs(self):
        return self.started_jobs

    # 设置已经完成的JOB数量
    def setFinishedJobs(self, p_FinishedJobs):
        self.finished_jobs = p_FinishedJobs

    # 返回已经完成的JOB数量
    def getFinishedJobs(self):
        return self.finished_jobs

    # 设置任务开始的时间
    def setStartTime(self, p_StartTime):
        self.start_time = p_StartTime

    # 返回任务开始的时间
    def getStartTime(self):
        return self.start_time

    # 设置任务结束的时间
    def setEndTime(self, p_EndTime):
        self.end_time = p_EndTime

    # 返回任务结束的时间
    def getEndTime(self):
        return self.end_time

    # 设置任务的并发程度
    def setParallel(self, p_Parallel):
        self.parallel = p_Parallel

    # 返回任务的并发程度
    def getParallel(self):
        return self.parallel

    # 设置任务的循环次数
    def setLoop(self, p_Loop):
        self.loop = p_Loop

    # 返回任务的循环次数
    def getLoop(self):
        return self.loop

    # 设置并发作业启动时每次启动间隔时间
    def setStarterInterval(self, p_StarterInterval):
        self.starter_interval = p_StarterInterval

    # 返回并发作业启动时每次启动间隔时间
    # 默认0，即不间隔
    def getStarterInterval(self):
        return self.starter_interval

    # 设置并发作业启动时每次启动进程数量
    def setStarterMaxProcess(self, p_StarterMaxProcess):
        self.starter_maxprocess = p_StarterMaxProcess

    # 返回并发作业启动时每次启动进程数量
    # 默认9999，全部进程一起启动
    def getStarterMaxProcess(self):
        return self.starter_maxprocess

    # 设置上一次Starter工作的时间
    def setStarterLastActiveTime(self, p_LastActiveTime):
        self.starter_last_active_time = p_LastActiveTime

    # 返回上一次Starter工作的时间
    def getStarterLastActiveTime(self):
        return self.starter_last_active_time

    # 设置Starter已经启动的进程数量
    def setStarterStartedProcess(self, p_StarterStartedProcess):
        self.starter_started_process = p_StarterStartedProcess

    # 返回Starter已经启动的进程数量
    def getStarterStartedProcess(self):
        return self.starter_started_process

    # 设置正在执行的脚本名称
    def setScript(self, p_Script):
        self.script = p_Script

    # 返回正在执行的脚本名称
    def getScript(self):
        return self.script

    # 设置正在执行的脚本名称
    def setErrorMessage(self, p_ErrorMessage):
        self.error_message = p_ErrorMessage

    # 返回正在执行的脚本名称
    def getErrorMessage(self):
        return self.error_message

    # 设置正在执行的脚本文件全路径
    def setScriptFullName(self, p_ScriptFullName):
        self.script_fullname = p_ScriptFullName

    # 返回正在执行的脚本文件全路径
    def getScriptFullName(self):
        return self.script_fullname

    # 设置JOB每次Task之间的考虑时间，即think_time
    def setThinkTime(self, p_ThinkTime):
        self.think_time = p_ThinkTime

    # 返回JOB每次Task之间的考虑时间，即think_time
    def getThinkTime(self):
        return self.think_time

    # 设置JOB的超时时间
    def setTimeOut(self, p_TimeOut):
        self.timeout = p_TimeOut

    # 返回JOB的超时时间
    def getTimeOut(self):
        return self.timeout

    # 设置进程关闭的方式，SHUTDOWN，ABORT
    def setShutdownMode(self, p_ShutdownMode):
        self.shutdown_mode = p_ShutdownMode

    # 返回进程关闭的方式，SHUTDOWN，ABORT
    # SHUTDOWN  当前JOB正常结束，不进行下一个循环
    # ABORT     当前JOB被强行关闭，事务终止
    def getShutdownMode(self):
        return self.shutdown_mode

    # 设置进程在发生失败时的处理方式EXIT, CONTINUE
    def setFailMode(self, p_FailMode):
        self.fail_mode = p_FailMode

    # 返回进程在发生失败时的处理方式EXIT, CONTINUE
    def getFailMode(self):
        return self.fail_mode

    # 设置Blowout失败的百分比阈值
    def setBlowoutThresHoldPrecent(self, p_BlowoutThresHoldPrecent):
        self.blowout_threshold_percent = p_BlowoutThresHoldPrecent

    # 设置Blowout失败的数量阈值
    def setBlowoutThresHoldCount(self, p_BlowoutThresHoldCount):
        self.blowout_threshold_count = p_BlowoutThresHoldCount

    # 返回Blowout失败的百分比阈值
    def getBlowoutThresHoldPrecent(self):
        return self.blowout_threshold_percent

    # 返回Blowout失败的数量阈值
    def getBlowoutThresHoldCount(self):
        return self.blowout_threshold_count

    # 返回所有JOB的任务列表
    def getTasks(self):
        return self.tasks

    # 返回所有JOB的任务列表
    def getTransactions(self):
        return self.transactions

    # 添加一个具体的任务
    def addTask(self, p_objTask):
        self.tasks.append(p_objTask)
        self.started_jobs = self.started_jobs + 1

    # 删除一个具体的任务
    def delTask(self, p_objTask):
        self.tasks.remove(p_objTask)
