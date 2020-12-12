# -*- coding: utf-8 -*-
import copy
import time
import datetime


class TaskHistory:
    def __init__(self):
        self.TaskHandler_ID = 0        # 任务处理器编号，范围是0 - (parallel-1)
        self.ProcessInfo = 0           # 进程信息，这里存放的是进程PID
        self.start_time = None         # 进程开始时间，这里存放的是UnixTimeStamp
        self.end_time = None           # 进程结束时间，这里存放的是UnixTimeStamp
        self.Finished_Status = None    # 进程结束状态，有FINISHED, FAILED, ABORTED, SHUTDOWNED,
        self.exit_code = 0             # 进程退出代码


class Task:
    def __init__(self):
        self.TaskHandler_ID = 0        # 任务处理器编号，范围是1-parallel
        self.ProcessInfo = 0           # 进程信息，这里存放的是进程PID，如果没有进程存在，这里是0
        self.start_time = None         # 进程开始时间，这里存放的是UnixTimeStamp
        self.end_time = None           # 进程结束时间，这里存放的是UnixTimeStamp
        self.exit_code = 0             # 进程退出状态
        self.Finished_Status = ""      # 进程结束状态，有FINISHED, ABORTED, SHUTDOWN,


class JOB:
    def __init__(self):
        self.id = 0

        # 进程启动的时候会利用starter的机制，即每次只启动部分进程数量，不断累积达到最高峰
        # 在进程数量已经达到最大并发数后，不再有这个限制，剩下的进程只要有空闲就会启动
        self.starter_maxprocess = 9999            # starter 最多每次启动的数量
        self.starter_interval = 0                 # starter 每次启动的时间间隔
        self.starter_last_active_time = None      # starter 最后启动脚本的时间，unix的时间戳
        self.starter_started_process = 0          # starter 已经启动的进程数
        self.parallel = 1                         # 程序并发度

        self.loop = 1                             # 需要循环的次数
        self.failed_jobs = 0                      # 已经失败的次数
        self.finished_jobs = 0                    # 已经完成的次数
        self.started_jobs = 0                     # 已经启动的次数
        self.active_jobs = 0                      # 当前活动的JOB数量

        self.error_message = None                 # 错误失败原因

        self.script = None
        self.script_fullname = None
        self.think_time = 0
        self.timeout = 0                          # 超时的秒数限制，0表示不限制
        self.submit_time = None
        self.start_time = None
        self.end_time = None
        self.blowout_threshold_count = 0          # 完全失败阈值，若达到该阈值，认为后续继续测试没有意义。0表示不做限制
        self.status = "Submitted"
        self.tasks = []                           # 当前任务的具体进程信息
        self.taskhistory = []                     # 当前任务的进程信息备份
        self.transactionstaistics = []            # 当前JOB的transaction统计信息
        self.transactionhistory = []              # 当前JOB的transaction的历史信息

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

    # 设置当前正在运行的JOB数量
    def setActiveJobs(self, p_active_jobs: int):
        self.active_jobs = p_active_jobs

    # 返回当前正在运行的JOB数量
    def getActiveJobs(self):
        return self.active_jobs

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
    def setStartTime(self, p_Start_Time):
        self.start_time = p_Start_Time

    # 返回任务开始的时间
    def getStartTime(self):
        return self.start_time

    # 设置任务结束的时间
    def setEndTime(self, p_End_Time):
        self.end_time = p_End_Time

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

    # 设置Blowout失败的数量阈值
    def setBlowoutThresHoldCount(self, p_BlowoutThresHoldCount):
        self.blowout_threshold_count = p_BlowoutThresHoldCount

    # 返回Blowout失败的数量阈值
    def getBlowoutThresHoldCount(self):
        return self.blowout_threshold_count

    # 返回所有JOB的任务列表
    def getTasks(self):
        return self.tasks

    # 初始化Task列表
    def initTaskList(self):
        # 根据并发度在start后直接创建全部的Task列表
        for nPos in range(0, self.parallel):
            m_Task = Task()
            m_Task.TaskHandler_ID = nPos
            m_Task.ProcessInfo = 0
            self.tasks.append(copy.copy(m_Task))

    # 启动作业
    # 返回一个包含TaskHandlerID的列表
    def getTaskStarter(self):
        currenttime = int(time.mktime(datetime.datetime.now().timetuple()))
        m_IDLEHandlerIDList = []
        if self.active_jobs >= self.parallel:
            # 如果当前活动进程数量已经超过了并发要求，直接退出
            return m_IDLEHandlerIDList
        if self.started_jobs >= self.loop:
            # 如果到目前位置，已经启动的进程数量超过了总数要求，直接退出
            return m_IDLEHandlerIDList
        if self.starter_interval != 0 and self.started_jobs < self.parallel:
            # 如果上一个批次启动时间到现在还不到限制时间要求，则不再启动
            if self.starter_last_active_time + self.starter_interval > currenttime:
                # 还不到可以启动进程的时间, 返回空列表
                return m_IDLEHandlerIDList
        # 循环判断每个JOB信息，考虑think_time以及starter_interval
        for nPos in range(0, self.parallel):
            if self.tasks[nPos].ProcessInfo == 0:  # 进程当前空闲
                if self.think_time != 0:
                    # 需要考虑think_time
                    if self.tasks[nPos].end_time + self.think_time > currenttime:
                        # 还不满足ThinkTime的限制要求，跳过
                        continue
                else:
                    # 不需要考虑think_time, 假设可以启动
                    m_IDLEHandlerIDList.append(nPos)
                    if len(m_IDLEHandlerIDList) >= (self.parallel - self.active_jobs):
                        # 如果已经要启动的进程已经满足最大进程数限制，则不再考虑新进程
                        break
                    if self.starter_interval != 0 and len(m_IDLEHandlerIDList) >= self.starter_maxprocess:
                        # 如果已经达到了每个批次能够启动进程的最多限制，则不再启动
                        break
            else:
                # 进程当前不空闲
                continue
        # 更新批次启动时间的时间
        if self.starter_interval != 0:
            self.starter_last_active_time = currenttime
        return m_IDLEHandlerIDList

    # 启动作业任务
    def StartTask(self, p_TaskHandlerID: int, p_ProcessID: int):
        self.tasks[p_TaskHandlerID].ProcessInfo = p_ProcessID
        self.tasks[p_TaskHandlerID].start_time = int(time.mktime(datetime.datetime.now().timetuple()))
        self.tasks[p_TaskHandlerID].end_time = None
        self.active_jobs = self.active_jobs + 1
        self.started_jobs = self.started_jobs + 1

    # 备份任务
    def FinishTask(self, p_TaskHandler_ID, p_Task_ExitCode: int, p_Task_FinishedStatus: str):
        # 备份Task信息
        m_TaskHistory = TaskHistory()
        m_TaskHistory.TaskHandler_ID = p_TaskHandler_ID
        m_TaskHistory.start_time = self.tasks[p_TaskHandler_ID].start_time
        m_TaskHistory.end_time = int(time.mktime(datetime.datetime.now().timetuple()))
        m_TaskHistory.exit_code = self.tasks[p_TaskHandler_ID].exit_code
        m_TaskHistory.ProcessInfo = self.tasks[p_TaskHandler_ID].ProcessInfo
        if p_Task_FinishedStatus == "TIMEOUT":
            m_TaskHistory.Finished_Status = "TIMEOUT"
            # 进程被强行终止
            self.failed_jobs = self.failed_jobs + 1
            self.finished_jobs = self.finished_jobs + 1
        elif p_Task_FinishedStatus == "ABORTED":
            m_TaskHistory.Finished_Status = "ABORTED"
            # 进程被强行终止
            self.failed_jobs = self.failed_jobs + 1
            self.finished_jobs = self.finished_jobs + 1
        elif p_Task_FinishedStatus == "SHUTDOWNED":
            m_TaskHistory.Finished_Status = "SHUTDOWNED"
            # 进程正常结束
            self.finished_jobs = self.finished_jobs + 1
        elif p_Task_ExitCode != 0:
            # 任务不正常退出
            m_TaskHistory.Finished_Status = "FAILED"
            self.failed_jobs = self.failed_jobs + 1
            self.finished_jobs = self.finished_jobs + 1
        else:
            m_TaskHistory.Finished_Status = "FINISHED"
            self.finished_jobs = self.finished_jobs + 1
        self.taskhistory.append(copy.copy(m_TaskHistory))

        # 清空当前Task, 这里不会清空End_Time，以保持Think_time的判断逻辑
        self.tasks[p_TaskHandler_ID].start_time = None
        self.tasks[p_TaskHandler_ID].exit_code = 0
        self.tasks[p_TaskHandler_ID].ProcessInfo = 0
        self.tasks[p_TaskHandler_ID].Finished_Status = ""
        self.active_jobs = self.active_jobs - 1
