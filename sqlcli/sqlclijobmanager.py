# -*- coding: utf-8 -*-
import re
import threading
import time
import os
import datetime
import copy
from multiprocessing import Process

from .sqlcliexception import SQLCliException


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
        self.job_name = ""

        # 进程启动的时候会利用starter的机制，即每次只启动部分进程数量，不断累积达到最高峰
        # 在进程数量已经达到最大并发数后，不再有这个限制，剩下的进程只要有空闲就会启动
        self.starter_interval = 0                 # starter 每次启动的时间间隔
        self.starter_last_active_time = None      # starter 最后启动脚本的时间，unix的时间戳
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
        self.transactionstaistics = []            # 当前JOB的transaction统计信息
        self.transactionhistory = []              # 当前JOB的transaction的历史信息

    # 返回JOB的编号信息
    def getJobID(self):
        return self.id

    # 设置JOBID
    def setJobID(self, p_nJobID):
        self.id = p_nJobID

    # 返回JOB的编号信息
    def getJobName(self):
        return self.job_name

    # 设置JOBID
    def setJobName(self, p_szJobName):
        self.job_name = p_szJobName

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

    # 设置上一次Starter工作的时间
    def setStarterLastActiveTime(self, p_LastActiveTime):
        self.starter_last_active_time = p_LastActiveTime

    # 返回上一次Starter工作的时间
    def getStarterLastActiveTime(self):
        return self.starter_last_active_time

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

    # 根据参数设置JOB的相关参数
    def setjob(self, p_ParameterName: str, p_ParameterValue: str):
        if p_ParameterName.strip().lower() == "parallel":
            self.setParallel(int(p_ParameterValue))
        elif p_ParameterName.strip().lower() == "starter_interval":
            self.setStarterInterval(int(p_ParameterValue))
        elif p_ParameterName.strip().lower() == "loop":
            self.setLoop(int(p_ParameterValue))
        elif p_ParameterName.strip().lower() == "script":
            self.setScript(p_ParameterValue)
        elif p_ParameterName.strip().lower() == "think_time":
            self.setThinkTime(int(p_ParameterValue))
        elif p_ParameterName.strip().lower() == "timeout":
            self.setTimeOut(int(p_ParameterValue))
        elif p_ParameterName.strip().lower() == "blowout_threshold_count":
            self.setBlowoutThresHoldCount(int(p_ParameterValue))
        else:
            raise SQLCliException("Invalid JOB Parameter name. [" + str(p_ParameterName) + "]")

    # 返回所有JOB的任务列表
    def getTasks(self):
        return self.tasks

    # 设置Task信息
    def setTask(self, p_Task: Task):
        m_TaskFound = False
        for m_nPos in range(0, len(self.tasks)):
            if self.tasks[m_nPos].TaskHandler_ID == p_Task.TaskHandler_ID:
                m_TaskFound = True
                self.tasks[m_nPos].start_time = p_Task.start_time
                self.tasks[m_nPos].end_time = p_Task.end_time
                self.tasks[m_nPos].exit_code = p_Task.exit_code
                self.tasks[m_nPos].ProcessInfo = p_Task.ProcessInfo
                self.tasks[m_nPos].Finished_Status = p_Task.Finished_Status
                break
        if not m_TaskFound:
            m_Task = Task()
            m_Task.TaskHandler_ID = p_Task.TaskHandler_ID
            m_Task.start_time = p_Task.start_time
            m_Task.end_time = p_Task.end_time
            m_Task.exit_code = p_Task.exit_code
            m_Task.ProcessInfo = p_Task.ProcessInfo
            m_Task.Finished_Status = p_Task.Finished_Status
            self.tasks.append(copy.copy(m_Task))

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

    # 完成当前任务
    def FinishTask(self, p_TaskHandler_ID, p_Task_ExitCode: int, p_Task_FinishedStatus: str):
        # 备份Task信息
        if p_Task_FinishedStatus == "TIMEOUT":
            # 进程被强行终止
            self.failed_jobs = self.failed_jobs + 1
            self.finished_jobs = self.finished_jobs + 1
        elif p_Task_FinishedStatus == "ABORTED":
            # 进程被强行终止
            self.failed_jobs = self.failed_jobs + 1
            self.finished_jobs = self.finished_jobs + 1
        elif p_Task_FinishedStatus == "SHUTDOWNED":
            # 进程正常结束
            self.finished_jobs = self.finished_jobs + 1
        elif p_Task_ExitCode != 0:
            # 任务不正常退出
            self.failed_jobs = self.failed_jobs + 1
            self.finished_jobs = self.finished_jobs + 1
        else:
            self.finished_jobs = self.finished_jobs + 1

        # 清空当前Task, 这里不会清空End_Time，以保持Think_time的判断逻辑
        self.tasks[p_TaskHandler_ID].start_time = None
        self.tasks[p_TaskHandler_ID].exit_code = 0
        self.tasks[p_TaskHandler_ID].ProcessInfo = 0
        self.tasks[p_TaskHandler_ID].Finished_Status = ""
        self.active_jobs = self.active_jobs - 1


# JOB任务的管理
class JOBManager(object):
    def __init__(self):
        # 进程管理的相关上下文信息
        # 来自于父进程，当子进程不进行特殊设置的时候，进程管理中用到的信息将集成父进程
        self.ProcessContextInfo = {}

        # 进程句柄信息
        self.ProcessInfo = {}

        # 当前清理进程的状态, NOT-STARTED, RUNNING, WAITINGFOR_STOP, STOPPED
        # 如果WorkerStatus==WAITINGFOR_STOP 则Worker主动停止
        self.WorkerStatus = "NOT-STARTED"

        # 当前的JOB流水号
        self.JobID = 1

        # 记录当前Agent启动状态
        self.isAgentStarted = False

        # 记录Meta的数据库连接信息
        self.MetaConn = None

    # 设置Meta的连接信息
    def setMetaConn(self, p_conn):
        self.MetaConn = p_conn

    # 设置当前后台代理进程的状态
    def setWorkerStatus(self, p_WorkerStatus):
        self.WorkerStatus = p_WorkerStatus

    # 获得当前清理进程的状态
    def getWorkerStatus(self):
        return self.WorkerStatus

    # 根据JobID返回指定的JOB信息
    def getJobByID(self, p_szJobID):
        # 返回指定的Job信息，如果找不到，返回None
        m_SQL = "SELECT Job_ID,Job_Name,Starter_Interval, " \
                    "Starter_Last_Active_Time, Parallel, Loop, " \
                    "Started_JOBS, Failed_JOBS,Finished_JOBS,Active_JOBS,Error_Message, " \
                    "Script, Script_FullName, Think_Time,  Timeout, Submit_Time, Start_Time, End_Time, " \
                    "Blowout_Threshold_Count, Status FROM SQLCLI_JOBS WHERE Job_ID = " + str(p_szJobID)
        m_db_cursor = self.MetaConn.cursor()
        m_db_cursor.execute(m_SQL)
        m_rs = m_db_cursor.fetchone()
        if m_rs is None:
            m_db_cursor.close()
            return None
        else:
            m_Job = JOB()
            m_Job.setJobID(m_rs[0])
            m_Job.setJobName(m_rs[1])
            m_Job.setStarterInterval(m_rs[2])
            m_Job.setStarterLastActiveTime(m_rs[3])
            m_Job.setParallel(m_rs[4])
            m_Job.setLoop(m_rs[5])
            m_Job.setStartedJobs(m_rs[6])
            m_Job.setFailedJobs(m_rs[7])
            m_Job.setFinishedJobs(m_rs[8])
            m_Job.setActiveJobs(m_rs[9])
            m_Job.setErrorMessage(m_rs[10])
            m_Job.setScript(m_rs[11])
            m_Job.setScriptFullName(m_rs[12])
            m_Job.setThinkTime(m_rs[13])
            m_Job.setTimeOut(m_rs[14])
            m_Job.setSubmitTime(m_rs[15])
            m_Job.setStartTime(m_rs[16])
            m_Job.setEndTime(m_rs[17])
            m_Job.setBlowoutThresHoldCount(m_rs[18])
            m_Job.setStatus(m_rs[19])
            m_db_cursor.close()

            # 获取Task信息
            m_SQL = "SELECT TaskHandler_ID,ProcessInfo,start_time,end_time,exit_code, Finished_Status "  \
                    "FROM   SQLCLI_TASKS WHERE JOB_ID=" + str(m_Job.getJobID())
            m_db_cursor = self.MetaConn.cursor()
            m_db_cursor.execute(m_SQL)
            m_rs = m_db_cursor.fetchall()
            if m_rs is not None:
                for m_row in m_rs:
                    m_Task = Task()
                    m_Task.TaskHandler_ID = m_row[0]
                    m_Task.ProcessInfo = m_row[1]
                    m_Task.start_time = m_row[2]
                    m_Task.end_time = m_row[3]
                    m_Task.exit_code = m_row[4]
                    m_Task.Finished_Status = m_row[5]
                    m_Job.setTask(m_Task)
            m_db_cursor.close()
            return m_Job

    # 根据JobName返回指定的JOB信息
    def getJobByName(self, p_szJobName):
        # 返回指定的Job信息，如果找不到，返回None
        m_SQL = "SELECT Job_ID,Job_Name,Starter_Interval, " \
                    "Starter_Last_Active_Time, Parallel, Loop, Failed_JOBS,Finished_JOBS,Active_JOBS,Error_Message, " \
                    "Script, Script_FullName, Think_Time,  Timeout, Submit_Time, Start_Time, End_Time, " \
                    "Blowout_Threshold_Count, Status FROM SQLCLI_JOBS WHERE JOB_NAME = '" + str(p_szJobName) + "'"
        m_db_cursor = self.MetaConn.cursor()
        m_db_cursor.execute(m_SQL)
        m_rs = m_db_cursor.fetchone()
        if m_rs is None:
            m_db_cursor.close()
            return None
        else:
            m_Job = JOB()
            m_Job.setJobID(m_rs[0])
            m_Job.setJobName(m_rs[1])
            m_Job.setStarterInterval(m_rs[2])
            m_Job.setStarterLastActiveTime(m_rs[3])
            m_Job.setParallel(m_rs[4])
            m_Job.setLoop(m_rs[5])
            m_Job.setFailedJobs(m_rs[6])
            m_Job.setFinishedJobs(m_rs[7])
            m_Job.setActiveJobs(m_rs[8])
            m_Job.setErrorMessage(m_rs[9])
            m_Job.setScript(m_rs[10])
            m_Job.setScriptFullName(m_rs[11])
            m_Job.setThinkTime(m_rs[12])
            m_Job.setTimeOut(m_rs[13])
            m_Job.setSubmitTime(m_rs[14])
            m_Job.setStartTime(m_rs[15])
            m_Job.setEndTime(m_rs[16])
            m_Job.setBlowoutThresHoldCount(m_rs[17])
            m_Job.setStatus(m_rs[18])
            m_db_cursor.close()

            # 获取Task信息
            m_SQL = "SELECT TaskHandler_ID,ProcessInfo,start_time,end_time,exit_code, Finished_Status "  \
                    "FROM   SQLCLI_TASKS WHERE JOB_ID=" + str(m_Job.getJobID())
            m_db_cursor = self.MetaConn.cursor()
            m_db_cursor.execute(m_SQL)
            m_rs = m_db_cursor.fetchall()
            if m_rs is not None:
                for m_row in m_rs:
                    m_Task = Task()
                    m_Task.TaskHandler_ID = m_row[0]
                    m_Task.ProcessInfo = m_row[1]
                    m_Task.start_time = m_row[2]
                    m_Task.end_time = m_row[3]
                    m_Task.exit_code = m_row[4]
                    m_Task.Finished_Status = m_row[5]
                    m_Job.setTask(m_Task)
            m_db_cursor.close()
            return m_Job

    # 根据JobName返回指定的JOB信息
    def getAllJobs(self):
        # 返回指定的Job信息，如果找不到，返回None
        m_SQL = "SELECT Job_ID FROM SQLCLI_JOBS ORDER BY Job_ID"
        m_db_cursor = self.MetaConn.cursor()
        m_db_cursor.execute(m_SQL)
        m_rs = m_db_cursor.fetchall()
        if m_rs is None:
            m_db_cursor.close()
            return []
        else:
            m_JOBList = []
            for m_row in m_rs:
                m_Job = self.getJobByID(m_row[0])
                m_JOBList.append(m_Job)
            m_db_cursor.close()
            return m_JOBList

    @staticmethod
    def runSQLCli(p_args):
        from .main import SQLCli

        # 运行子进程的时候，不需要启动JOBManager
        m_SQLCli = SQLCli(
            sqlscript=p_args["sqlscript"],
            logon=p_args["logon"],
            logfilename=p_args["logfilename"],
            sqlmap=p_args["sqlmap"],
            nologo=p_args["nologo"],
            HeadlessMode=True,
            WorkerName=p_args["workername"],
            sqlperf=p_args["sqlperf"],
            EnableJobManager=False,
            JOBManagerURL=p_args["JOBManagerURL"]
        )
        m_SQLCli.run_cli()

    # 后台守护线程，跟踪进程信息，启动或强制关闭进程
    def JOBManagerAgent(self):
        if self.getWorkerStatus() == "NOT-STARTED":
            self.setWorkerStatus("RUNNING")
        while True:
            # 如果程序退出，则关闭该Agent线程
            if self.getWorkerStatus() == "WAITINGFOR_STOP":
                self.setWorkerStatus("STOPPED")
                break
            # 循环处理工作JOB
            for m_Job in self.getAllJobs():
                if m_Job.getStatus() in ("FAILED", "SHUTDOWNED", "FINISHED", "ABORTED"):
                    # 已经失败的Case不再处理
                    continue
                if m_Job.getStatus() in ("RUNNING", "WAITINGFOR_SHUTDOWN", "WAITINGFOR_ABORT"):
                    # 依次检查Task的状态
                    # 即使已经处于WAITINGFOR_SHUTDOWN或者WAITINGFOR_ABORT中，也不排除还有没有完成的作业
                    m_TaskList = m_Job.getTasks()
                    currenttime = int(time.mktime(datetime.datetime.now().timetuple()))
                    bAllTaskFinished = True
                    for m_Task in m_TaskList:
                        m_ProcessID = m_Task.ProcessInfo
                        if m_ProcessID != 0:
                            m_Process = self.ProcessInfo[m_ProcessID]
                            if not m_Process.is_alive():
                                # 进程ID不是0，进程已经不存在，或者是正常完成，或者是异常退出
                                m_Job.FinishTask(m_Task.TaskHandler_ID, m_Process.exitcode, "")
                                self.SaveJob(m_Job)
                                # 从当前保存的进程信息中释放该进程
                                self.ProcessInfo.pop(m_ProcessID)
                            else:
                                # 进程还在运行中
                                if m_Job.getTimeOut() != 0:
                                    # 设置了超时时间，我们需要根据超时时间进行判断
                                    if m_Task.start_time + m_Job.getTimeOut() < currenttime:
                                        m_Process.terminate()
                                        m_Job.FinishTask(m_Task.TaskHandler_ID, m_Process.exitcode, "TIMEOUT")
                                        self.SaveJob(m_Job)
                                        # 从当前保存的进程信息中释放该进程
                                        self.ProcessInfo.pop(m_ProcessID)
                                    else:
                                        if m_Job.getStatus() == "WAITINGFOR_ABORT":
                                            m_Process.terminate()
                                            m_Job.FinishTask(m_Task.TaskHandler_ID, m_Process.exitcode, "ABORTED")
                                            self.SaveJob(m_Job)
                                            # 从当前保存的进程信息中释放该进程
                                            self.ProcessInfo.pop(m_ProcessID)
                                        bAllTaskFinished = False
                                else:
                                    if m_Job.getStatus() == "WAITINGFOR_ABORT":
                                        m_Process.terminate()
                                        m_Job.FinishTask(m_Task.TaskHandler_ID, m_Process.exitcode, "ABORTED")
                                        self.SaveJob(m_Job)
                                        # 从当前保存的进程信息中释放该进程
                                        self.ProcessInfo.pop(m_ProcessID)
                                    bAllTaskFinished = False
                                continue
                    if bAllTaskFinished and m_Job.getStatus() == "WAITINGFOR_SHUTDOWN":
                        # 检查脚本信息，如果脚本压根不存在，则无法后续的操作
                        m_Job.setStatus("SHUTDOWNED")
                        m_Job.setErrorMessage("JOB has been shutdown successful.")
                        self.SaveJob(m_Job)
                        continue
                    if m_Job.getStatus() == "WAITINGFOR_ABORT":
                        # 检查脚本信息，如果脚本压根不存在，则无法后续的操作
                        m_Job.setStatus("ABORTED")
                        m_Job.setErrorMessage("JOB has been aborted.")
                        self.SaveJob(m_Job)
                        continue
                    if m_Job.getBlowoutThresHoldCount() != 0:
                        if m_Job.getFailedJobs() >= m_Job.getBlowoutThresHoldCount():
                            if bAllTaskFinished:
                                # 已经失败的脚本实在太多，不能再继续
                                m_Job.setStatus("FAILED")
                                m_Job.setErrorMessage("JOB blowout, terminate.")
                                self.SaveJob(m_Job)
                            continue
                    if m_Job.getScript() is None:
                        # 检查脚本信息，如果脚本压根不存在，则无法后续的操作
                        m_Job.setStatus("FAILED")
                        m_Job.setErrorMessage("Script parameter is null.")
                        self.SaveJob(m_Job)
                        continue
                    if m_Job.getScriptFullName() is None:
                        # 如果脚本没有补充完全的脚本名称，则此刻进行补充
                        # 命令里头的是全路径名，或者是基于当前目录的相对文件名
                        m_Script_FileName = m_Job.getScript()
                        if os.path.isfile(m_Script_FileName):
                            m_SQL_ScriptBaseName = os.path.basename(m_Script_FileName)
                            m_SQL_ScriptFullName = os.path.abspath(m_Script_FileName)
                        else:
                            if self.getProcessContextInfo("sqlscript") is not None:
                                m_SQL_ScriptHomeDirectory = os.path.dirname(self.getProcessContextInfo("sqlscript"))
                                if os.path.isfile(os.path.join(m_SQL_ScriptHomeDirectory, m_Script_FileName)):
                                    m_SQL_ScriptBaseName = \
                                        os.path.basename(os.path.join(m_SQL_ScriptHomeDirectory, m_Script_FileName))
                                    m_SQL_ScriptFullName = \
                                        os.path.abspath(os.path.join(m_SQL_ScriptHomeDirectory, m_Script_FileName))
                                else:
                                    m_Job.setStatus("FAILED")
                                    m_Job.setErrorMessage("Script [" + m_Script_FileName + "] does not exist.")
                                    self.SaveJob(m_Job)
                                    continue
                            else:
                                m_Job.setStatus("FAILED")
                                m_Job.setErrorMessage("Script [" + m_Script_FileName + "] does not exist.")
                                self.SaveJob(m_Job)
                                continue
                        m_Job.setScript(m_SQL_ScriptBaseName)
                        m_Job.setScriptFullName(m_SQL_ScriptFullName)
                        self.SaveJob(m_Job)
                    if m_Job.getFinishedJobs() >= m_Job.getLoop():
                        # 已经完成了全部的作业，标记为完成状态
                        m_Job.setStatus("FINISHED")
                        m_Job.setEndTime(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())))
                        # 将任务添加到后台进程信息中
                        self.SaveJob(m_Job)
                        continue
                    # 开始陆续启动需要完成的任务
                    # 获得可以启动的任务进程列表
                    m_TaskStarterList = m_Job.getTaskStarter()
                    # 给每一个进程提供唯一的日志文件名
                    m_JOB_Sequence = m_Job.getStartedJobs()
                    for m_TaskStarter in m_TaskStarterList:
                        # 循环启动所有的进程
                        m_args = {"logon": self.getProcessContextInfo("logon"),
                                  "nologo": self.getProcessContextInfo("nologo"),
                                  "sqlperf": self.getProcessContextInfo("sqlperf"),
                                  "sqlmap": self.getProcessContextInfo("sqlmap"),
                                  "sqlscript": m_Job.getScriptFullName(),
                                  "JOBManagerURL": self.getProcessContextInfo("JOBManagerURL"),
                                  "workername":
                                      m_Job.getJobName() + "#" + str(m_TaskStarter) + "-" +
                                      str(m_JOB_Sequence+1)
                                  }
                        if self.getProcessContextInfo("logfilename") is not None:
                            m_logfilename = os.path.join(
                                os.path.dirname(self.getProcessContextInfo("logfilename")),
                                m_Job.getScript().split('.')[0] + "_" + str(m_Job.getJobID()) +
                                "-" + str(m_JOB_Sequence+1) + ".log")
                        else:
                            m_logfilename = \
                                m_Job.getScript().split('.')[0] + "_" + \
                                str(m_Job.getJobID()) + "-" + \
                                str(m_JOB_Sequence+1) + ".log"
                        m_args["logfilename"] = m_logfilename
                        m_Process = Process(target=self.runSQLCli,
                                            args=(m_args,))
                        m_Process.start()
                        # 将Process信息放入到JOB列表中，启动进程
                        m_Job.StartTask(m_TaskStarter, m_Process.pid)
                        m_Job.setStartedJobs(m_JOB_Sequence+1)
                        m_JOB_Sequence = m_JOB_Sequence + 1
                        self.SaveJob(m_Job)
                        self.ProcessInfo[m_Process.pid] = m_Process
            # 每2秒检查一次任务
            time.sleep(2)

    # 启动Agent进程
    def register(self):
        # 启动后台守护线程，用来处理延时启动，超时等问题
        Agenthread = threading.Thread(target=self.JOBManagerAgent)
        Agenthread.setDaemon(True)  # 主进程退出，守护进程也会退出
        Agenthread.setName("JobManagerAgent")
        Agenthread.start()
        self.isAgentStarted = True

    # 退出Agent进程
    def unregister(self):
        if self.getWorkerStatus() == "RUNNING":
            self.setWorkerStatus("WAITINGFOR_STOP")
            while True:
                if self.getWorkerStatus() == "STOPPED":
                    break
                else:
                    time.sleep(1)
        self.isAgentStarted = False

    def SaveJob(self, p_objJOB: JOB):
        m_SQL = "SELECT COUNT(1) FROM SQLCLI_JOBS WHERE JOB_ID = " + str(p_objJOB.id)
        m_db_cursor = self.MetaConn.cursor()
        m_db_cursor.execute(m_SQL)
        m_rs = m_db_cursor.fetchone()
        if m_rs[0] == 0:
            m_SQL = "Insert Into SQLCLI_JOBS(JOB_ID,JOB_Name,Starter_Interval, Starter_Last_Active_Time, " \
                    "Parallel, Loop, Started_JOBS, Failed_JOBS,Finished_JOBS,Active_JOBS,Error_Message, " \
                    "Script, Script_FullName, Think_Time,  Timeout, Submit_Time, Start_Time, End_Time, " \
                    "Blowout_Threshold_Count, Status) VALUES (" \
                    "?,?,?,?," \
                    "?,?,?,?,?,?,?, " \
                    "?,?,?,?,?,?,?," \
                    "?,?" \
                    ")"
            m_Data = [
                p_objJOB.id,
                p_objJOB.job_name,
                p_objJOB.starter_interval,
                p_objJOB.starter_last_active_time,
                p_objJOB.parallel,
                p_objJOB.loop,
                p_objJOB.started_jobs,
                p_objJOB.failed_jobs,
                p_objJOB.finished_jobs,
                p_objJOB.active_jobs,
                p_objJOB.error_message,
                p_objJOB.script,
                p_objJOB.script_fullname,
                p_objJOB.think_time,
                p_objJOB.timeout,
                p_objJOB.submit_time,
                p_objJOB.start_time,
                p_objJOB.end_time,
                p_objJOB.blowout_threshold_count,
                p_objJOB.status
            ]
            m_db_cursor.execute(m_SQL, parameters=m_Data)
        else:
            m_SQL = "Update SQLCLI_JOBS " \
                    "SET    JOB_NAME = ?," \
                    "       Starter_Interval = ?," \
                    "       Starter_Last_Active_Time = ?," \
                    "       Parallel = ?," \
                    "       Loop = ?," \
                    "       Started_JOBS = ?," \
                    "       Failed_JOBS = ?," \
                    "       Finished_JOBS = ?," \
                    "       Active_JOBS = ?," \
                    "       Error_Message = ?," \
                    "       Script = ?," \
                    "       Script_FullName = ?," \
                    "       Think_Time = ?," \
                    "       Timeout = ?," \
                    "       Submit_Time = ?," \
                    "       Start_Time = ?," \
                    "       End_Time = ?," \
                    "       Blowout_Threshold_Count = ?," \
                    "       Status = ? " \
                    "WHERE  JOB_ID = " + str(p_objJOB.id)
            m_Data = [
                p_objJOB.job_name,
                p_objJOB.starter_interval,
                p_objJOB.starter_last_active_time,
                p_objJOB.parallel,
                p_objJOB.loop,
                p_objJOB.started_jobs,
                p_objJOB.failed_jobs,
                p_objJOB.finished_jobs,
                p_objJOB.active_jobs,
                p_objJOB.error_message,
                p_objJOB.script,
                p_objJOB.script_fullname,
                p_objJOB.think_time,
                p_objJOB.timeout,
                p_objJOB.submit_time,
                p_objJOB.start_time,
                p_objJOB.end_time,
                p_objJOB.blowout_threshold_count,
                p_objJOB.status
            ]
            m_db_cursor.execute(m_SQL, parameters=m_Data)
            for m_Task in p_objJOB.getTasks():
                m_SQL = "SELECT COUNT(1) FROM SQLCLI_TASKS " \
                        "WHERE JOB_ID=" + str(p_objJOB.id) + " AND TaskHandler_ID=" + str(m_Task.TaskHandler_ID)
                m_db_cursor = self.MetaConn.cursor()
                m_db_cursor.execute(m_SQL)
                m_rs = m_db_cursor.fetchone()
                if m_rs[0] == 0:
                    m_SQL = "Insert into SQLCLI_TASKS(JOB_ID,TaskHandler_ID,ProcessInfo," \
                            "start_time,end_time,exit_code,Finished_Status) VALUES(?,?,?,?,?,?,?)"
                    m_Data = [
                        p_objJOB.id,
                        m_Task.TaskHandler_ID,
                        m_Task.ProcessInfo,
                        m_Task.start_time,
                        m_Task.end_time,
                        m_Task.exit_code,
                        m_Task.Finished_Status
                    ]
                    m_db_cursor.execute(m_SQL, parameters=m_Data)
                else:
                    m_SQL = "UPDATE SQLCLI_TASKS " \
                            "SET    ProcessInfo = ?," \
                            "       start_time = ?," \
                            "       end_time = ?," \
                            "       exit_code = ?," \
                            "       Finished_Status = ? " \
                            "WHERE JOB_ID=" + str(p_objJOB.id) + " AND TaskHandler_ID=" + str(m_Task.TaskHandler_ID)
                    m_Data = [
                        m_Task.ProcessInfo,
                        m_Task.start_time,
                        m_Task.end_time,
                        m_Task.exit_code,
                        m_Task.Finished_Status
                    ]
                    m_db_cursor.execute(m_SQL, parameters=m_Data)
        m_db_cursor.close()
        self.MetaConn.commit()

    # 提交一个任务
    def createjob(self, p_szname: str):
        # 初始化一个任务
        m_Job = JOB()
        m_Job.setSubmitTime(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())))

        # 创建第一个JOB的时候，初始化共享服务器
        if not self.isAgentStarted:
            self.register()

        # 设置JOBID
        m_Job.setJobID(self.JobID + 1)
        m_Job.setJobName(p_szJobName=p_szname)

        # 当前的JOBID加一
        self.JobID = self.JobID + 1

        # 返回JOB信息
        return m_Job

    # 显示当前所有已经提交的任务信息
    def showjob(self, p_szjobName: str):
        """
            job_name status active_jobs failed_jobs finished_jobs submit_time start_time end_time

            max_transaction_time  avg_transaction_time min_transaction_time,
            parallel, loop, script, script_fullname, think_time, timeout,
            blowout_threshold_count
        """
        # 如果输入的参数为all，则显示全部的JOB信息
        if p_szjobName.lower() == "all":
            m_Header = ["job_name", "status", "active_jobs", "failed_jobs", "finished_jobs",
                        "submit_time", "start_time", "end_time"]
            m_Result = []
            for m_JOB in self.getAllJobs():
                m_Result.append([m_JOB.getJobName(), m_JOB.getStatus(), m_JOB.getActiveJobs(),
                                 m_JOB.getFailedJobs(), m_JOB.getFinishedJobs(),
                                 str(m_JOB.getSubmitTime()), str(m_JOB.getStartTime()),
                                 str(m_JOB.getEndTime())])
            return None, m_Result, m_Header, None, "Total [" + str(len(m_Result)) + "] Jobs."
        else:
            strMessages = ""
            m_Job = self.getJobByName(p_szjobName)
            if m_Job is None:
                return None, None, None, None, "JOB [" + p_szjobName + "] does not exist."
            strMessages = strMessages + 'JOB_Name = [{0:12}]; ID = [{1:4d}]; Status = [{2:19}]\n'.\
                format(p_szjobName, m_Job.getJobID(), m_Job.getStatus())
            strMessages = strMessages + 'ActiveJobs/FailedJobs/FinishedJobs: [{0:10d}/{1:10d}/{2:10d}]\n'.\
                format(m_Job.getActiveJobs(), m_Job.getFailedJobs(), m_Job.getFinishedJobs())
            strMessages = strMessages + 'Submit Time: [{0:55}]\n'.format(str(m_Job.getSubmitTime()))
            strMessages = strMessages + 'Start Time : [{0:20}] ; End Time: [{1:20}]\n'.\
                format(str(m_Job.getStartTime()), str(m_Job.getEndTime()))
            strMessages = strMessages + 'Script              : [{0:46}]\n'.format(str(m_Job.getScript()))
            strMessages = strMessages + 'Script Full FileName: [{0:46}]\n'.format(str(m_Job.getScriptFullName()))
            strMessages = strMessages + 'Parallel: [{0:10d}]; Loop: [{1:10d}]; Starter Interval: [{2:5d}s]\n'.\
                format(m_Job.getParallel(), m_Job.getLoop(),
                       m_Job.getStarterInterval())
            if m_Job.getStartTime() is None:
                m_ElapsedTime = 0
            else:
                m_ElapsedTime = time.time() - time.mktime(time.strptime(m_Job.getStartTime(), "%Y-%m-%d %H:%M:%S"))
            strMessages = strMessages + 'Think time: [{0:10d}]; Timeout: [{1:10d}]; Elapsed: [{2:10s}]\n'.\
                format(m_Job.getThinkTime(), m_Job.getTimeOut(), "%10.2f" % float(m_ElapsedTime))
            strMessages = strMessages + 'Blowout Threshold Count: [{0:43d}]\n'.\
                format(m_Job.getBlowoutThresHoldCount())
            strMessages = strMessages + 'Message : [{0:58s}]\n'.format(str(m_Job.getErrorMessage()))
            strMessages = strMessages + 'Detail Tasks>>>:\n'
            strMessages = strMessages + '+{0:10s}+{1:10s}+{2:20s}+{3:20s}+\n'.format('-'*10, '-'*10, '-'*20, '-'*20)
            strMessages = strMessages + '|{0:10s}|{1:10s}|{2:20s}|{3:20s}|\n'.format(
                'Task-ID', 'PID', 'Start_Time', 'End_Time')
            strMessages = strMessages + '+{0:10s}+{1:10s}+{2:20s}+{3:20s}+\n'.format('-'*10, '-'*10, '-'*20, '-'*20)
            for m_Task in m_Job.getTasks():
                if m_Task.start_time is None:
                    m_StartTime = "____-__-__ __:__:__"
                else:
                    m_StartTime = datetime.datetime.fromtimestamp(m_Task.start_time).\
                        strftime("%Y-%m-%d %H:%M:%S")
                if m_Task.end_time is None:
                    m_EndTime = "____-__-__ __:__:__"
                else:
                    m_EndTime = datetime.datetime.fromtimestamp(m_Task.end_time).\
                        strftime("%Y-%m-%d %H:%M:%S")
                strMessages = strMessages + '|{0:10d}|{1:10d}|{2:20s}|{3:20s}|\n'.\
                    format(m_Task.TaskHandler_ID, m_Task.ProcessInfo,
                           m_StartTime, m_EndTime)
                strMessages = strMessages + '+{0:10s}+{1:10s}+{2:20s}+{3:20s}+\n'.\
                    format('-' * 10, '-' * 10, '-' * 20, '-' * 20)
            return None, None, None, None, strMessages

    # 启动JOB
    def startjob(self, p_jobName: str):
        # 将JOB从Submitted变成Runnning
        # 如果输入的参数为all，则启动全部的JOB信息
        nJobStarted = 0
        if p_jobName.lower() == "all":
            m_JobList = self.getAllJobs()
        else:
            m_JobList = [self.getJobByName(p_jobName), ]
        for m_Job in m_JobList:
            if m_Job.getStatus() == "Submitted":
                nJobStarted = nJobStarted + 1
                # 初始化Task列表
                m_Job.initTaskList()
                # 标记Task已经开始运行
                m_Job.setStatus("RUNNING")
                # 设置Task运行开始时间
                m_Job.setStartTime(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())))
                # 将任务信息更新到后台进程信息中
                self.SaveJob(m_Job)
        return nJobStarted

    # 等待所有的JOB完成
    # 这里不包括光Submit，并没有实质性开始动作的JOB
    def waitjob(self, p_jobName: str):
        if p_jobName.lower() == "all":
            while True:
                # 没有正在运行的JOB
                if not self.isAllJobClosed():
                    time.sleep(3)
                    continue
                # 没有已经提交，但是还没有运行的JOB
                bAllProcessFinished = True
                for m_Job in self.getAllJobs():
                    if m_Job.getStatus() not in \
                            ["Submitted", "FINISHED", "SHUTDOWNED", "ABORTED"]:
                        bAllProcessFinished = False
                        time.sleep(3)
                        continue
                if bAllProcessFinished:
                    break
        else:
            while True:
                if self.isJobClosed(p_jobName):
                    break
                else:
                    time.sleep(3)

    # 停止JOB作业
    def shutdownjob(self, p_jobName: str):
        # 将JOB从Runnning变成waitingfor_shutdown
        # 如果输入的参数为all，则停止全部的JOB信息
        nJobShutdowned = 0
        if p_jobName.lower() == "all":
            m_JobList = self.getAllJobs()
        else:
            m_JobList = [self.getJobByName(p_jobName), ]
        for m_Job in m_JobList:
            if m_Job.getStatus() in ("RUNNING", "Submitted"):
                nJobShutdowned = nJobShutdowned + 1
                # 标记Task已经开始运行
                m_Job.setStatus("WAITINGFOR_SHUTDOWN")
                # 将任务信息更新到后台进程信息中
                self.SaveJob(m_Job)
        return nJobShutdowned

    # 放弃JOB作业
    def abortjob(self, p_jobName: str):
        # 将JOB从Runnning变成waitingfor_abort
        # 如果输入的参数为all，则放弃全部的JOB信息
        nJobAborted = 0
        if p_jobName.lower() == "all":
            m_JobList = self.getAllJobs()
        else:
            m_JobList = [self.getJobByName(p_jobName), ]
        for m_Job in m_JobList:
            if m_Job.getStatus() == "RUNNING":
                nJobAborted = nJobAborted + 1
                # 标记Task已经开始运行
                m_Job.setStatus("WAITINGFOR_ABORT")
                # 将任务信息更新到后台进程信息中
                self.SaveJob(m_Job)
        return nJobAborted

    # 判断是否所有有效的子进程都已经退出
    def isJobClosed(self, p_JobName: str):
        m_Job = self.getJobByName(p_JobName)
        if m_Job is None:
            raise SQLCliException("Invalid JOB name. [" + str(p_JobName) + "]")
        return m_Job.getStatus() == "FINISHED"

    # 判断是否所有有效的子进程都已经退出
    def isAllJobClosed(self):
        # 当前有活动进程存在
        return len(self.ProcessInfo) == 0

    # 处理JOB的相关命令
    def Process_Command(self, p_szCommand: str):
        if self.MetaConn is None:
            # 如果没有Meta连接，直接退出
            return None, None, None, None, "SQLCLI-00000: JOB Manager is not started. command abort!"

        m_szSQL = p_szCommand.strip()

        # 创建新的JOB
        matchObj = re.match(r"job\s+create\s+(.*)(\s+)?$",
                            m_szSQL, re.IGNORECASE | re.DOTALL)
        if matchObj:
            m_ParameterList = str(matchObj.group(1)).strip().split()
            # 第一个参数是JOBNAME
            m_JobName = m_ParameterList[0].strip()
            m_Job = self.createjob(m_JobName)
            m_ParameterList = m_ParameterList[1:]
            for m_nPos in range(0, len(m_ParameterList) // 2):
                m_ParameterName = m_ParameterList[2*m_nPos]
                m_ParameterValue = m_ParameterList[2 * m_nPos + 1]
                m_Job.setjob(m_ParameterName, m_ParameterValue)
            self.SaveJob(m_Job)
            return None, None, None, None, "JOB [" + m_JobName + "] create successful."

        # 显示当前的JOB
        matchObj = re.match(r"job\s+show\s+(.*)$",
                            m_szSQL, re.IGNORECASE | re.DOTALL)
        if matchObj:
            m_JobName = str(matchObj.group(1)).strip()
            return self.showjob(m_JobName)

        # 设置JOB的各种参数
        matchObj = re.match(r"job\s+set\s+(.*)\s+(.*)\s+(.*)$",
                            m_szSQL, re.IGNORECASE | re.DOTALL)
        if matchObj:
            m_JobName = str(matchObj.group(1)).strip()
            m_ParameterName = str(matchObj.group(2)).strip()
            m_ParameterValue = str(matchObj.group(3)).strip()
            m_Job = self.getJobByName(m_JobName)
            m_Job.setjob(m_ParameterName, m_ParameterValue)
            self.SaveJob(m_Job)
            return None, None, None, None, "JOB [" + m_JobName + "] set successful."

        # 启动JOB
        matchObj = re.match(r"job\s+start\s+(.*)$",
                            m_szSQL, re.IGNORECASE | re.DOTALL)
        if matchObj:
            m_JobName = str(matchObj.group(1)).strip()
            nJobStarted = self.startjob(m_JobName)
            return None, None, None, None, "Total [" + str(nJobStarted) + "] jobs started."

        # 等待JOB完成
        matchObj = re.match(r"job\s+wait\s+(.*)$",
                            m_szSQL, re.IGNORECASE | re.DOTALL)
        if matchObj:
            m_JobName = str(matchObj.group(1)).strip()
            self.waitjob(m_JobName)
            return None, None, None, None, "All jobs [" + m_JobName + "] finished."

        # 终止JOB作业
        matchObj = re.match(r"job\s+shutdown\s+(.*)$",
                            m_szSQL, re.IGNORECASE | re.DOTALL)
        if matchObj:
            m_JobName = str(matchObj.group(1)).strip()
            nJobShutdowned = self.shutdownjob(m_JobName)
            return None, None, None, None, "Total [" + str(nJobShutdowned) + "] jobs shutdowned."

        # 放弃JOB作业
        matchObj = re.match(r"job\s+abort\s+(.*)$",
                            m_szSQL, re.IGNORECASE | re.DOTALL)
        if matchObj:
            m_JobName = str(matchObj.group(1)).strip()
            nJobAborted = self.abortjob(m_JobName)
            return None, None, None, None, "Total [" + str(nJobAborted) + "] jobs aborted."

        raise SQLCliException("Invalid JOB Command [" + m_szSQL + "]")

    # 设置进程的启动相关上下文信息
    def setProcessContextInfo(self, p_ContextName, p_ContextValue):
        self.ProcessContextInfo[p_ContextName] = p_ContextValue

    # 获得进程的启动相关上下文信息
    def getProcessContextInfo(self, p_ContextName):
        return self.ProcessContextInfo[p_ContextName]
