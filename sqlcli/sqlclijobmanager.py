# -*- coding: utf-8 -*-
import re
import threading
import time
import os
import datetime
from multiprocessing import Process, Lock
from multiprocessing.managers import BaseManager

from .sqlcliexception import SQLCliException
from .sqlclisga import SQLCliGlobalSharedMemory
from .sqlclijob import JOB, Task


# JOB任务的管理
class JOBManager(object):
    def __init__(self):
        # 初始化进程锁, 用于对进程信息的共享
        self.LOCK_JOBCATALOG = Lock()

        # 是否已经注册
        self._isRegistered = False

        # Python共享服务器管理
        self.manager = BaseManager()
        self.manager.register('SQLCliGlobalSharedMemory', callable=SQLCliGlobalSharedMemory)

        # 后台共享信息
        self.SharedProcessInfoHandler = None

        # 进程管理的相关上下文信息
        # 来自于父进程，当子进程不进行特殊设置的时候，进程管理中用到的信息将集成父进程
        self.ProcessContextInfo = {}

        # 进程句柄信息
        self.ProcessInfo = {}

    # 返回是否已经注册到共享服务器
    def isRegistered(self):
        return self._isRegistered

    @staticmethod
    def runSQLCli(p_args):
        from .main import SQLCli

        HeadLessConsole = open(os.devnull, "w")
        m_SQLCli = SQLCli(
            sqlscript=p_args["sqlscript"],
            logon=p_args["logon"],
            logfilename=p_args["logfilename"],
            sqlmap=p_args["sqlmap"],
            nologo=p_args["nologo"],
            breakwitherror=False,
            Console=HeadLessConsole,
            HeadlessMode=True,
            sqlperf=p_args["sqlperf"]
        )
        m_SQLCli.run_cli()

    # 后台守护线程，跟踪进程信息，启动或强制关闭进程
    def JOBManagerAgent(self):
        while True:
            if self.SharedProcessInfoHandler.getWorkerStatus() == "WAITINGFOR_STOP":
                # 如果程序退出，则退出该线程
                self.SharedProcessInfoHandler.setWorkerStatus("STOPPED")
                break
            m_Jobs = self.SharedProcessInfoHandler.Get_Jobs()
            for Job_Name, Job_Context in m_Jobs.items():
                if Job_Context.getStatus() == "Failed":
                    # 已经失败的Case不再处理
                    continue
                if Job_Context.getStatus() == "Running":
                    # 检查进程运行的状态，如果进程已经不存在，检查exitcode
                    m_TaskList = Job_Context.getTasks()
                    for m_Task in m_TaskList:
                        m_ProcessID = m_Task.ProcessInfo
                        m_Process = self.ProcessInfo[m_ProcessID]
                        if not m_Process.is_alive():
                            self.LOCK_JOBCATALOG.acquire()  # 对共享的进程状态信息加锁
                            # 进程已经不存在，标记进程状态
                            Job_Context.setFinishedJobs(Job_Context.getFinishedJobs() + 1)
                            m_ExitCode = m_Process.exitcode
                            if m_ExitCode != 0:
                                Job_Context.setFailedJobs(Job_Context.getFailedJobs() + 1)
                            # 从Task以及ProcessInfo中删除相关记录
                            Job_Context.delTask(m_Task)
                            self.SharedProcessInfoHandler.Update_Job(Job_Name, Job_Context)
                            self.LOCK_JOBCATALOG.release()  # 对共享的进程状态信息解锁
                            self.ProcessInfo.pop(m_ProcessID)
                    if Job_Context.getScript() is None:
                        # 检查脚本信息
                        self.LOCK_JOBCATALOG.acquire()  # 对共享的进程状态信息加锁
                        Job_Context.setStatus("Failed")
                        Job_Context.setErrorMessage("Script parameter is null.")
                        self.SharedProcessInfoHandler.Update_Job(Job_Name, Job_Context)
                        self.LOCK_JOBCATALOG.release()  # 对共享的进程状态信息解锁
                        continue
                    if Job_Context.getFinishedJobs() >= Job_Context.getLoop():
                        # 已经完成了全部的作业，标记为完成状态
                        self.LOCK_JOBCATALOG.acquire()  # 对共享的进程状态信息加锁
                        Job_Context.setStatus("Finished")
                        Job_Context.setEndTime(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())))
                        # 将任务添加到后台进程信息中
                        self.SharedProcessInfoHandler.Update_Job(Job_Name, Job_Context)
                        self.LOCK_JOBCATALOG.release()  # 对共享的进程状态信息解锁
                        continue
                    if Job_Context.getScriptFullName() is None:
                        # 如果脚本没有补充完全的脚本名称，则此刻进行补充
                        # 命令里头的是全路径名，或者是基于当前目录的相对文件名
                        m_Script_FileName = Job_Context.getScript()
                        if os.path.isfile(m_Script_FileName):
                            m_SQL_ScriptBaseName = os.path.basename(m_Script_FileName)
                            m_SQL_ScriptFullName = os.path.abspath(m_Script_FileName)
                        else:
                            m_SQL_ScriptHomeDirectory = os.path.dirname(self.getProcessContextInfo("sqlscript"))
                            if os.path.isfile(os.path.join(m_SQL_ScriptHomeDirectory, m_Script_FileName)):
                                m_SQL_ScriptBaseName = \
                                    os.path.basename(os.path.join(m_SQL_ScriptHomeDirectory, m_Script_FileName))
                                m_SQL_ScriptFullName = \
                                    os.path.abspath(os.path.join(m_SQL_ScriptHomeDirectory, m_Script_FileName))
                            else:
                                self.LOCK_JOBCATALOG.acquire()  # 对共享的进程状态信息加锁
                                Job_Context.setStatus("Failed")
                                Job_Context.setErrorMessage("Script [" + m_Script_FileName + "] does not exist.")
                                self.SharedProcessInfoHandler.Update_Job(Job_Name, Job_Context)
                                self.LOCK_JOBCATALOG.release()  # 对共享的进程状态信息解锁
                                continue
                        self.LOCK_JOBCATALOG.acquire()  # 对共享的进程状态信息加锁
                        Job_Context.setScript(m_SQL_ScriptBaseName)
                        Job_Context.setScriptFullName(m_SQL_ScriptFullName)
                        self.SharedProcessInfoHandler.Update_Job(Job_Name, Job_Context)
                        self.LOCK_JOBCATALOG.release()  # 对共享的进程状态信息解锁
                    if Job_Context.getStartedJobs() >= Job_Context.getLoop():
                        # 所有需要的进程都已经启动，不再处理这个任务
                        continue
                    if Job_Context.getActiveJobs() >= Job_Context.getParallel():
                        # 判断已经达到了最大并发数的限制
                        continue
                    # 本次需要启动多少个进程
                    m_ProcessNeedStarted = 0
                    # 读取最后一次启动的时间
                    m_StarterLastActiveTime = Job_Context.getStarterLastActiveTime()
                    m_StarterInterval = Job_Context.getStarterInterval()
                    if m_StarterInterval != 0:
                        # 任务配置了启动时间间隔的要求
                        if m_StarterLastActiveTime is None:
                            # 之前没有启动过，这是第一次要启动进程
                            m_ProcessNeedStarted = Job_Context.getStarterMaxProcess()
                            if m_ProcessNeedStarted > Job_Context.getParallel() - Job_Context.getActiveJobs():
                                m_ProcessNeedStarted = Job_Context.getParallel() - Job_Context.getActiveJobs()
                        if m_StarterLastActiveTime is not None:
                            # 每次启动的时间有限制，应检查时间，保证启动周期
                            m_CurrentTime = int(time.mktime(datetime.datetime.now().timetuple()))
                            if m_StarterLastActiveTime + m_StarterInterval > m_CurrentTime:
                                # 还不到可以启动的时间，暂时不启动
                                continue
                            else:
                                m_ProcessNeedStarted = Job_Context.getStarterMaxProcess()
                                if m_ProcessNeedStarted > Job_Context.getParallel() - Job_Context.getActiveJobs():
                                    m_ProcessNeedStarted = Job_Context.getParallel() - Job_Context.getActiveJobs()
                    else:
                        # 没有配置启动时间的间隔，则本次启动应该是达到最大数量的全部
                        m_ProcessNeedStarted = Job_Context.getParallel() - Job_Context.getActiveJobs()
                    if m_ProcessNeedStarted > Job_Context.getLoop() - Job_Context.getFinishedJobs():
                        # 剩余的进程不需要全部启动
                        m_ProcessNeedStarted = Job_Context.getLoop() - Job_Context.getFinishedJobs()
                    # 有进程需要进行启动
                    if m_ProcessNeedStarted != 0:
                        # 标记最后启动的时间
                        m_CurrentTime = int(time.mktime(datetime.datetime.now().timetuple()))
                        self.LOCK_JOBCATALOG.acquire()  # 对共享的进程状态信息加锁
                        Job_Context.setStarterLastActiveTime(m_CurrentTime)
                        self.SharedProcessInfoHandler.Update_Job(Job_Name, Job_Context)
                        self.LOCK_JOBCATALOG.release()  # 对共享的进程状态信息解锁
                        # 循环将任务放入到Task中，并开始启动进程
                        for nPos in range(0, m_ProcessNeedStarted):
                            m_args = {"logon": self.getProcessContextInfo("logon"),
                                      "nologo": self.getProcessContextInfo("nologo"),
                                      "sqlperf": self.getProcessContextInfo("sqlperf"),
                                      "sqlmap": self.getProcessContextInfo("sqlmap"),
                                      "sqlscript": Job_Context.getScriptFullName()}
                            # Job_Context = JOB()
                            if self.getProcessContextInfo("logfilename") is not None:
                                m_logfilename = os.path.join(
                                    os.path.dirname(self.getProcessContextInfo("logfilename")),
                                    Job_Context.getScript().split('.')[0] + "_" + str(Job_Context.getJobID()) +
                                    "-" + str(Job_Context.getStartedJobs()) + ".log")
                            else:
                                m_logfilename = \
                                    Job_Context.getScript().split('.')[0] + "_" + str(Job_Context.getJobID()) + \
                                    "-" + str(Job_Context.getStartedJobs()) + ".log"
                            m_args["logfilename"] = m_logfilename
                            m_Process = Process(target=self.runSQLCli, args=(m_args,))
                            m_Process.start()
                            # 将Process信息放入到JOB列表中， 并启动
                            m_ProcessTask = Task()
                            m_ProcessTask.ProcessInfo = m_Process.pid
                            m_ProcessTask.start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
                            self.LOCK_JOBCATALOG.acquire()  # 对共享的进程状态信息加锁
                            Job_Context.addTask(m_ProcessTask)
                            self.SharedProcessInfoHandler.Update_Job(Job_Name, Job_Context)
                            self.LOCK_JOBCATALOG.release()  # 对共享的进程状态信息解锁
                            self.ProcessInfo[m_Process.pid] = m_Process

            # 如果有WAITING_SHUTDOWN的，则不再启动，当没有活动进程的时候，标记为CLOSED

            # 如果有WAITING_ABORT的，则杀掉进程

            # 如果有Timeout的，则Abort

            # 如果还有没完成的，且符合Think_Time约定的，启动它

            time.sleep(2)

    # 返回进程信息到共享服务器，来满足进程并发使用
    def register(self):
        # 注册后台共享进程管理
        self.manager.start()
        func = getattr(self.manager, 'SQLCliGlobalSharedMemory')
        self.SharedProcessInfoHandler = func()
        self.SharedProcessInfoHandler.setWorkerStatus("STARTED")

        # 启动后台守护线程，用来处理延时启动，超时等问题
        Agenthread = threading.Thread(target=self.JOBManagerAgent)
        Agenthread.setDaemon(True)  # 主进程退出，守护进程也会退出
        Agenthread.setName("JobManagerAgent")
        Agenthread.start()

        # 标记状态信息
        self._isRegistered = True

    # 退出共享服务器
    def unregister(self):
        if self._isRegistered:
            # 通知线程退出处理
            self.SharedProcessInfoHandler.setWorkerStatus("WAITINGFOR_STOP")
            self._isRegistered = False
            # 退出RPC进程
            self.manager.shutdown()

    # 提交一个任务
    def createjob(self, p_szname: str):
        # 初始化一个任务
        m_Job = JOB()
        m_Job.setSubmitTime(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())))

        # 初始化共享服务器
        if not self.isRegistered():
            self.register()

        # 对共享的进程状态信息加锁
        self.LOCK_JOBCATALOG.acquire()

        # 设置JOBID
        m_Job.setJobID(self.SharedProcessInfoHandler.getJobID())

        # 将任务添加到后台进程信息中
        self.SharedProcessInfoHandler.Update_Job(p_szname, m_Job)

        # 任务不需要启动，对共享的进程状态信息解锁
        self.LOCK_JOBCATALOG.release()

    # 显示当前所有已经提交的任务信息
    def showjob(self, p_szjobName: str):
        """
            job_name status active_jobs failed_jobs finished_jobs submit_time start_time end_time

            max_transaction_time  avg_transaction_time min__transaction_time,
            parallel, starter_maxprocess, loop, script, script_fullname, think_time, timeout,
            shutdown_mode, fail_mode, blowout_threshold_percent, blowout_threshold_count
        """
        # 如果输入的参数为all，则显示全部的JOB信息
        if p_szjobName.lower() == "all":
            m_Jobs = self.SharedProcessInfoHandler.Get_Jobs()
            m_Header = ["job_name", "status", "active_jobs", "failed_jobs", "finished_jobs",
                        "submit_time", "start_time", "end_time"]
            m_Result = []
            for Job_Name, Job_Context in m_Jobs.items():
                m_Result.append([Job_Name, Job_Context.getStatus(), Job_Context.getActiveJobs(),
                                 Job_Context.getFailedJobs(), Job_Context.getFinishedJobs(),
                                 str(Job_Context.getSubmitTime()), str(Job_Context.getStartTime()),
                                 str(Job_Context.getEndTime())])
            return None, m_Result, m_Header, None, "Total [" + str(len(m_Result)) + "] Jobs."
        else:
            strMessages = ""
            m_Job = self.SharedProcessInfoHandler.Get_Job(p_szjobName)
            strMessages = strMessages + 'JOB_Name = [{0:15}] ; ID = [{1:8d}] ; Status = [{2:10}]\n'.\
                format(p_szjobName, m_Job.getJobID(), m_Job.getStatus())
            strMessages = strMessages + 'ActiveJobs/FailedJobs/FinishedJobs: [{0:10d}/{1:10d}/{2:10d}]\n'.\
                format(m_Job.getActiveJobs(), m_Job.getFailedJobs(), m_Job.getFinishedJobs())
            strMessages = strMessages + 'Submit Time: [{0:55}]\n'.format(str(m_Job.getSubmitTime()))
            strMessages = strMessages + 'Start Time : [{0:20}] ; End Time: [{1:20}]\n'.\
                format(str(m_Job.getStartTime()), str(m_Job.getEndTime()))
            strMessages = strMessages + 'Script              : [{0:46}]\n'.format(str(m_Job.getScript()))
            strMessages = strMessages + 'Script Full FileName: [{0:46}]\n'.format(str(m_Job.getScriptFullName()))
            strMessages = strMessages + 'Parallel: [{0:10d}]; Loop: [{1:10d}]; Starter: [{2:8d}/{3:5d}s]\n'.\
                format(m_Job.getParallel(), m_Job.getLoop(),
                       m_Job.getStarterMaxProcess(),
                       m_Job.getStarterInterval())
            if m_Job.getStartTime() is None:
                m_ElapsedTime = 0
            else:
                m_ElapsedTime = time.time() - time.mktime(time.strptime(m_Job.getStartTime(), "%Y-%m-%d %H:%M:%S"))

            strMessages = strMessages + 'Think time: [{0:10d}]; Timeout: [{1:10d}]; Elapsed: [{2:10s}]\n'.\
                format(m_Job.getThinkTime(), m_Job.getTimeOut(), "%10.2f" % float(m_ElapsedTime))
            strMessages = strMessages + 'Shutdown MODE: [{0:19}]; FAIL_MODE: [{1:19}]\n'.\
                format(m_Job.getShutdownMode(), m_Job.getFailMode())
            strMessages = strMessages + 'Blowout Threshold Percent%/Count: [{0:16d}%/{1:16d}]\n'.\
                format(m_Job.getBlowoutThresHoldPrecent(), m_Job.getBlowoutThresHoldCount())
            strMessages = strMessages + 'Error Message : [{0:52s}]\n'.format(str(m_Job.getErrorMessage()))
            return None, None, None, None, strMessages

    # 设置JOB的各种参数
    def setjob(self, p_jobName: str, p_ParameterName: str, p_ParameterValue: str):
        m_Job = self.SharedProcessInfoHandler.Get_Job(p_jobName)
        if m_Job is None:
            raise SQLCliException("Invalid JOB name. [" + str(p_jobName) + "]")
        if p_ParameterName.strip().lower() == "parallel":
            m_Job.setParallel(int(p_ParameterValue))
        elif p_ParameterName.strip().lower() == "starter_maxprocess":
            m_Job.setStarterMaxProcess(int(p_ParameterValue))
        elif p_ParameterName.strip().lower() == "starter_interval":
            m_Job.setStarterInterval(int(p_ParameterValue))
        elif p_ParameterName.strip().lower() == "loop":
            m_Job.setLoop(int(p_ParameterValue))
        elif p_ParameterName.strip().lower() == "script":
            m_Job.setScript(p_ParameterValue)
        elif p_ParameterName.strip().lower() == "think_time":
            m_Job.setThinkTime(int(p_ParameterValue))
        elif p_ParameterName.strip().lower() == "timeout":
            m_Job.setTimeOut(int(p_ParameterValue))
        elif p_ParameterName.strip().lower() == "shutdown_mode":
            m_Job.setShutdownMode(p_ParameterValue)
        elif p_ParameterName.strip().lower() == "fail_mode":
            m_Job.setFailMode(p_ParameterValue)
        elif p_ParameterName.strip().lower() == "blowout_threshold_percent":
            m_Job.setBlowoutThresHoldPrecent(int(p_ParameterValue))
        elif p_ParameterName.strip().lower() == "blowout_threshold_count":
            m_Job.setBlowoutThresHoldCount(int(p_ParameterValue))
        else:
            raise SQLCliException("Invalid JOB Parameter name. [" + str(p_ParameterName) + "]")
        # 回写到保存的结构中
        self.LOCK_JOBCATALOG.acquire()   # 对共享的进程状态信息加锁
        self.SharedProcessInfoHandler.Update_Job(p_jobName, m_Job)
        self.LOCK_JOBCATALOG.release()   # 对共享的进程状态信息解锁

    # 启动JOB
    def startjob(self, p_jobName: str):
        # 将JOB从Submitted变成Runnning
        # 日后可以考虑资源等设置信息

        # 如果输入的参数为all，则启动全部的JOB信息
        nJobStarted = 0
        if p_jobName.lower() == "all":
            m_Jobs = self.SharedProcessInfoHandler.Get_Jobs()
        else:
            m_Jobs = {p_jobName: self.SharedProcessInfoHandler.Get_Job(p_jobName), }
        for Job_Name, Job_Context in m_Jobs.items():
            if Job_Context.getStatus() == "Submitted":
                nJobStarted = nJobStarted + 1
                self.LOCK_JOBCATALOG.acquire()  # 对共享的进程状态信息加锁
                Job_Context.setStatus("Running")
                Job_Context.setStartTime(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())))
                # 将任务添加到后台进程信息中
                self.SharedProcessInfoHandler.Update_Job(Job_Name, Job_Context)
                self.LOCK_JOBCATALOG.release()  # 对共享的进程状态信息解锁

        return nJobStarted

    # 等待所有的JOB完成
    def waitjob(self, p_jobName: str):
        if p_jobName.lower() == "all":
            while True:
                # 没有正在运行的JOB
                if not self.isAllJobClosed():
                    time.sleep(3)
                    continue
                # 没有已经提交，但是还没有运行的JOB
                bAllProcessFinished = True
                m_Jobs = self.SharedProcessInfoHandler.Get_Jobs()
                for Job_Name, Job_Context in m_Jobs.items():
                    if Job_Context.getStatus() not in ["Finished", ]:
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

    # 判断是否所有有效的子进程都已经退出
    def isJobClosed(self, p_JobName: str):
        m_Job = self.SharedProcessInfoHandler.Get_Job(p_JobName)
        if m_Job is None:
            raise SQLCliException("Invalid JOB name. [" + str(p_JobName) + "]")
        return m_Job.getStatus() == "Finished"

    # 判断是否所有有效的子进程都已经退出
    def isAllJobClosed(self):
        return len(self.ProcessInfo) == 0

    # 处理JOB的相关命令
    def Process_Command(self, p_szCommand: str):
        m_szSQL = p_szCommand.strip()

        # 创建新的JOB
        matchObj = re.match(r"job\s+create\s+(.*)$",
                            m_szSQL, re.IGNORECASE | re.DOTALL)
        if matchObj:
            m_JobName = str(matchObj.group(1)).strip()
            self.createjob(m_JobName)
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
            self.setjob(m_JobName, m_ParameterName, m_ParameterValue)
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

        raise SQLCliException("Invalid JOB Command [" + m_szSQL + "]")

    # 设置进程的启动相关上下文信息
    def setProcessContextInfo(self, p_ContextName, p_ContextValue):
        self.ProcessContextInfo[p_ContextName] = p_ContextValue

    # 获得进程的启动相关上下文信息
    def getProcessContextInfo(self, p_ContextName):
        return self.ProcessContextInfo[p_ContextName]
