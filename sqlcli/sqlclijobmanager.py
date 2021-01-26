# -*- coding: utf-8 -*-
import re
import threading
import time
import os
import datetime
from multiprocessing import Process

from .sqlcliexception import SQLCliException
from .sqlclijob import JOB


# JOB任务的管理
class JOBManager(object):
    def __init__(self):
        # 进程管理的相关上下文信息
        # 来自于父进程，当子进程不进行特殊设置的时候，进程管理中用到的信息将集成父进程
        self.ProcessContextInfo = {}

        # 进程句柄信息
        self.ProcessInfo = {}

        # 是否已经注册
        self._isRegistered = False

        # 当前清理进程的状态
        # 如果WorkerStatus==WAITINGFOR_STOP，则Worker主动停止
        self.WorkerStatus = "NOT-STARTED"

        # 当前的JOB流水号
        self.JobID = 1

        # 所有的JOB信息
        self.Jobs = dict()

        # 线程锁, 用来控制在更新JOB的时候的锁
        self.JobManagerLocker = threading.Lock()

    # 返回是否已经注册到共享服务器
    def isRegistered(self):
        return self._isRegistered

    # 设置当前清理进程的状态
    def setWorkerStatus(self, p_WorkerStatus):
        self.WorkerStatus = p_WorkerStatus

    # 获得当前清理进程的状态
    def getWorkerStatus(self):
        return self.WorkerStatus

    # 更新JOB信息
    def Update_Job(self, p_JobName: str, p_Job: JOB):
        # 将JOB加入到数组中
        self.JobManagerLocker.acquire()
        self.Jobs[p_JobName] = p_Job
        self.JobManagerLocker.release()

    # 返回指定的JOB信息，根据JobName来判断
    def Get_Job(self, p_szJobName):
        # 返回指定的Job信息，如果找不到，返回None
        if p_szJobName in self.Jobs.keys():
            return self.Jobs[p_szJobName]
        else:
            return None

    @staticmethod
    def runSQLCli(p_args):
        from .main import SQLCli

        m_SQLCli = SQLCli(
            sqlscript=p_args["sqlscript"],
            logon=p_args["logon"],
            logfilename=p_args["logfilename"],
            sqlmap=p_args["sqlmap"],
            nologo=p_args["nologo"],
            HeadlessMode=True,
            WorkerName=p_args["workername"],
            sqlperf=p_args["sqlperf"],
            SharedProcessInfo=p_args["sga"]
        )
        m_SQLCli.run_cli()

    # 后台守护线程，跟踪进程信息，启动或强制关闭进程
    def JOBManagerAgent(self):
        while True:
            # 如果程序退出，则关闭该Agent线程
            if self.getWorkerStatus() == "WAITINGFOR_STOP":
                self.setWorkerStatus("NOT-STARTED")
                break
            # 循环处理工作JOB
            for Job_Name, Job_Context in self.Jobs.items():
                if Job_Context.getStatus() in ("FAILED", "SHUTDOWNED", "FINISHED", "ABORTED"):
                    # 已经失败的Case不再处理
                    continue
                if Job_Context.getStatus() in ("RUNNING", "WAITINGFOR_SHUTDOWN", "WAITINGFOR_ABORT"):
                    # 依次检查Task的状态
                    # 即使已经处于WAITINGFOR_SHUTDOWN或者WAITINGFOR_ABORT中，也不排除还有没有完成的作业
                    m_TaskList = Job_Context.getTasks()
                    currenttime = int(time.mktime(datetime.datetime.now().timetuple()))
                    bAllTaskFinished = True
                    for m_Task in m_TaskList:
                        m_ProcessID = m_Task.ProcessInfo
                        if m_ProcessID != 0:
                            m_Process = self.ProcessInfo[m_ProcessID]
                            if not m_Process.is_alive():
                                # 进程ID不是0，进程已经不存在，或者是正常完成，或者是异常退出
                                Job_Context.FinishTask(m_Task.TaskHandler_ID, m_Process.exitcode, "")
                                self.Update_Job(Job_Name, Job_Context)
                                # 从当前保存的进程信息中释放该进程
                                self.ProcessInfo.pop(m_ProcessID)
                            else:
                                # 进程还在运行中
                                if Job_Context.getTimeOut() != 0:
                                    # 设置了超时时间，我们需要根据超时时间进行判断
                                    if m_Task.start_time + Job_Context.getTimeOut() < currenttime:
                                        m_Process.terminate()
                                        Job_Context.FinishTask(m_Task.TaskHandler_ID,
                                                               m_Process.exitcode,
                                                               "TIMEOUT")
                                        self.Update_Job(Job_Name, Job_Context)
                                        # 从当前保存的进程信息中释放该进程
                                        self.ProcessInfo.pop(m_ProcessID)
                                    else:
                                        if Job_Context.getStatus() == "WAITINGFOR_ABORT":
                                            m_Process.terminate()
                                            Job_Context.FinishTask(m_Task.TaskHandler_ID,
                                                                   m_Process.exitcode,
                                                                   "ABORTED")
                                            self.Update_Job(Job_Name, Job_Context)
                                            # 从当前保存的进程信息中释放该进程
                                            self.ProcessInfo.pop(m_ProcessID)
                                        bAllTaskFinished = False
                                else:
                                    if Job_Context.getStatus() == "WAITINGFOR_ABORT":
                                        m_Process.terminate()
                                        Job_Context.FinishTask(m_Task.TaskHandler_ID,
                                                               m_Process.exitcode,
                                                               "ABORTED")
                                        self.Update_Job(Job_Name, Job_Context)
                                        # 从当前保存的进程信息中释放该进程
                                        self.ProcessInfo.pop(m_ProcessID)
                                    bAllTaskFinished = False
                                continue
                    if bAllTaskFinished and Job_Context.getStatus() == "WAITINGFOR_SHUTDOWN":
                        # 检查脚本信息，如果脚本压根不存在，则无法后续的操作
                        Job_Context.setStatus("SHUTDOWNED")
                        Job_Context.setErrorMessage("JOB has been shutdown successful.")
                        self.Update_Job(Job_Name, Job_Context)
                        continue
                    if Job_Context.getStatus() == "WAITINGFOR_ABORT":
                        # 检查脚本信息，如果脚本压根不存在，则无法后续的操作
                        Job_Context.setStatus("ABORTED")
                        Job_Context.setErrorMessage("JOB has been aborted.")
                        self.Update_Job(Job_Name, Job_Context)
                        continue
                    if Job_Context.getBlowoutThresHoldCount() != 0:
                        if Job_Context.getFailedJobs() >= Job_Context.getBlowoutThresHoldCount():
                            if bAllTaskFinished:
                                # 已经失败的脚本实在太多，不能再继续
                                Job_Context.setStatus("FAILED")
                                Job_Context.setErrorMessage("JOB blowout, terminate.")
                                self.Update_Job(Job_Name, Job_Context)
                            continue
                    if Job_Context.getScript() is None:
                        # 检查脚本信息，如果脚本压根不存在，则无法后续的操作
                        Job_Context.setStatus("FAILED")
                        Job_Context.setErrorMessage("Script parameter is null.")
                        self.Update_Job(Job_Name, Job_Context)
                        continue
                    if Job_Context.getScriptFullName() is None:
                        # 如果脚本没有补充完全的脚本名称，则此刻进行补充
                        # 命令里头的是全路径名，或者是基于当前目录的相对文件名
                        m_Script_FileName = Job_Context.getScript()
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
                                    Job_Context.setStatus("FAILED")
                                    Job_Context.setErrorMessage("Script [" + m_Script_FileName + "] does not exist.")
                                    self.Update_Job(Job_Name, Job_Context)
                                    continue
                            else:
                                Job_Context.setStatus("FAILED")
                                Job_Context.setErrorMessage("Script [" + m_Script_FileName + "] does not exist.")
                                self.Update_Job(Job_Name, Job_Context)
                                continue
                        Job_Context.setScript(m_SQL_ScriptBaseName)
                        Job_Context.setScriptFullName(m_SQL_ScriptFullName)
                        self.Update_Job(Job_Name, Job_Context)
                    if Job_Context.getFinishedJobs() >= Job_Context.getLoop():
                        # 已经完成了全部的作业，标记为完成状态
                        Job_Context.setStatus("FINISHED")
                        Job_Context.setEndTime(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())))
                        # 将任务添加到后台进程信息中
                        self.Update_Job(Job_Name, Job_Context)
                        continue
                    # 开始陆续启动需要完成的任务
                    # 获得可以启动的任务进程列表
                    m_TaskStarterList = Job_Context.getTaskStarter()
                    # 给每一个进程提供唯一的日志文件名
                    m_JOB_Sequence = Job_Context.getStartedJobs()
                    for m_TaskStarter in m_TaskStarterList:
                        # 循环启动所有的进程
                        m_args = {"logon": self.getProcessContextInfo("logon"),
                                  "nologo": self.getProcessContextInfo("nologo"),
                                  "sqlperf": self.getProcessContextInfo("sqlperf"),
                                  "sqlmap": self.getProcessContextInfo("sqlmap"),
                                  "sqlscript": Job_Context.getScriptFullName(),
                                  "sga": self.getProcessContextInfo("sga"),
                                  "workername":
                                      Job_Name + "#" + str(m_TaskStarter) + "-" +
                                      str(m_JOB_Sequence+1)
                                  }
                        if self.getProcessContextInfo("logfilename") is not None:
                            m_logfilename = os.path.join(
                                os.path.dirname(self.getProcessContextInfo("logfilename")),
                                Job_Context.getScript().split('.')[0] + "_" + str(Job_Context.getJobID()) +
                                "-" + str(m_JOB_Sequence+1) + ".log")
                        else:
                            m_logfilename = \
                                Job_Context.getScript().split('.')[0] + "_" + \
                                str(Job_Context.getJobID()) + "-" + \
                                str(m_JOB_Sequence+1) + ".log"
                        m_args["logfilename"] = m_logfilename
                        m_JOB_Sequence = m_JOB_Sequence + 1
                        m_Process = Process(target=self.runSQLCli,
                                            args=(m_args,))
                        m_Process.start()
                        # 将Process信息放入到JOB列表中，启动进程
                        Job_Context.StartTask(m_TaskStarter, m_Process.pid)
                        self.Update_Job(Job_Name, Job_Context)
                        self.ProcessInfo[m_Process.pid] = m_Process
            # 每2秒检查一次任务
            time.sleep(2)

    # 返回进程信息到共享服务器，来满足进程并发使用
    def register(self):
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
            self._isRegistered = False

    # 提交一个任务
    def createjob(self, p_szname: str):
        # 初始化一个任务
        m_Job = JOB()
        m_Job.setSubmitTime(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())))

        # 初始化共享服务器
        if not self.isRegistered():
            self.register()

        # 设置JOBID
        m_Job.setJobID(self.JobID + 1)
        self.JobID = self.JobID + 1

        # 将任务添加到后台进程信息中
        self.Update_Job(p_szname, m_Job)

    # 显示当前所有已经提交的任务信息
    def showjob(self, p_szjobName: str):
        """
            job_name status active_jobs failed_jobs finished_jobs submit_time start_time end_time

            max_transaction_time  avg_transaction_time min_transaction_time,
            parallel, starter_maxprocess, loop, script, script_fullname, think_time, timeout,
            blowout_threshold_count
        """
        # 如果输入的参数为all，则显示全部的JOB信息
        if p_szjobName.lower() == "all":
            m_Header = ["job_name", "status", "active_jobs", "failed_jobs", "finished_jobs",
                        "submit_time", "start_time", "end_time"]
            m_Result = []
            for Job_Name, Job_Context in self.Jobs.items():
                m_Result.append([Job_Name, Job_Context.getStatus(), Job_Context.getActiveJobs(),
                                 Job_Context.getFailedJobs(), Job_Context.getFinishedJobs(),
                                 str(Job_Context.getSubmitTime()), str(Job_Context.getStartTime()),
                                 str(Job_Context.getEndTime())])
            return None, m_Result, m_Header, None, "Total [" + str(len(m_Result)) + "] Jobs."
        else:
            strMessages = ""
            m_Job = self.Get_Job(p_szjobName)
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

    # 设置JOB的各种参数
    def setjob(self, p_jobName: str, p_ParameterName: str, p_ParameterValue: str):
        m_Job = self.Get_Job(p_jobName)
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
        elif p_ParameterName.strip().lower() == "blowout_threshold_count":
            m_Job.setBlowoutThresHoldCount(int(p_ParameterValue))
        else:
            raise SQLCliException("Invalid JOB Parameter name. [" + str(p_ParameterName) + "]")
        # 回写到保存的结构中
        self.Update_Job(p_jobName, m_Job)

    # 启动JOB
    def startjob(self, p_jobName: str):
        # 将JOB从Submitted变成Runnning
        # 如果输入的参数为all，则启动全部的JOB信息
        nJobStarted = 0
        if p_jobName.lower() == "all":
            m_Jobs = self.Jobs
        else:
            m_Jobs = {p_jobName: self.Get_Job(p_jobName), }
        for Job_Name, Job_Context in m_Jobs.items():
            if Job_Context.getStatus() == "Submitted":
                nJobStarted = nJobStarted + 1
                # 初始化Task列表
                Job_Context.initTaskList()
                # 标记Task已经开始运行
                Job_Context.setStatus("RUNNING")
                # 设置Task运行开始时间
                Job_Context.setStartTime(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())))
                # 将任务信息更新到后台进程信息中
                self.Update_Job(Job_Name, Job_Context)
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
                if not self.isRegistered():
                    # 多任务进程管理没有启动，也就不可能有RUNNING信息
                    break
                for Job_Name, Job_Context in self.Jobs.items():
                    if Job_Context.getStatus() not in \
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
        if not self.isRegistered():
            # Job Manager没有启动，没有可以停止的JOB
            return 0

        # 如果输入的参数为all，则停止全部的JOB信息
        nJobShutdowned = 0
        if p_jobName.lower() == "all":
            m_Jobs = self.Jobs
        else:
            m_Jobs = {p_jobName: self.Get_Job(p_jobName), }
        for Job_Name, Job_Context in m_Jobs.items():
            if Job_Context.getStatus() in ("RUNNING", "Submitted"):
                nJobShutdowned = nJobShutdowned + 1
                # 标记Task已经开始运行
                Job_Context.setStatus("WAITINGFOR_SHUTDOWN")
                # 将任务信息更新到后台进程信息中
                self.Update_Job(Job_Name, Job_Context)
        return nJobShutdowned

    # 放弃JOB作业
    def abortjob(self, p_jobName: str):
        # 将JOB从Runnning变成waitingfor_abort
        # 如果输入的参数为all，则放弃全部的JOB信息
        nJobAborted = 0
        if p_jobName.lower() == "all":
            m_Jobs = self.Jobs
        else:
            m_Jobs = {p_jobName: self.Get_Job(p_jobName), }
        for Job_Name, Job_Context in m_Jobs.items():
            if Job_Context.getStatus() == "RUNNING":
                nJobAborted = nJobAborted + 1
                # 标记Task已经开始运行
                Job_Context.setStatus("WAITINGFOR_ABORT")
                # 将任务信息更新到后台进程信息中
                self.Update_Job(Job_Name, Job_Context)
        return nJobAborted

    # 判断是否所有有效的子进程都已经退出
    def isJobClosed(self, p_JobName: str):
        m_Job = self.Get_Job(p_JobName)
        if m_Job is None:
            raise SQLCliException("Invalid JOB name. [" + str(p_JobName) + "]")
        return m_Job.getStatus() == "FINISHED"

    # 判断是否所有有效的子进程都已经退出
    def isAllJobClosed(self):
        # 当前有活动进程存在
        return len(self.ProcessInfo) == 0

    # 处理JOB的相关命令
    def Process_Command(self, p_szCommand: str):
        m_szSQL = p_szCommand.strip()

        # 创建新的JOB
        matchObj = re.match(r"job\s+create\s+(.*)(\s+)?$",
                            m_szSQL, re.IGNORECASE | re.DOTALL)
        if matchObj:
            m_ParameterList = str(matchObj.group(1)).strip().split()
            # 第一个参数是JOBNAME
            m_JobName = m_ParameterList[0].strip()
            self.createjob(m_JobName)
            m_ParameterList = m_ParameterList[1:]
            for m_nPos in range(0, len(m_ParameterList) // 2):
                m_ParameterName = m_ParameterList[2*m_nPos]
                m_ParameterValue = m_ParameterList[2 * m_nPos + 1]
                self.setjob(m_JobName, m_ParameterName, m_ParameterValue)
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
