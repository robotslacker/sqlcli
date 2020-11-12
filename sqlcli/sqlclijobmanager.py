# -*- coding: utf-8 -*-
import re
import threading
import ctypes
import inspect
import time
from multiprocessing import Lock
from multiprocessing.managers import BaseManager

from .sqlcliexception import SQLCliException
from .sqlclisga import SQLCliGlobalSharedMemory
from .sqlclijob import JOB


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

        # 守护线程，用来启动服务，清理超时作业等
        self.Agenthread = None

        # 所有后台进程
        self.BackGroundJobHandler = None

    # 返回是否已经注册到共享服务器
    def isRegistered(self):
        return self._isRegistered

    # 后台守护线程，跟踪进程信息，启动或强制关闭进程
    def JOBManagerAgent(self):
        while True:
            m_Jobs = self.BackGroundJobHandler.Get_Jobs()
            for Job_Name, Job_Context in m_Jobs.items():
                # 如果有Accepted状态，且符合Starter要求的，标记为Starting
                if Job_Context.getStatus() == "Accepted":
                    if Job_Context.getActiveJobs() < Job_Context.getParallel():
                        # 当前不是满载
                        if Job_Context.getStarterLastActiveTime() is None:
                            # 系统第一次启动
                            pass
                    # ActiveJobs < Parallel.
                    # LastStarterTime + Starter_Interval < now
                    # 创建Task

                    pass

            # 如果有Starting状态，则启动

            # 如果有WAITING_SHUTDOWN的，则不再启动，当没有活动进程的时候，标记为CLOSED

            # 如果有WAITING_ABORT的，则杀掉进程

            # 如果已经全部完成，标记为Finished

            # 如果有Timeout的，则Abort

            # 如果还有没完成的，且符合Think_Time约定的，启动它

            time.sleep(10)

    # 向线程发出一个终止信号，等待线程退出
    @staticmethod
    def _async_raise(tid, exctype):
        """raises the exception, performs cleanup if needed"""
        tid = ctypes.c_long(tid)
        if not inspect.isclass(exctype):
            exctype = type(exctype)
        res = ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, ctypes.py_object(exctype))
        if res == 0:
            raise ValueError("invalid thread id")
        elif res != 1:
            # """if it returns a number greater than one, you're in trouble,
            # and you should call it again with exc=NULL to revert the effect"""
            ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, None)

    # 退出指定的线程
    def stop_thread(self, thread):
        self._async_raise(thread.ident, SystemExit)

    # 返回进程信息到共享服务器，来满足进程并发使用
    def register(self):
        # 注册后台共享进程管理
        self.manager.start()
        func = getattr(self.manager, 'SQLCliGlobalSharedMemory')
        self.BackGroundJobHandler = func()

        # 启动后台守护线程，用来处理延时启动，超时等问题
        self.Agenthread = threading.Thread(target=self.JOBManagerAgent)
        self.Agenthread.setDaemon(True)  # 主进程退出，守护进程也会退出
        self.Agenthread.setName("JobManagerAgent")
        self.Agenthread.start()

        # 标记状态信息
        self._isRegistered = True

    # 退出共享服务器
    def unregister(self):
        # 当前进程关闭后退出RPC的Manager, 关闭守护线程
        self.manager.shutdown()
        self._isRegistered = False
        self.stop_thread(self.Agenthread)
        self.Agenthread = None

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
        m_Job.setJobID(self.BackGroundJobHandler.getJobID())

        # 将任务添加到后台进程信息中
        self.BackGroundJobHandler.Update_Job(p_szname, m_Job)

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
            m_Jobs = self.BackGroundJobHandler.Get_Jobs()
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
            m_Job = self.BackGroundJobHandler.Get_Job(p_szjobName)
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
            strMessages = strMessages + 'Think time: [{0:10d}]; Timeout: [{1:10d}]; Elapsed: [{2:10d}]\n'.\
                format(m_Job.getThinkTime(), m_Job.getTimeOut(), m_ElapsedTime)
            strMessages = strMessages + 'Shutdown MODE: [{0:19}]; FAIL_MODE: [{1:19}]\n'.\
                format(m_Job.getShutdownMode(), m_Job.getFailMode())
            strMessages = strMessages + 'Blowout Threshold Percent%/Count: [{0:16d}%/{1:16d}]\n'.\
                format(m_Job.getBlowoutThresHoldPrecent(), m_Job.getBlowoutThresHoldCount())
            return None, None, None, None, strMessages

    # 设置JOB的各种参数
    def setjob(self, p_jobName: str, p_ParameterName: str, p_ParameterValue: str):
        m_Job = self.BackGroundJobHandler.Get_Job(p_jobName)
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
        self.BackGroundJobHandler.Update_Job(p_jobName, m_Job)
        self.LOCK_JOBCATALOG.release()   # 对共享的进程状态信息解锁

    # 启动JOB
    def startjob(self, p_jobName: str):
        # 将JOB从Submitted变成Accepted
        # 日后可以考虑资源等设置信息

        # 如果输入的参数为all，则启动全部的JOB信息
        nJobStarted = 0
        if p_jobName.lower() == "all":
            m_Jobs = self.BackGroundJobHandler.Get_Jobs()
        else:
            m_Jobs = {p_jobName: self.BackGroundJobHandler.Get_Job(p_jobName), }
        for Job_Name, Job_Context in m_Jobs.items():
            if Job_Context.getStatus() == "Submitted":
                nJobStarted = nJobStarted + 1
                self.LOCK_JOBCATALOG.acquire()  # 对共享的进程状态信息加锁
                Job_Context.setStatus("Accepted")
                # 将任务添加到后台进程信息中
                self.BackGroundJobHandler.Update_Job(Job_Name, Job_Context)
                self.LOCK_JOBCATALOG.release()  # 对共享的进程状态信息解锁

        return nJobStarted

    # 判断是否所有有效的子进程都已经退出
    def isAllJobClosed(self):
        # 如果还没有初始化BackGroundJobHandler，自然也没有相关作业
        if self.BackGroundJobHandler is None:
            return True
        m_Jobs = self.BackGroundJobHandler.Get_Jobs()
        for Job_Name, Job_Context in m_Jobs.items():
            if not Job_Context.getStatus() in ["Submitted", "Finished", "Stopped", "Killed"]:
                return False
        return True

    # 处理JOB的相关命令
    def Process_Command(self, p_szCommand):
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

        raise SQLCliException("Invalid JOB Command [" + m_szSQL + "]")
