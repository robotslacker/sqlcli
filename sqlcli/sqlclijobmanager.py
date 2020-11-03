# -*- coding: utf-8 -*-
import re
import threading
import ctypes
import inspect
from multiprocessing import Process, Lock
from multiprocessing.managers import BaseManager

from .sqlcliexception import SQLCliException
from .sqlclisga import SQLCli_GlobalSharedMemory
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
        self.manager.register('SQLCli_GlobalSharedMemory', callable=SQLCli_GlobalSharedMemory)

        # 守护线程，用来启动服务，清理超时作业等
        self.Agenthread = None

        # 所有后台进程
        self.BackGroundJobHandler = None

    # 返回是否已经注册到共享服务器
    def isRegistered(self):
        return self._isRegistered

    # 后台守护线程，跟踪进程信息，启动或强制关闭进程
    @staticmethod
    def JOBManagerAgent():
        'dddd'
        pass

    # 向线程发出一个终止信号，等待线程退出
    def _async_raise(self, tid, exctype):
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
        func = getattr(self.manager, 'SQLCli_GlobalSharedMemory')
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

        # 初始化共享服务器
        if not self.isRegistered():
            self.register()

        # 对共享的进程状态信息加锁
        self.LOCK_JOBCATALOG.acquire()

        # 将任务添加到后台进程信息中
        self.BackGroundJobHandler.Update_Job(p_szname, m_Job)

        # 任务不需要启动，对共享的进程状态信息解锁
        self.LOCK_JOBCATALOG.release()

    # 显示当前所有已经提交的任务信息
    def showjobs(self, p_szjobName: str):
        """
            job_name status active_jobs failed_jobs finished_jobs submit_time start_time end_time 

            max_transaction_time  avg_transaction_time min__transaction_time,
            parallel, starter_maxprocess, loop, script, script_fullname, think_time, timeout,
            shutdown_mode, fail_mode, blowout_threshold_percent, blowout_threshold_count
        """
        # 如果输入的参数为all，则显示全部的JOB信息
        if p_szjobName.upper() == "all":
            m_Jobs = self.BackGroundJobHandler.Get_Jobs()
        else:
            m_Jobs = {p_szjobName : self.BackGroundJobHandler.Get_Job(p_szjobName)}
        m_Header = ["job_name", "status", "active_jobs", "failed_jobs", "finished_jobs", 
                "submit_time", "start_time", "end_time", "max_transaction_time", 
                "avg_transaction_time", "min__transaction_time"]
        m_Result = []
        for Job_Name, Job_Context in m_Jobs.items():
            m_Result.append([Job_Name, Job_Context.Status, ])

    # 处理JOB的相关命令
    def Process_Command(self, p_szCommand):
        m_szSQL = p_szCommand.strip()

        # 创建新的JOB
        matchObj = re.match(r"job\s+create\s+(.*)$",
                            m_szSQL, re.IGNORECASE | re.DOTALL)
        if matchObj:
            m_JobName = str(matchObj.group(1)).strip()
            self.createjob(m_JobName)
            return None, None, None, None, "JOB [" + m_JobName + "] create successful!"

        # 显示当前的JOB
        matchObj = re.match(r"job\s+show\s+(.*)?$",
                            m_szSQL, re.IGNORECASE | re.DOTALL)
        if matchObj:
            m_JobName = str(matchObj.group(1)).strip()
            self.showjobs(m_JobName)
            return None, None, None, None, "JOB [" + m_JobName + "] create successful!"

        raise SQLCliException("Invalid JOB Command.")