# -*- coding: utf-8 -*-
import os
import sys
import threading
import traceback
from io import open
import jaydebeapi
import jpype
import re
import time
from multiprocessing import Process, Lock
from multiprocessing.managers import BaseManager
import setproctitle
import shlex
import copy
from cli_helpers.tabular_output import TabularOutputFormatter, preprocessors
import click
from prompt_toolkit.shortcuts import PromptSession

from .sqlexecute import SQLExecute
from .sqlparse import SQLMapping
from .kafkawrapper import KafkaWrapper
from .sqlcliexception import SQLCliException

from .sqlinternal import Create_file
from .sqlinternal import Convert_file
from .sqlinternal import Create_SeedCacheFile
from .commandanalyze import register_special_command
from .commandanalyze import CommandNotFound
from .__init__ import __version__
from .sqlparse import SQLAnalyze

import itertools

click.disable_unicode_literals_warning = True

PACKAGE_ROOT = os.path.abspath(os.path.dirname(__file__))
# 进程锁, 用来控制对BackGround_Jobs的修改
LOCK_JOBCATALOG = Lock()


# BackGroundJobs 是一个共享结构，在当前主进程和随后的子进程之间共享
class BackGroundJobs(object):
    m_Jobs = []

    # 返回所有的后台JOB信息
    def Get_Jobs(self):
        return self.m_Jobs

    # 返回指定的JOB信息，根据JOBID来判断
    def Get_Job(self, p_szJObID):
        # 返回指定的Job信息，如果找不到，返回None
        for m_nPos in range(0, len(self.m_Jobs)):
            if str(self.m_Jobs[m_nPos]["JOB#"]) == str(p_szJObID):
                return self.m_Jobs[m_nPos]
        return None

    # 根据JOBID设置指定的JOB信息
    def Set_Job(self, p_szJObID, p_objJob):
        # 返回指定的Job信息，如果找不到，返回None
        for m_nPos in range(0, len(self.m_Jobs)):
            if str(self.m_Jobs[m_nPos]["JOB#"]) == str(p_szJObID):
                self.m_Jobs[m_nPos] = copy.copy(p_objJob)
                return True
        return False

    # 根据JOBID设置指定的JOB状态
    def Set_JobStatus(self, p_szJObID, p_szStatus):
        # 返回指定的Job信息，如果找不到，返回None
        for m_nPos in range(0, len(self.m_Jobs)):
            if str(self.m_Jobs[m_nPos]["JOB#"]) == str(p_szJObID):
                self.m_Jobs[m_nPos]["Status"] = p_szStatus
                return True
        return False

    # 在队列中追加一个JOB信息
    def Append_Job(self, p_objJob):
        self.m_Jobs.append(p_objJob)


class SQLCli(object):
    jar_file = None

    # 数据库连接的各种参数
    db_url = None
    db_username = None
    db_password = None
    db_type = None
    db_driver_type = None
    db_host = None
    db_port = None
    db_service_name = None

    # SQLCli的初始化参数
    logon = None
    logfilename = None
    sqlscript = None
    sqlmap = None
    nologo = None

    # 目前程序运行的当前SQL
    m_Current_RunningSQL = None
    m_Current_RunningStarted = None

    # 目前程序的终止状态
    m_Current_RunningStatus = None

    # 程序停止标志
    m_Shutdown_Flag = False             # 在程序的每个语句，Sleep间歇中都会判断这个标志

    # 后台进程队列
    m_BackGround_Jobs = None
    m_Max_JobID = 0                     # 当前的最大JOBID

    # 屏幕输出
    Console = None                      # 程序的控制台显示
    logfile = None                      # 程序输出日志文件
    HeadlessMode = False                # 没有显示输出，即不需要回显，用于子进程的显示
    logger = None                       # 程序的输出日志
    m_SQLPerf = None                    # SQL日志输出

    def __init__(
            self,
            logon=None,
            logfilename=None,
            sqlscript=None,
            sqlmap=None,
            nologo=None,
            breakwitherror=False,
            sqlperf=None,
            Console=sys.stdout,
            HeadlessMode=False,
            WorkerName=None,
            logger=None
    ):
        self.m_ProcessList = []                   # 所有本进程启动的子进程句柄信息
        self.db_saved_conn = {}                   # 数据库Session备s份
        self.SQLMappingHandler = SQLMapping()     # 函数句柄，处理SQLMapping信息
        self.SQLExecuteHandler = SQLExecute()     # 函数句柄，具体来执行SQL
        self.KafkaHandler = KafkaWrapper()        # 函数句柄，处理Kafka的消息
        self.prompt_app = None                    # PromptKit控制台
        self.db_conn = None                       # 当前应用的数据库连接句柄
        if WorkerName is None:
            self.m_Worker_Name = "0"              # 当前进程名称. 对于主进程是：MAIN, 对于子进程是:JOBID-Copies-FinishedCount
        else:
            self.m_Worker_Name = WorkerName       # 当前进程名称. 如果有参数传递，以参数为准

        # 传递各种参数
        self.sqlscript = sqlscript
        self.sqlmap = sqlmap
        self.nologo = nologo
        self.logon = logon
        self.logfilename = logfilename
        self.Console = Console
        self.HeadlessMode = HeadlessMode
        self.m_SQLPerf = sqlperf
        if HeadlessMode:
            HeadLessConsole = open(os.devnull, "w")
            self.Console = HeadLessConsole
        self.logger = logger

        # 设置其他的变量
        self.SQLExecuteHandler.sqlscript = sqlscript
        self.SQLExecuteHandler.SQLMappingHandler = self.SQLMappingHandler
        self.SQLExecuteHandler.logfile = self.logfile
        self.SQLExecuteHandler.Console = self.Console
        self.SQLExecuteHandler.SQLPerfFile = self.m_SQLPerf
        self.SQLExecuteHandler.logger = self.logger
        self.SQLMappingHandler.Console = self.Console

        # 默认的输出格式
        self.formatter = TabularOutputFormatter(format_name='ascii')
        self.formatter.sqlcli = self
        self.syntax_style = 'default'
        self.output_style = None

        # 加载一些特殊的命令
        self.register_special_commands()

        # 设置进程的名称
        self.set_Worker_Name(self.m_Worker_Name)

        # 设置WHENEVER_SQLERROR
        if breakwitherror:
            self.SQLExecuteHandler.options["WHENEVER_SQLERROR"] = "EXIT"

    def set_Worker_Name(self, p_szWorkerName):
        self.m_Worker_Name = p_szWorkerName
        self.SQLExecuteHandler.set_Worker_Name(p_szWorkerName)

    def register_special_commands(self):

        # 加载数据库驱动
        register_special_command(
            self.load_driver,
            command="loaddriver",
            description="load JDBC driver .",
            hidden=False
        )

        # 加载SQL映射文件
        register_special_command(
            self.load_sqlmap,
            command="loadsqlmap",
            description="load SQL Mapping file .",
            hidden=False
        )

        # 连接数据库
        register_special_command(
            handler=self.connect_db,
            command="connect",
            description="Connect to database .",
            hidden=False
        )

        # 连接数据库
        register_special_command(
            handler=self.session_manage,
            command="session",
            description="Manage connect sessions.",
            hidden=False
        )

        # 断开连接数据库
        register_special_command(
            handler=self.disconnect_db,
            command="disconnect",
            description="Disconnect database .",
            hidden=False
        )

        # 从文件中执行脚本
        register_special_command(
            self.execute_from_file,
            command="start",
            description="Execute commands from file.",
            hidden=False
        )

        # sleep一段时间
        register_special_command(
            self.sleep,
            command="sleep",
            description="Sleep some time (seconds)",
            hidden=False
        )

        # 设置各种参数选项
        register_special_command(
            self.set_options,
            command="set",
            description="set options .",
            hidden=False
        )

        # 提交后台SQL任务
        register_special_command(
            self.submit_job,
            command="Submitjob",
            description="Submit Jobs",
            hidden=False
        )

        # 开始运行后台SQL任务
        register_special_command(
            self.startjob,
            command="StartJob",
            description="Start Jobs",
            hidden=False
        )

        # 立即关闭后台SQL任务
        register_special_command(
            self.shutdown_job,
            command="shutdownjob",
            description="Shutdown Jobs",
            hidden=False
        )

        # 平和关闭后台SQL任务
        register_special_command(
            self.close_job,
            command="closejob",
            description="Close Jobs",
            hidden=False
        )

        # 等待JOB完成
        register_special_command(
            self.wait_job,
            command="waitjob",
            description="Wait Job complete",
            hidden=False
        )

        # 强制关闭后台SQL任务
        register_special_command(
            self.abort_job,
            command="abortjob",
            description="Abort Jobs",
            hidden=False
        )

        # 查看各种信息
        register_special_command(
            self.show_job,
            command="showjob",
            description="show informations",
            hidden=False
        )

        # 执行特殊的命令
        register_special_command(
            self.execute_internal_command,
            command="__internal__",
            description="execute internal command.",
            hidden=False
        )

        # 退出当前应用程序
        register_special_command(
            self.exit,
            command="exit",
            description="Exit program.",
            hidden=False
        )

    # 设置停止标志。 在程序Sleep间隙或者语句执行间隙，如果收到Shutdown标志，就会停止程序继续运行
    def Shutdown(self):
        self.SQLExecuteHandler.m_Shutdown_Flag = True
        self.m_Shutdown_Flag = True

    # 退出当前应用程序
    def exit(self, arg, **_):
        if arg is not None and len(arg) != 0:
            raise SQLCliException("Unnecessary parameter. Use exit.")

        # 没有后台作业
        if self.m_BackGround_Jobs is None:
            raise EOFError

        # 如果运行在脚本模式下，则一直等待子进程退出后再退出
        if self.sqlscript is not None:
            while True:
                m_ExitStauts = True
                # 如果后面还有需要的作业完成没有完成，拒绝退出应用程序
                for m_Process in self.m_ProcessList:
                    if m_Process["ProcessHandle"].is_alive():
                        m_ExitStauts = False
                        break
                if m_ExitStauts:
                    # 所有子进程都已经退出了
                    break
                time.sleep(3)

        # 检查是否所有进程都已经退出
        m_ExitStauts = True
        # 如果后面还有需要的作业完成没有完成，拒绝退出应用程序
        for m_Process in self.m_ProcessList:
            if m_Process["ProcessHandle"].is_alive():
                m_ExitStauts = False
                break

        # 正常退出程序
        if m_ExitStauts:
            raise EOFError
        else:
            yield (
                None,
                None,
                None,
                "Please wait all background process complete.")

    # 加载JDBC驱动文件
    def load_driver(self, arg, **_):
        if arg is None or len(str(arg)) == 0:
            raise SQLCliException("Missing required argument, load [driver file name] [driver class name].")
        else:
            load_parameters = str(arg).split()
            m_DriverClassName = None
            m_DatabaseType = None
            if len(load_parameters) > 1:
                m_DriverClassName = str(load_parameters[1]).strip()
            # 首先尝试，绝对路径查找这个文件
            # 如果没有找到，尝试从脚本所在的路径开始查找
            if not os.path.exists(str(load_parameters[0])):
                if self.sqlscript is not None:
                    if os.path.exists(os.path.join(os.path.dirname(self.sqlscript), str(load_parameters[0]))):
                        m_JarFullFileName = os.path.join(os.path.dirname(self.sqlscript), str(load_parameters[0]))
                        m_JarBaseFileName = os.path.basename(m_JarFullFileName)
                    else:
                        raise SQLCliException("driver file [" + str(load_parameters[0]) + "] does not exist.")
                else:
                    # 用户在Console上输入，如果路径信息不全，则放弃
                    raise SQLCliException("driver file [" + str(load_parameters[0]) + "] does not exist.")
            else:
                m_JarFullFileName = os.path.abspath(str(load_parameters[0]))
                m_JarBaseFileName = os.path.basename(m_JarFullFileName)
                if m_JarBaseFileName.startswith("linkoopdb"):
                    m_DatabaseType = "linkoopdb"
                elif m_JarBaseFileName.startswith("mysql"):
                    m_DatabaseType = "mysql"
                elif m_JarBaseFileName.startswith("postgresql"):
                    m_DatabaseType = "postgresql"
                elif m_JarBaseFileName.startswith("sqljdbc"):
                    m_DatabaseType = "sqlserver"
                elif m_JarBaseFileName.startswith("ojdbc"):
                    m_DatabaseType = "oracle"
                elif m_JarBaseFileName.startswith("terajdbc"):
                    m_DatabaseType = "teradata"
                elif m_JarBaseFileName.startswith("tdgssconfig"):
                    m_DatabaseType = "teradata-gss"
                elif m_JarBaseFileName.startswith("clickhouse"):
                    m_DatabaseType = "clickhouse"
                else:
                    m_DatabaseType = "UNKNOWN"

            # 将jar包信息添加到self.jar_file中
            if self.jar_file:
                bExitOldConfig = False
                for m_nPos in range(0, len(self.jar_file)):
                    if self.jar_file[m_nPos]["JarName"] == m_JarBaseFileName:
                        m_Jar_Config = self.jar_file[m_nPos]
                        m_Jar_Config["FullName"] = m_JarFullFileName
                        self.jar_file[m_nPos] = m_Jar_Config
                        bExitOldConfig = True
                        break
                if not bExitOldConfig:
                    m_Jar_Config = {"JarName": m_JarBaseFileName,
                                    "FullName": m_JarFullFileName,
                                    "ClassName": m_DriverClassName,
                                    "Database": m_DatabaseType}
                    self.jar_file.append(m_Jar_Config)
            else:
                self.jar_file = []
                m_Jar_Config = {"JarName": m_JarBaseFileName,
                                "FullName": m_JarFullFileName,
                                "ClassName": m_DriverClassName,
                                "Database": m_DatabaseType}
                self.jar_file.append(m_Jar_Config)

        yield (
            None,
            None,
            None,
            'Driver loaded.'
        )

    # 加载数据库SQL映射
    def load_sqlmap(self, arg, **_):
        self.SQLExecuteHandler.options["SQLREWRITE"] = "ON"
        self.SQLMappingHandler.Load_SQL_Mappings(self.sqlscript, arg)
        self.sqlmap = arg
        yield (
            None,
            None,
            None,
            'Mapping file loaded.'
        )

    # 等待JOB完成
    def wait_job(self, arg, **_):
        if arg is None or len(str(arg).strip()) == 0:
            raise SQLCliException("Missing required argument. waitjob [all|job#].")
        m_Parameters = str(arg).split()
        m_nLoopCount = 0
        if m_Parameters[0].upper() == "ALL":
            # 等待所有的JOB完成
            while True:
                m_bFoundJob = False
                for m_Process in self.m_ProcessList:
                    if m_Process["ProcessHandle"].is_alive():
                        m_bFoundJob = True
                if not m_bFoundJob:
                    yield (
                        None,
                        None,
                        None,
                        "All Job Completed")
                    return
                else:
                    m_nLoopCount = m_nLoopCount + 1
                    time.sleep(3)
                    if m_nLoopCount == 10:
                        m_nLoopCount = 0
                        if len(m_Parameters) == 2 and m_Parameters[1].upper() == "SHOW":
                            self.DoSQL("showjob all")
        elif m_Parameters[0].upper().isnumeric():
            # 显示指定的JOB信息
            m_Job_ID = int(m_Parameters[0].upper())
            while True:
                m_bFoundJob = False
                for m_Process in self.m_ProcessList:
                    if m_Process["JOB#"] == m_Job_ID:
                        m_bFoundJob = True
                        if not m_Process["ProcessHandle"].is_alive():
                            yield (
                                None,
                                None,
                                None,
                                "Job " + str(m_Job_ID) + " Completed")
                            return
                    else:
                        continue

                # 继续等待
                if not m_bFoundJob:
                    yield (
                        None,
                        None,
                        None,
                        "No job to wait.")
                    return
                else:
                    m_nLoopCount = m_nLoopCount + 1
                    time.sleep(3)
                    if m_nLoopCount == 10:
                        m_nLoopCount = 0
                        if len(m_Parameters) == 2 and m_Parameters[1].upper() == "SHOW":
                            self.DoSQL("showjob " + str(m_Job_ID))
                    continue
        else:
            raise SQLCliException("Argument error. waitjob [all|job#].")

    # 显示当前正在执行的JOB
    def show_job(self, arg, **_):
        if arg is None or len(str(arg).strip()) == 0:
            raise SQLCliException("Missing required argument. showjob [all|job#].")
        m_Parameters = str(arg).split()

        if m_Parameters[0].upper() == "ALL":
            # show jobs
            if self.m_BackGround_Jobs is None:
                yield (
                    None,
                    None,
                    None,
                    "No Jobs")
                return

            m_Result = []
            for m_BackGround_Job in self.m_BackGround_Jobs.Get_Jobs():
                m_Result.append(
                    [
                        str(m_BackGround_Job["JOB#"]),
                        m_BackGround_Job["ScriptBaseName"],
                        m_BackGround_Job["Status"],
                        m_BackGround_Job["StartedTime"],
                        m_BackGround_Job["EndTime"],
                        m_BackGround_Job["Finished"],
                        m_BackGround_Job["LoopCount"],
                    ]
                )
            yield (
                None,
                m_Result,
                ["JOB#", "ScriptBaseName", "Status", "Started", "End", "Finished", "LoopCount"],
                "Total " + str(self.m_Max_JobID) + " Jobs.")
            return

        if m_Parameters[0].upper().isnumeric():
            m_Job_ID = int(m_Parameters[0].upper())
        else:
            raise SQLCliException("Argument error. showjob [job#].")

        # 遍历JOB，找到需要的那条信息
        m_Result = ""
        for m_BackGround_Job in self.m_BackGround_Jobs.Get_Jobs():
            if int(m_BackGround_Job["JOB#"]) == m_Job_ID:
                if m_BackGround_Job["Current_SQL"] is None:
                    m_Current_SQL = "[None]"
                else:
                    m_Current_SQL = "\n" + str(m_BackGround_Job["Current_SQL"]) + "\n"
                m_Result = "Job Describe [" + str(m_Job_ID) + "]\n" + \
                           "  ScriptBaseName = [" + m_BackGround_Job["ScriptBaseName"] + "]\n" + \
                           "  ScriptFullName = [" + m_BackGround_Job["ScriptFullName"] + "]\n" + \
                           "  Status = [" + m_BackGround_Job["Status"] + "]\n" + \
                           "  StartedTime = [" + str(m_BackGround_Job["StartedTime"]) + "]\n" + \
                           "  EndTime = [" + str(m_BackGround_Job["EndTime"]) + "]\n" + \
                           "  Finished = [" + str(m_BackGround_Job["Finished"]) + "]\n" + \
                           "  LoopCount = [" + str(m_BackGround_Job["LoopCount"]) + "]\n" + \
                           "  Current_SQL = " + m_Current_SQL
            else:
                continue
        yield (
            None,
            None,
            None,
            m_Result)
        return

    # 单独的进程运行指定的一个JOB
    @staticmethod
    def runJob(p_args, p_Job):
        def runJobInThread(p_SQLCli):
            p_SQLCli.run_cli()

        # 对共享的进程状态信息加锁
        LOCK_JOBCATALOG.acquire()

        m_JobID = str(p_args["JOB#"])
        m_CurrentJob = p_Job.Get_Job(p_args["JOB#"])
        if m_CurrentJob is None:
            print("JOB [" + str(p_args["JOB#"]) + "] does not exist!")
            return

        # 设置进程的标题，随后开始运行程序
        setproctitle.setproctitle('SQLCli Worker:' + str(m_JobID) + " Script:" + str(m_CurrentJob["ScriptBaseName"]))

        # 标记JOB开始时间
        if m_CurrentJob["Status"] != "Starting":
            # 任务不需要启动，对共享的进程状态信息解锁
            LOCK_JOBCATALOG.release()
            return

        m_CurrentJob["Status"] = "RUNNING"
        m_CurrentJob["PID"] = os.getpid()
        m_CurrentLoop = int(m_CurrentJob["Finished"])
        p_Job.Set_Job(m_JobID, m_CurrentJob)

        # 对共享的进程状态信息解锁
        LOCK_JOBCATALOG.release()

        while True:           # 循环执行JOB，考虑有可能JOB需要重复多次
            # 启动Worker线程
            # 默认的logfilename和当前文件的logfile文件目录一致
            # 如果运行在终端模式下，则在当前目录下生成日志文件
            if p_args["logfilename"] is not None:
                m_logfilename = os.path.join(
                    os.path.dirname(p_args["logfilename"]),
                    m_CurrentJob["ScriptBaseName"].split('.')[0] + "_" + m_JobID + "-" + str(m_CurrentLoop) + ".log")
            else:
                m_logfilename = \
                    m_CurrentJob["ScriptBaseName"].split('.')[0] + "_" + m_JobID + "-" + str(m_CurrentLoop) + ".log"
            HeadLessConsole = open(os.devnull, "w")
            m_SQLCli = SQLCli(
                sqlscript=m_CurrentJob["ScriptFullName"],
                logon=p_args["logon"],
                logfilename=m_logfilename,
                sqlmap=p_args["sqlmap"],
                nologo=p_args["nologo"],
                breakwitherror=p_args["breakwitherror"],
                Console=HeadLessConsole,
                HeadlessMode=True,
                sqlperf=p_args["sqlperf"]
            )
            m_SQLCli.set_Worker_Name(
                str(p_args["Parent_WorkerName"])+":"+str(m_JobID) + "-" +
                str(m_CurrentJob["Copies"]) + "-" +
                str(m_CurrentJob["Finished"]))
            m_WorkerThread = threading.Thread(target=runJobInThread, args=(m_SQLCli,))
            m_WorkerThread.start()

            # 循环检查Worker的状态
            m_OldCurrentSQL = ""
            while m_WorkerThread.is_alive():
                # 对共享的进程状态信息加锁
                LOCK_JOBCATALOG.acquire()

                # 如果要求程序停止，则设置停止标志
                if p_Job.Get_Job(m_JobID)["Status"] == "WAITINGFOR_SHUTDOWN":
                    m_SQLCli.Shutdown()        # 标记Shutdown标记
                    m_CurrentJob["Status"] = "SHUTDOWNING"
                    p_Job.Set_Job(m_JobID, m_CurrentJob)
                if p_Job.Get_Job(m_JobID)["Status"] == "WAITINGFOR_CLOSE":
                    m_CurrentJob["Status"] = "CLOSING"
                    p_Job.Set_Job(m_JobID, m_CurrentJob)
                # 更新Current_SQL
                m_CurrentSQL = m_SQLCli.m_Current_RunningSQL
                if m_OldCurrentSQL != m_CurrentSQL:
                    m_CurrentJob["Current_SQL"] = m_CurrentSQL
                    m_OldCurrentSQL = m_CurrentSQL
                    p_Job.Set_Job(m_JobID, m_CurrentJob)

                # 对共享的进程状态信息解锁
                LOCK_JOBCATALOG.release()

                # 3秒后再检查
                time.sleep(3)

            # 清理僵死进程
            m_WorkerThread.join()

            # 任务已经完成，检查Loop次数
            m_CurrentLoopCount = int(m_CurrentJob["Finished"])
            m_ToDoLoopCount = int(m_CurrentJob["LoopCount"])
            m_CurrentJob["Finished"] = str(m_CurrentLoopCount + 1)
            m_SQLCli.set_Worker_Name(str(m_JobID) + "-" +
                                     str(m_CurrentJob["Copies"]) + "-" +
                                     str(m_CurrentJob["Finished"]))
            if (m_CurrentLoopCount + 1) >= m_ToDoLoopCount:              # 所有任务都已经完成
                # 对共享的进程状态信息加锁
                LOCK_JOBCATALOG.acquire()

                m_CurrentJob["EndTime"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
                m_CurrentJob["Status"] = "STOPPED"
                p_Job.Set_Job(m_JobID, m_CurrentJob)

                # 对共享的进程状态信息解锁
                LOCK_JOBCATALOG.release()
                break                          # 运行已经全部结束
            else:
                # 如果已经关闭，则直接退出，标志CLOSED
                if m_CurrentJob["Status"] == "CLOSING":
                    # 对共享的进程状态信息加锁
                    LOCK_JOBCATALOG.acquire()

                    m_CurrentJob["EndTime"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
                    m_CurrentJob["Status"] = "CLOSED"
                    p_Job.Set_Job(m_JobID, m_CurrentJob)

                    # 对共享的进程状态信息解锁
                    LOCK_JOBCATALOG.release()
                    break  # 运行已经全部结束

                # 如果已经关闭，则直接退出，标志SHUTDOWN
                if m_CurrentJob["Status"] == "SHUTDOWNING":
                    # 对共享的进程状态信息加锁
                    LOCK_JOBCATALOG.acquire()

                    m_CurrentJob["EndTime"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
                    m_CurrentJob["Status"] = "SHUTDOWN"
                    p_Job.Set_Job(m_JobID, m_CurrentJob)

                    # 对共享的进程状态信息解锁
                    LOCK_JOBCATALOG.release()
                    break  # 运行已经全部结束

                # 继续新的一轮运行
                m_CurrentLoop = m_CurrentLoop + 1

                # 修改线程的WorkerName，标记当前循环次数和并发数

                continue                       # 继续当前JOB的下一次运行

    # 启动后台SQL任务
    def startjob(self, arg, **_):
        if arg is None or len(str(arg).strip()) == 0:
            raise SQLCliException("Missing required argument. startjob [job#|all].")

        # 对共享的进程状态信息加锁
        LOCK_JOBCATALOG.acquire()

        # 如果参数中有all信息，启动全部的JOB，否则启动输入的JOB
        m_JobLists = []
        m_Parameters = str(arg).split()
        for m_Parameter in m_Parameters:
            if m_Parameter.upper() == 'ALL':
                if self.m_BackGround_Jobs is None:
                    raise SQLCliException("No Jobs to start.")
                m_JobLists.clear()
                for m_BackGround_Job in self.m_BackGround_Jobs.Get_Jobs():
                    if m_BackGround_Job["Status"] in "Not Started":
                        m_JobLists.append(m_BackGround_Job)
                        m_BackGround_Job["StartedTime"] = \
                            time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
                        m_BackGround_Job["Status"] = "Starting"
                        self.m_BackGround_Jobs.Set_Job(m_BackGround_Job["JOB#"], m_BackGround_Job)
                break
            else:
                for m_BackGround_Job in self.m_BackGround_Jobs.Get_Jobs():
                    if str(m_BackGround_Job["JOB#"]) == str(m_Parameter):
                        if m_BackGround_Job["Status"] in "Not Started":
                            m_JobLists.append(m_BackGround_Job)
                            m_BackGround_Job["StartedTime"] = \
                                time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
                            m_BackGround_Job["Status"] = "Starting"
                            self.m_BackGround_Jobs.Set_Job(m_BackGround_Job["JOB#"], m_BackGround_Job)

        # 启动JOB
        for m_Job in m_JobLists:
            m_args = {"JOB#": m_Job["JOB#"], "logon": self.logon, "sqlmap": self.sqlmap, "nologo": self.nologo,
                      "breakwitherror": (self.SQLExecuteHandler.options["WHENEVER_SQLERROR"] == "EXIT"),
                      "logfilename": self.logfilename, "sqlperf": self.m_SQLPerf,
                      "Parent_WorkerName": self.m_Worker_Name,
                      "PID": 0, "exitcode": 0}
            m_Process = Process(target=self.runJob, args=(m_args, self.m_BackGround_Jobs))
            m_Process.start()
            self.m_ProcessList.append({"ProcessHandle": m_Process, "JOB#": m_Job["JOB#"]})

        # 对共享的进程状态信息解锁
        LOCK_JOBCATALOG.release()

        yield (
            None,
            None,
            None,
            str(len(m_JobLists)) + " Jobs Started.")

    # 提交到后台执行SQL
    def submit_job(self, arg, **_):
        if arg is None or len(str(arg).strip()) == 0:
            raise SQLCliException(
                "Missing required argument, SubmitJob <Script Name 1> <Copies> <Loop Count>.")
        m_ParameterLists = str(arg).split()

        m_Script_FileName = m_ParameterLists[0]
        m_Execute_Copies = 1
        m_Script_LoopCount = 1
        if len(m_ParameterLists) > 1:
            if m_ParameterLists[1].isnumeric():
                m_Execute_Copies = int(m_ParameterLists[1])
        if len(m_ParameterLists) > 2:
            if m_ParameterLists[2].isnumeric():
                m_Script_LoopCount = int(m_ParameterLists[2])

        # 命令里头的是全路径名，或者是基于当前目录的相对文件名
        if os.path.isfile(m_Script_FileName):
            m_SQL_ScriptBaseName = os.path.basename(m_Script_FileName)
            m_SQL_ScriptFullName = os.path.abspath(m_Script_FileName)
        else:
            # 从脚本所在的目录开始查找
            if self.sqlscript is None:
                # 并非在脚本模式下
                raise SQLCliException(
                    "File [" + m_Script_FileName + "] does not exist! Submit failed.")
            if os.path.isfile(os.path.join(os.path.dirname(self.sqlscript), m_Script_FileName)):
                m_SQL_ScriptBaseName = \
                    os.path.basename(os.path.join(os.path.dirname(self.sqlscript), m_Script_FileName))
                m_SQL_ScriptFullName = \
                    os.path.abspath(os.path.join(os.path.dirname(self.sqlscript), m_Script_FileName))
            else:
                raise SQLCliException(
                    "File [" + m_Script_FileName + "] does not exist! Submit failed.")

        # 对共享的进程状态信息加锁
        LOCK_JOBCATALOG.acquire()

        # 给所有的Job增加一个编号，随后放入到数组中
        for m_nPos in range(0, m_Execute_Copies):
            self.m_Max_JobID = self.m_Max_JobID + 1
            self.m_BackGround_Jobs.Append_Job(
                {
                    "JOB#": self.m_Max_JobID,
                    "ScriptBaseName": m_SQL_ScriptBaseName,
                    "ScriptFullName": m_SQL_ScriptFullName,
                    "Current_SQL": None,
                    "StartedTime": None,
                    "Status": "Not Started",
                    "EndTime": None,
                    "Logfile": None,
                    "Finished": str(0),
                    "Copies": str(m_Execute_Copies),
                    "LoopCount": str(m_Script_LoopCount)
                }
            )

        # 对共享的进程状态信息解锁
        LOCK_JOBCATALOG.release()

        yield (
            None,
            None,
            None,
            "Jobs Submitted. " +
            m_Script_FileName + " Will execute " +
            str(m_Execute_Copies) + " Copies. Loop " + str(m_Script_LoopCount) + " times."
        )

    # 平和关闭正在运行的进程
    def shutdown_job(self, arg, **_):
        if arg is None or len(str(arg).strip()) == 0:
            raise SQLCliException("Missing required argument. shutdownjob [all|job job_id].")

        # 如果没有后台任务，直接退出
        if self.m_BackGround_Jobs is None:
            yield (
                None,
                None,
                None,
                "No Jobs")
            return

        m_Parameters = str(arg).split()
        m_nCount = 0
        if m_Parameters[0].upper() == "ALL":
            # 对共享的进程状态信息加锁
            LOCK_JOBCATALOG.acquire()

            for m_BackGround_Job in self.m_BackGround_Jobs.Get_Jobs():
                if str(m_BackGround_Job["Status"]) in ("RUNNING", "WAITINGFOR_CLOSE"):
                    m_BackGround_Job["Status"] = "WAITINGFOR_SHUTDOWN"
                    m_nCount = m_nCount + 1
                    self.m_BackGround_Jobs.Set_Job(m_BackGround_Job["JOB#"], m_BackGround_Job)
                if str(m_BackGround_Job["Status"]) in "Not Started":
                    # 对于没有启动的JOB，直接关闭
                    m_BackGround_Job["Status"] = "SHUTDOWN"
                    m_nCount = m_nCount + 1
                    self.m_BackGround_Jobs.Set_Job(m_BackGround_Job["JOB#"], m_BackGround_Job)

            # 对共享的进程状态信息解锁
            LOCK_JOBCATALOG.release()

            yield (
                None,
                None,
                None,
                "Total " + str(m_nCount) + " Jobs will shutdown.")
            return

        # 如果不是closejob all，第一个参数必须是整数的JOB#
        if m_Parameters[0].upper().isnumeric():
            m_Job_ID = int(m_Parameters[0].upper())
        else:
            raise SQLCliException("Argument error. shutdown job [job#].")
        # 遍历JOB，找到需要的那条信息
        m_Result = False
        # 对共享的进程状态信息加锁
        LOCK_JOBCATALOG.acquire()

        for m_BackGround_Job in self.m_BackGround_Jobs.Get_Jobs():
            if int(m_BackGround_Job["JOB#"]) == m_Job_ID:
                if str(m_BackGround_Job["Status"]) in ("RUNNING", "WAITINGFOR_CLOSE"):
                    m_BackGround_Job["Status"] = "WAITINGFOR_SHUTDOWN"
                    self.m_BackGround_Jobs.Set_Job(m_BackGround_Job["JOB#"], m_BackGround_Job)
                    m_Result = True
                if str(m_BackGround_Job["Status"]) in "Not Started":
                    # 对于没有启动的JOB，直接关闭
                    m_BackGround_Job["Status"] = "SHUTDOWN"
                    self.m_BackGround_Jobs.Set_Job(m_BackGround_Job["JOB#"], m_BackGround_Job)
                    m_Result = True
                break

        # 对共享的进程状态信息解锁
        LOCK_JOBCATALOG.release()

        if m_Result:
            yield (
                None,
                None,
                None,
                "Job " + str(m_Job_ID) + " will shutdown.")
        else:
            yield (
                None,
                None,
                None,
                "Job " + str(m_Job_ID) + " does not exist or does not need shutdown.")

    # 强制关闭正在运行的进程
    def abort_job(self, arg, **_):
        if arg is None or len(str(arg).strip()) == 0:
            raise SQLCliException("Missing required argument. abortjob [all|job job_id].")

        # 如果没有后台任务，直接退出
        if self.m_BackGround_Jobs is None:
            yield (
                None,
                None,
                None,
                "No Jobs")
            return

        m_Parameters = str(arg).split()
        if m_Parameters[0].upper() == "ALL":
            # 对共享的进程状态信息加锁
            LOCK_JOBCATALOG.acquire()

            m_nCount = 0
            for m_Process in self.m_ProcessList:
                if m_Process["ProcessHandle"].is_alive():
                    m_Process["ProcessHandle"].terminate()
                    m_Process["ProcessHandle"].join()
                    self.m_BackGround_Jobs.Set_JobStatus(m_Process["JOB#"], "ABORTED")
                    m_nCount = m_nCount + 1

            # 对共享的进程状态信息解锁
            LOCK_JOBCATALOG.release()

            yield (
                None,
                None,
                None,
                "Total " + str(m_nCount) + " Jobs will close.")
            return

        # 如果不是closejob all，第一个参数必须是整数的JOB#
        if m_Parameters[0].upper().isnumeric():
            m_Job_ID = int(m_Parameters[0].upper())
        else:
            raise SQLCliException("Argument error. close job [job#].")
        # 遍历JOB，找到需要的那条信息
        m_Result = False
        # 对共享的进程状态信息加锁
        LOCK_JOBCATALOG.acquire()

        for m_Process in self.m_ProcessList:
            if int(m_Process["JOB#"]) == m_Job_ID:
                if m_Process["ProcessHandle"].is_alive():
                    m_Process["ProcessHandle"].terminate()
                    self.m_BackGround_Jobs.Set_JobStatus(m_Process["JOB#"], "ABORTED")
                    m_Result = True
                break

        # 对共享的进程状态信息解锁
        LOCK_JOBCATALOG.release()

        if m_Result:
            yield (
                None,
                None,
                None,
                "Job " + str(m_Job_ID) + " aborted.")
        else:
            yield (
                None,
                None,
                None,
                "Job " + str(m_Job_ID) + " does not exist or does not need abort.")

    # 平和关闭正在运行的进程
    def close_job(self, arg, **_):
        if arg is None or len(str(arg).strip()) == 0:
            raise SQLCliException("Missing required argument. closejob [all|job job_id].")

        # 如果没有后台任务，直接退出
        if self.m_BackGround_Jobs is None:
            yield (
                None,
                None,
                None,
                "No Jobs")
            return

        m_Parameters = str(arg).split()
        m_nCount = 0
        if m_Parameters[0].upper() == "ALL":
            # 对共享的进程状态信息加锁
            LOCK_JOBCATALOG.acquire()

            for m_BackGround_Job in self.m_BackGround_Jobs.Get_Jobs():
                # 所有RUNNING状态的程序，都标记为WAITINGFOR_STOP
                if str(m_BackGround_Job["Status"]) in "RUNNING":
                    m_BackGround_Job["Status"] = "WAITINGFOR_CLOSE"
                    m_nCount = m_nCount + 1
                    self.m_BackGround_Jobs.Set_Job(m_BackGround_Job["JOB#"], m_BackGround_Job)
                if str(m_BackGround_Job["Status"]) in "Not Started":
                    # 对于没有启动的JOB，直接关闭
                    m_BackGround_Job["Status"] = "CLOSED"
                    m_nCount = m_nCount + 1
                    self.m_BackGround_Jobs.Set_Job(m_BackGround_Job["JOB#"], m_BackGround_Job)

            # 对共享的进程状态信息解锁
            LOCK_JOBCATALOG.release()

            yield (
                None,
                None,
                None,
                "Total " + str(m_nCount) + " Jobs will close.")
            return

        # 如果不是closejob all，第一个参数必须是整数的JOB#
        if m_Parameters[0].upper().isnumeric():
            m_Job_ID = int(m_Parameters[0].upper())
        else:
            raise SQLCliException("Argument error. close job [job#].")
        # 遍历JOB，找到需要的那条信息
        m_Result = False
        # 对共享的进程状态信息加锁
        LOCK_JOBCATALOG.acquire()

        for m_BackGround_Job in self.m_BackGround_Jobs.Get_Jobs():
            if int(m_BackGround_Job["JOB#"]) == m_Job_ID:
                if str(m_BackGround_Job["Status"]) in "RUNNING":
                    m_BackGround_Job["Status"] = "WAITINGFOR_CLOSE"
                    self.m_BackGround_Jobs.Set_Job(m_BackGround_Job["JOB#"], m_BackGround_Job)
                    m_Result = True
                if str(m_BackGround_Job["Status"]) in "Not Started":
                    # 对于没有启动的JOB，直接关闭
                    m_BackGround_Job["Status"] = "CLOSED"
                    self.m_BackGround_Jobs.Set_Job(m_BackGround_Job["JOB#"], m_BackGround_Job)
                    m_Result = True
                break

        # 对共享的进程状态信息解锁
        LOCK_JOBCATALOG.release()

        if m_Result:
            yield (
                None,
                None,
                None,
                "Job " + str(m_Job_ID) + " will close.")
        else:
            yield (
                None,
                None,
                None,
                "Job " + str(m_Job_ID) + " does not exist or does not need close.")

    # 连接数据库
    def connect_db(self, arg, **_):
        if arg is None or len(str(arg)) == 0:
            raise SQLCliException(
                "Missing required argument\n." + "connect [user name]/[password]@" +
                "jdbc:[db type]:[driver type]://[host]:[port]/[service name]")
        elif self.jar_file is None:
            raise SQLCliException("Please load driver first.")

        # 去掉为空的元素
        # connect_parameters = re.split(r'://|:|@|/|\s+', arg)

        m_connect_parameterlist = shlex.shlex(arg)
        m_connect_parameterlist.whitespace = '://|:|@| '
        m_connect_parameterlist.quotes = '"'
        m_connect_parameterlist.whitespace_split = True
        connect_parameters = list(m_connect_parameterlist)
        for m_nPos in range(0, len(connect_parameters)):
            if connect_parameters[m_nPos].startswith('"') and connect_parameters[m_nPos].endswith('"'):
                connect_parameters[m_nPos] = connect_parameters[m_nPos][1:-1]
        if len(connect_parameters) == 8:
            # 指定了所有的数据库连接参数
            self.db_username = connect_parameters[0]
            self.db_password = connect_parameters[1]
            self.db_type = connect_parameters[3]
            self.db_driver_type = connect_parameters[4]
            self.db_host = connect_parameters[5]
            self.db_port = connect_parameters[6]
            self.db_service_name = connect_parameters[7]
            self.db_url = \
                connect_parameters[2] + ':' + connect_parameters[3] + ':' + \
                connect_parameters[4] + '://' + connect_parameters[5] + ':' + \
                connect_parameters[6] + ':/' + connect_parameters[7]
        elif len(connect_parameters) == 2:
            # 用户只指定了用户名和口令， 认为用户和上次保留一直的连接字符串信息
            self.db_username = connect_parameters[0]
            self.db_password = connect_parameters[1]
            if not self.db_url:
                if "SQLCLI_CONNECTION_URL" in os.environ:
                    # 从环境变量里头拼的连接字符串
                    connect_parameters = [var for var in re.split(r'//|:|@|/', os.environ['SQLCLI_CONNECTION_URL']) if
                                          var]
                    if len(connect_parameters) == 6:
                        self.db_type = connect_parameters[1]
                        self.db_driver_type = connect_parameters[2]
                        self.db_host = connect_parameters[3]
                        self.db_port = connect_parameters[4]
                        self.db_service_name = connect_parameters[5]
                        self.db_url = \
                            connect_parameters[0] + ':' + connect_parameters[1] + ':' +\
                            connect_parameters[2] + '://' + connect_parameters[3] + ':' + \
                            connect_parameters[4] + ':/' + connect_parameters[5]
                    else:
                        print("db_type = [" + str(self.db_type) + "]")
                        print("db_host = [" + str(self.db_host) + "]")
                        print("db_port = [" + str(self.db_port) + "]")
                        print("db_service_name = [" + str(self.db_service_name) + "]")
                        print("db_url = [" + str(self.db_url) + "]")
                        raise SQLCliException("Unexpeced env SQLCLI_CONNECTION_URL\n." +
                                              "jdbc:[db type]:[driver type]://[host]:[port]/[service name]")
                else:
                    # 用户第一次连接，而且没有指定环境变量
                    raise SQLCliException("Missed SQLCLI_CONNECTION_URL in env.")
        elif len(connect_parameters) == 4:
            # 用户写法是connect user xxx password xxxx; 密码可能包含引号
            if connect_parameters[0].upper() == "USER" and connect_parameters[2].upper() == "PASSWORD":
                self.db_username = connect_parameters[1]
                self.db_password = connect_parameters[3].replace("'", "").replace('"', "")
                if not self.db_url:
                    if "SQLCLI_CONNECTION_URL" in os.environ:
                        # 从环境变量里头拼的连接字符串
                        connect_parameters = [var for var in re.split(r'//|:|@|/',
                                                                      os.environ['SQLCLI_CONNECTION_URL']) if var]
                        if len(connect_parameters) == 6:
                            self.db_type = connect_parameters[1]
                            self.db_driver_type = connect_parameters[2]
                            self.db_host = connect_parameters[3]
                            self.db_port = connect_parameters[4]
                            self.db_service_name = connect_parameters[5]
                            self.db_url = \
                                connect_parameters[0] + ':' + connect_parameters[1] + ':' +\
                                connect_parameters[2] + '://' + connect_parameters[3] + ':' + \
                                connect_parameters[4] + ':/' + connect_parameters[5]
                        else:
                            print("db_type = [" + str(self.db_type) + "]")
                            print("db_host = [" + str(self.db_host) + "]")
                            print("db_port = [" + str(self.db_port) + "]")
                            print("db_service_name = [" + str(self.db_service_name) + "]")
                            print("db_url = [" + str(self.db_url) + "]")
                            raise SQLCliException("Unexpeced env SQLCLI_CONNECTION_URL\n." +
                                                  "jdbc:[db type]:[driver type]://[host]:[port]/[service name]")
                    else:
                        # 用户第一次连接，而且没有指定环境变量
                        raise SQLCliException("Missed SQLCLI_CONNECTION_URL in env.")
        else:
            # 不知道的参数写法
            if "SQLCLI_DEBUG" in os.environ:
                print("DEBUG:: Connect Str =[" + str(connect_parameters) + "]")
            raise SQLCliException("Missing required argument\n." + "connect [user name]/[password]@" +
                                  "jdbc:[db type]:[driver type]://[host]:[port]/[service name]")

        # 连接数据库
        try:
            # https://github.com/jpype-project/jpype/issues/290
            # jpype bug, when run jpype in multithread
            if jpype.isJVMStarted() and not jpype.isThreadAttachedToJVM():
                jpype.attachThreadToJVM()
                jpype.java.lang.Thread.currentThread().setContextClassLoader(
                    jpype.java.lang.ClassLoader.getSystemClassLoader())

            # 拼接所有的Jar包， jaydebeapi将根据class的名字加载指定的文件
            m_JarList = []
            for m_Jar_Config in self.jar_file:
                m_JarList.append(m_Jar_Config["FullName"])

            if self.db_type.upper() == "ORACLE":
                self.db_conn = jaydebeapi.connect("oracle.jdbc.driver.OracleDriver",
                                                  "jdbc:oracle:thin:@" +
                                                  self.db_host + ":" + self.db_port + "/" + self.db_service_name,
                                                  [self.db_username, self.db_password],
                                                  m_JarList)
            elif self.db_type.upper() == "LINKOOPDB":
                self.db_conn = jaydebeapi.connect("com.datapps.linkoopdb.jdbc.JdbcDriver",
                                                  "jdbc:linkoopdb:tcp://" +
                                                  self.db_host + ":" + self.db_port + "/" + self.db_service_name +
                                                  ";query_iterator=1",
                                                  [self.db_username, self.db_password],
                                                  m_JarList)
            elif self.db_type.upper() == "MYSQL":
                self.db_conn = jaydebeapi.connect("com.mysql.cj.jdbc.Driver",
                                                  "jdbc:mysql://" +
                                                  self.db_host + ":" + self.db_port + "/" + self.db_service_name,
                                                  [self.db_username, self.db_password],
                                                  m_JarList)
            elif self.db_type.upper() == "POSTGRESQL":
                self.db_conn = jaydebeapi.connect("org.postgresql.Driver",
                                                  "jdbc:postgresql://" +
                                                  self.db_host + ":" + self.db_port + "/" + self.db_service_name,
                                                  [self.db_username, self.db_password],
                                                  m_JarList)
            elif self.db_type.upper() == "SQLSERVER":
                self.db_conn = jaydebeapi.connect("com.microsoft.sqlserver.jdbc.SQLServerDriver",
                                                  "jdbc:sqlserver://" +
                                                  self.db_host + ":" + self.db_port + ";" +
                                                  "databasename=" + self.db_service_name,
                                                  [self.db_username, self.db_password],
                                                  m_JarList)
            elif self.db_type.upper() == "CLICKHOUSE":
                self.db_conn = jaydebeapi.connect("ru.yandex.clickhouse.ClickHouseDriver",
                                                  "jdbc:clickhouse://" +
                                                  self.db_host + ":" + self.db_port + "/" + self.db_service_name,
                                                  {'user': self.db_username, 'password': self.db_password,
                                                   'socket_timeout': "360000000"},
                                                  m_JarList)
            elif self.db_type.upper() == "TERADATA":
                self.db_conn = jaydebeapi.connect("com.teradata.jdbc.TeraDriver",
                                                  "jdbc:teradata://" +
                                                  self.db_host +
                                                  "/CLIENT_CHARSET=UTF8,CHARSET=UTF8," +
                                                  "TMODE=TERA,LOB_SUPPORT=ON,COLUMN_NAME=ON,MAYBENULL=ON," +
                                                  "database=" + self.db_service_name,
                                                  [self.db_username, self.db_password],
                                                  m_JarList)
            else:
                raise SQLCliException("Unknown driver. " + self.db_type.upper() + " Exit ")

            self.SQLExecuteHandler.set_connection(self.db_conn)
        except Exception as e:  # Connecting to a database fail.
            if "SQLCLI_DEBUG" in os.environ:
                print('traceback.print_exc():\n%s' % traceback.print_exc())
                print('traceback.format_exc():\n%s' % traceback.format_exc())
                print("db_user = [" + str(self.db_username) + "]")
                print("db_pass = [" + str(self.db_password) + "]")
                print("db_type = [" + str(self.db_type) + "]")
                print("db_host = [" + str(self.db_host) + "]")
                print("db_port = [" + str(self.db_port) + "]")
                print("db_service_name = [" + str(self.db_service_name) + "]")
                print("db_url = [" + str(self.db_url) + "]")
                print("jar_file = [" + str(self.jar_file) + "]")
            raise SQLCliException(repr(e))

        yield (
            None,
            None,
            None,
            'Database connected.'
        )

    # 断开数据库连接
    def disconnect_db(self, arg, **_):
        if arg:
            return [(None, None, None, "unnecessary parameter")]
        if self.db_conn:
            self.db_conn.close()
        self.db_conn = None
        self.SQLExecuteHandler.conn = None
        yield (
            None,
            None,
            None,
            'Database disconnected.'
        )

    # 数据库会话管理
    def session_manage(self, arg, **_):
        if arg is None or len(str(arg)) == 0:
            raise SQLCliException(
                "Missing required argument: " + "Session save/restore [session name]")
        m_Parameters = str(arg).split()

        if len(m_Parameters) == 1 and m_Parameters[0] == 'show':
            m_Result = []
            for m_Session_Name, m_Connection in self.db_saved_conn.items():
                m_Result.append([str(m_Session_Name), str(m_Connection[1]), str(m_Connection[2])])
            if len(m_Result) == 0:
                yield (
                    None,
                    None,
                    None,
                    "No saved sesssions."
                )
            else:
                yield (
                    "Saved Sessions",
                    m_Result,
                    ["Sesssion Name", "User Name", "URL"],
                    "Total " + str(len(m_Result)) + " saved sesssions."
                )
            return

        # 要求两个参数 save/restore [session_name]
        if len(m_Parameters) != 2:
            raise SQLCliException(
                "Wrong argument : " + "Session save/restore [session name]")

        if m_Parameters[0] == 'save':
            if self.db_conn is None:
                raise SQLCliException(
                    "Please connect session first before save.")
            m_Session_Name = m_Parameters[1]
            self.db_saved_conn[m_Session_Name] = [self.db_conn, self.db_username, self.db_url]
        elif m_Parameters[0] == 'restore':
            m_Session_Name = m_Parameters[1]
            if m_Session_Name in self.db_saved_conn:
                self.db_conn = self.db_saved_conn[m_Session_Name][0]
                self.db_username = self.db_saved_conn[m_Session_Name][1]
                self.db_url = self.db_saved_conn[m_Session_Name][2]
                self.SQLExecuteHandler.set_connection(self.db_conn)
            else:
                raise SQLCliException(
                    "Session [" + m_Session_Name + "] does not exist. Please save it first.")
        else:
            raise SQLCliException(
                "Wrong argument : " + "Session save/restore [session name]")
        if m_Parameters[0] == 'save':
            yield None, None, None, "Session saved Successful."
        if m_Parameters[0] == 'restore':
            yield None, None, None, "Session restored Successful."
        return

    # 休息一段时间, 如果收到SHUTDOWN或者ABORT符号的时候，立刻终止SLEEP
    def sleep(self, arg, **_):
        if not arg:
            message = "Missing required argument, sleep [seconds]."
            return [(None, None, None, message)]
        try:
            # 每次最多休息3秒钟，随后检查一下运行状态, 如果要求退出，就退出程序
            m_Sleep_Time = int(arg)
            if m_Sleep_Time <= 0:
                message = "Parameter must be a valid number, sleep [seconds]."
                return [(None, None, None, message)]
            for m_nPos in range(0, int(arg)//3):
                if self.m_Shutdown_Flag:
                    raise EOFError
                time.sleep(3)
            time.sleep(int(arg) % 3)
        except ValueError:
            message = "Parameter must be a number, sleep [seconds]."
            return [(None, None, None, message)]
        return [(None, None, None, None)]

    # 从文件中执行SQL
    def execute_from_file(self, arg, **_):
        if not arg:
            message = "Missing required argument, filename."
            return [(None, None, None, message)]
        try:
            with open(os.path.expanduser(arg), encoding="utf-8") as f:
                query = f.read()
        except IOError as e:
            return [(None, None, None, str(e))]

        return self.SQLExecuteHandler.run(query, os.path.expanduser(arg))

    # 设置一些选项
    def set_options(self, arg, **_):
        if arg is None:
            raise SQLCliException("Missing required argument. set parameter parameter_value.")
        elif arg == "":      # 显示所有的配置
            m_Result = []
            for key, value in self.SQLExecuteHandler.options.items():
                m_Result.append([str(key), str(value)])
            yield (
                "Current set options: ",
                m_Result,
                ["option", "value"],
                ""
            )
        else:
            options_parameters = str(arg).split()
            if len(options_parameters) == 1:
                raise SQLCliException("Missing required argument. set parameter parameter_value.")

            # 处理DEBUG选项
            if options_parameters[0].upper() == "DEBUG":
                if options_parameters[1].upper() == 'ON':
                    os.environ['SQLCLI_DEBUG'] = "1"
                else:
                    if 'SQLCLI_DEBUG' in os.environ:
                        del os.environ['SQLCLI_DEBUG']

            # 如果不是已知的选项，则直接抛出到SQL引擎
            if options_parameters[0].upper() in self.SQLExecuteHandler.options:
                self.SQLExecuteHandler.options[options_parameters[0].upper()] = options_parameters[1]
                yield (
                    None,
                    None,
                    None,
                    '')
            else:
                raise CommandNotFound

    # 执行特殊的命令
    def execute_internal_command(self, arg, **_):
        # 处理kafka数据
        matchObj = re.match(r"(.*)kafka(.*)$", arg, re.IGNORECASE | re.DOTALL)
        if matchObj:
            (title, result, headers, status) = self.KafkaHandler.Process_SQLCommand(arg)
            yield title, result, headers, status
            return

        # 创建数据文件, 根据末尾的rows来决定创建的行数
        # 此时，SQL语句中的回车换行符没有意义
        matchObj = re.match(r"create\s+(.*?)\s+file\s+(.*?)\((.*)\)(\s+)?rows\s+(\d+)(\s+)?$",
                            arg, re.IGNORECASE | re.DOTALL)
        if matchObj:
            m_filetype = str(matchObj.group(1)).strip()
            m_filename = str(matchObj.group(2)).strip().replace('\r', '').replace('\n', '')
            m_formula_str = str(matchObj.group(3).replace('\r', '').replace('\n', '').strip())
            m_rows = int(matchObj.group(5))
            Create_file(p_filetype=m_filetype,
                        p_filename=m_filename,
                        p_formula_str=m_formula_str,
                        p_rows=m_rows)
            yield (
                None,
                None,
                None,
                str(m_rows) + ' rows created Successful.')
            return

        matchObj = re.match(r"create\s+(.*?)\s+file\s+(.*?)\((.*)\)(\s+)?$",
                            arg, re.IGNORECASE | re.DOTALL)
        if matchObj:
            m_filetype = str(matchObj.group(1)).strip()
            m_filename = str(matchObj.group(2)).strip().replace('\r', '').replace('\n', '')
            m_formula_str = str(matchObj.group(3).replace('\r', '').replace('\n', '').strip())
            m_rows = 1
            Create_file(p_filetype=m_filetype,
                        p_filename=m_filename,
                        p_formula_str=m_formula_str,
                        p_rows=m_rows)
            yield (
                None,
                None,
                None,
                str(m_rows) + ' rows created Successful.')
            return

        #  在不同的文件中进行相互转换
        matchObj = re.match(r"create\s+(.*?)\s+file\s+(.*?)\s+from\s+(.*?)file(.*?)(\s+)?$",
                            arg, re.IGNORECASE | re.DOTALL)
        if matchObj:
            # 在不同的文件中相互转换
            Convert_file(p_srcfileType=str(matchObj.group(3)).strip(),
                         p_srcfilename=str(matchObj.group(4)).strip(),
                         p_dstfileType=str(matchObj.group(1)).strip(),
                         p_dstfilename=str(matchObj.group(2)).strip())
            yield (
                None,
                None,
                None,
                'file converted Successful.')
            return

        # 创建随机数Seed的缓存文件
        matchObj = re.match(r"create\s+(integer|string)\s+seeddatafile\s+(.*?)\s+"
                            r"length\s+(\d+)\s+rows\s+(\d+)\s+with\s+null\s+rows\s+(\d+)$",
                            arg, re.IGNORECASE | re.DOTALL)
        if matchObj:
            m_DataType = str(matchObj.group(1)).lstrip().rstrip()
            m_SeedFileName = str(matchObj.group(2)).lstrip().rstrip()
            m_DataLength = int(matchObj.group(3))
            m_nRows = int(matchObj.group(4))
            m_nNullValueCount = int(matchObj.group(5))
            Create_SeedCacheFile(p_szDataType=m_DataType, p_nDataLength=m_DataLength, p_nRows=m_nRows,
                                 p_szSeedName=m_SeedFileName, p_nNullValueCount=m_nNullValueCount)
            yield (
                None,
                None,
                None,
                'seed file created Successful.')
            return

        # 创建随机数Seed的缓存文件
        matchObj = re.match(r"create\s+(integer|string)\s+seeddatafile\s+(.*?)\s+"
                            r"length\s+(\d+)\s+rows\s+(\d+)(\s+)?$",
                            arg, re.IGNORECASE | re.DOTALL)
        if matchObj:
            m_DataType = str(matchObj.group(1)).lstrip().rstrip()
            m_SeedFileName = str(matchObj.group(2)).lstrip().rstrip()
            m_DataLength = int(matchObj.group(3))
            m_nRows = int(matchObj.group(4))
            Create_SeedCacheFile(p_szDataType=m_DataType, p_nDataLength=m_DataLength, p_nRows=m_nRows,
                                 p_szSeedName=m_SeedFileName)
            yield (
                None,
                None,
                None,
                'seed file created Successful.')
            return

        # 不认识的internal命令
        raise SQLCliException("Unknown internal Command [" + str(arg) + "]. Please double check.")

    # 逐条处理SQL语句
    # 如果执行成功，返回true
    # 如果执行失败，返回false
    def DoSQL(self, text=None):
        # 如果程序被要求退出，则立即退出，不再执行任何SQL
        if self.m_Shutdown_Flag:
            raise EOFError

        # 判断传入SQL语句， 如果没有传递，则表示控制台程序，需要用户输入SQL语句
        if text is None:
            full_text = None
            while True:
                # 用户一行一行的输入SQL语句
                try:
                    if full_text is None:
                        text = self.prompt_app.prompt('SQL> ')
                    else:
                        text = self.prompt_app.prompt('   > ')
                except KeyboardInterrupt:
                    # KeyboardInterrupt 表示用户输入了CONTROL+C
                    return True
                # 拼接SQL语句
                if full_text is None:
                    full_text = text
                else:
                    full_text = full_text + '\n' + text
                # 判断SQL语句是否已经结束
                (ret_bSQLCompleted, ret_SQLSplitResults, ret_SQLSplitResultsWithComments) = SQLAnalyze(full_text)
                if ret_bSQLCompleted:
                    # SQL 语句已经结束
                    break
            text = full_text

        # 如果文本是空行，直接跳过
        if not text.strip():
            return True

        try:
            # 执行需要的SQL语句, 并记录当前运行脚本以及开始时间
            self.m_Current_RunningSQL = text
            self.m_Current_RunningStarted = time.time()
            result = self.SQLExecuteHandler.run(text)
            # 输出显示结果
            self.formatter.query = text
            for title, cur, headers, status in result:
                # 不控制每行的长度
                max_width = None

                # title 包含原有语句的SQL信息，如果ECHO打开的话
                # headers 包含原有语句的列名
                # cur 是语句的执行结果
                # output_format 输出格式
                #   ascii              默认，即表格格式
                #   vertical           分行显示，每行、每列都分行
                #   csv                csv格式显示
                formatted = self.format_output(
                    title, cur, headers,
                    self.SQLExecuteHandler.options["OUTPUT_FORMAT"].lower(),
                    max_width
                )

                # 输出显示信息
                try:
                    self.output(formatted, status)
                except KeyboardInterrupt:
                    # 显示过程中用户按下了CTRL+C
                    pass

            # 返回正确执行的消息
            return True
        except EOFError as e:
            # 当调用了exit或者quit的时候，会受到EOFError，这里直接抛出
            raise e
        except SQLCliException as e:
            # 用户执行的SQL出了错误, 由于SQLExecute已经打印了错误消息，这里直接退出
            self.output(None, e.message)
            if self.SQLExecuteHandler.options["WHENEVER_SQLERROR"] == "EXIT":
                raise e
        except Exception as e:
            if "SQLCLI_DEBUG" in os.environ:
                print('traceback.print_exc():\n%s' % traceback.print_exc())
                print('traceback.format_exc():\n%s' % traceback.format_exc())
            self.echo(repr(e), err=True, fg="red")
            return False
        finally:
            self.m_Current_RunningSQL = None
            self.m_Current_RunningStarted = None

    # 主程序
    def run_cli(self):
        # 程序运行的结果
        m_runCli_Result = True

        # 设置主程序的标题，随后开始运行程序
        setproctitle.setproctitle('SQLCli MAIN ' + " Script:" + str(self.sqlscript))

        # 打开输出日志, 如果打开失败，就直接退出
        try:
            if self.logfilename is not None:
                self.logfile = open(self.logfilename, mode="w", encoding="utf-8")
                self.SQLExecuteHandler.logfile = self.logfile
        except IOError as e:
            if "SQLCLI_DEBUG" in os.environ:
                print('traceback.print_exc():\n%s' % traceback.print_exc())
                print('traceback.format_exc():\n%s' % traceback.format_exc())
            self.echo("Can not open logfile for write [" + self.logfilename + "]", err=True, fg="red")
            self.echo(repr(e), err=True, fg="red")
            return False

        # 加载已经被隐式包含的数据库驱动，文件放置在SQLCli\jlib下
        m_jlib_directory = os.path.join(os.path.dirname(__file__), "jlib")
        if os.path.isdir(m_jlib_directory):
            list_file = os.listdir(m_jlib_directory)
            for i in range(0, len(list_file)):
                if list_file[i].endswith(".jar"):
                    m_JarFullFileName = os.path.join(m_jlib_directory, list_file[i])
                    m_JarBaseFileName = os.path.basename(m_JarFullFileName)
                    if self.jar_file is None:
                        self.jar_file = []
                    if m_JarBaseFileName.startswith("linkoopdb"):
                        m_DatabaseType = "linkoopdb"
                    elif m_JarBaseFileName.startswith("mysql"):
                        m_DatabaseType = "mysql"
                    elif m_JarBaseFileName.startswith("postgresql"):
                        m_DatabaseType = "postgresql"
                    elif m_JarBaseFileName.startswith("sqljdbc"):
                        m_DatabaseType = "sqlserver"
                    elif m_JarBaseFileName.startswith("ojdbc"):
                        m_DatabaseType = "oracle"
                    elif m_JarBaseFileName.startswith("terajdbc"):
                        m_DatabaseType = "teradata"
                    elif m_JarBaseFileName.startswith("tdgssconfig"):
                        m_DatabaseType = "teradata-gss"
                    else:
                        m_DatabaseType = "UNKNOWN"
                    m_Jar_Config = {"JarName": m_JarBaseFileName,
                                    "ClassName": None,
                                    "FullName": m_JarFullFileName,
                                    "Database": m_DatabaseType}
                    self.jar_file.append(m_Jar_Config)

        # 处理传递的映射文件
        if self.sqlmap is not None:   # 如果传递的参数，有Mapping，以参数为准，先加载参数中的Mapping文件
            self.SQLMappingHandler.Load_SQL_Mappings(self.sqlscript, self.sqlmap)
        elif "SQLCLI_SQLMAPPING" in os.environ:     # 如果没有参数，则以环境变量中的信息为准
            if len(os.environ["SQLCLI_SQLMAPPING"].strip()) > 0:
                self.SQLMappingHandler.Load_SQL_Mappings(self.sqlscript, os.environ["SQLCLI_SQLMAPPING"])
        else:  # 任何地方都没有sql mapping信息，设置QUERYREWRITE为OFF
            self.SQLExecuteHandler.options["SQLREWRITE"] = "OFF"

        # 给Page做准备，PAGE显示的默认换页方式.
        if not os.environ.get("LESS"):
            os.environ["LESS"] = "-RXF"

        # 如果参数要求不显示版本，则不再显示版本
        if not self.nologo:
            self.echo("SQLCli Release " + __version__)

        # 如果运行在脚本方式下，不在调用PromptSession, 调用PromptSession会导致程序在IDE下无法运行
        # 运行在无终端的模式下，也不会调用PromptSession, 调用PromptSession会导致程序出现Console错误
        # 对于脚本程序，在执行脚本完成后就会自动退出
        if self.sqlscript is None and not self.HeadlessMode:
            self.prompt_app = PromptSession()

        # 加载多进程管理
        # 一定要在start前注册，不然就注册无效
        manager = BaseManager()
        manager.register('BackGroundJobs', callable=BackGroundJobs)
        manager.start()
        func = getattr(manager, 'BackGroundJobs')
        self.m_BackGround_Jobs = func()

        # 开始依次处理SQL语句
        try:
            # 如果环境变量中包含了SQLCLI_CONNECTION_JAR_NAME
            # 则直接加载
            if "SQLCLI_CONNECTION_JAR_NAME" in os.environ:
                self.DoSQL('loaddriver ' + os.environ["SQLCLI_CONNECTION_JAR_NAME"])

            # 如果用户制定了用户名，口令，尝试直接进行数据库连接
            if self.logon:
                if not self.DoSQL("connect " + str(self.logon)):
                    m_runCli_Result = False
                    raise EOFError

            # 如果传递的参数中有SQL文件，先执行SQL文件, 执行完成后自动退出
            if self.sqlscript:
                try:
                    self.DoSQL('start ' + self.sqlscript)
                except SQLCliException:
                    m_runCli_Result = False
                    raise EOFError
                self.DoSQL('exit')
                self.echo("Disconnected.")
                return m_runCli_Result
            else:
                # 循环从控制台读取命令
                while True:
                    if not self.DoSQL():
                        m_runCli_Result = False
                        raise EOFError
        except (SQLCliException, EOFError):
            # SQLCliException只有在被设置了WHENEVER_SQLERROR为EXIT的时候，才会被捕获到
            self.echo("Disconnected.")

        # 当前进程关闭后退出RPC的Manager
        manager.shutdown()

        # 返回运行结果, True 运行成功。 False运行失败
        return m_runCli_Result

    def log_output(self, output):
        if self.logfile:
            click.echo(output, file=self.logfile)

    def echo(self, s, **kwargs):
        if self.logfile:
            click.echo(s, file=self.logfile)
        if self.logger is not None:
            self.logger.info(s)
        click.secho(s, **kwargs, file=self.Console)

    def output(self, output, status=None):
        if output:
            # size    记录了 每页输出最大行数，以及行的宽度。  Size(rows=30, columns=119)
            # margin  记录了每页需要留下多少边界行，如状态显示信息等 （2 或者 3）
            m_size_rows = 30
            m_size_columns = 119
            margin = 3

            # 打印输出信息
            fits = True
            buf = []
            output_via_pager = ((self.SQLExecuteHandler.options["PAGE"]).upper() == "ON")
            for i, line in enumerate(output, 1):
                if fits or output_via_pager:
                    # buffering
                    buf.append(line)
                    if len(line) > m_size_columns or i > (m_size_rows - margin):
                        # 如果行超过页要求，或者行内容过长，且没有分页要求的话，直接显示
                        fits = False
                        if not output_via_pager:
                            # doesn't fit, flush buffer
                            for bufline in buf:
                                self.echo(bufline)
                            buf = []
                else:
                    self.echo(line)

            if buf:
                if output_via_pager:
                    click.echo_via_pager("\n".join(buf))
                else:
                    for line in buf:
                        self.echo(line)

        if status:
            self.echo(status)

    def format_output(self, title, cur, headers, p_format_name, max_width=None):
        output = []

        output_kwargs = {
            "dialect": "unix",
            "disable_numparse": True,
            "preserve_whitespace": True,
            "preprocessors": (preprocessors.align_decimals,),
            "style": self.output_style,
        }

        if title:  # Only print the title if it's not None.
            output = itertools.chain(output, [title])

        if cur:
            if max_width is not None:
                cur = list(cur)

            formatted = self.formatter.format_output(
                cur,
                headers,
                format_name=p_format_name,
                column_types=None,
                **output_kwargs
            )
            if isinstance(formatted, str):
                formatted = formatted.splitlines()
            formatted = iter(formatted)

            # 获得输出信息的首行
            first_line = next(formatted)
            # 获得输出信息的格式控制
            formatted = itertools.chain([first_line], formatted)
            # 返回输出信息
            output = itertools.chain(output, formatted)
        return output


@click.command()
@click.option("--version", is_flag=True, help="Output sqlcli's version.")
@click.option("--logon", type=str, help="logon user name and password. user/pass",)
@click.option("--logfile", type=str, help="Log every query and its results to a file.",)
@click.option("--execute", type=str, help="Execute SQL script.")
@click.option("--sqlmap", type=str, help="SQL Mapping file.")
@click.option("--nologo", is_flag=True, help="Execute with silent mode.")
@click.option("--sqlperf", type=str, help="SQL performance Log.")
def cli(
        version,
        logon,
        logfile,
        execute,
        sqlmap,
        nologo,
        sqlperf
):
    if version:
        print("Version:", __version__)
        sys.exit(0)

    sqlcli = SQLCli(
        logfilename=logfile,
        logon=logon,
        sqlscript=execute,
        sqlmap=sqlmap,
        nologo=nologo,
        sqlperf=sqlperf
    )

    # 运行主程序
    sqlcli.run_cli()


if __name__ == "__main__":
    cli()
