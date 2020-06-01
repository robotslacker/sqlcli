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
from multiprocessing import Process, Manager, Lock
import setproctitle

from cli_helpers.tabular_output import TabularOutputFormatter
from cli_helpers.tabular_output import preprocessors
import click

from prompt_toolkit.shortcuts import PromptSession
from .sqlexecute import SQLExecute
from .sqlinternal import Create_file
from .sqlinternal import Create_SeedCacheFile
from .sqlcliexception import SQLCliException
from .commandanalyze import register_special_command
from .commandanalyze import CommandNotFound
from .sqlparse import SQLMapping

from .__init__ import __version__
from .sqlparse import SQLAnalyze

import itertools

click.disable_unicode_literals_warning = True

PACKAGE_ROOT = os.path.abspath(os.path.dirname(__file__))
# 进程锁, 用来控制对BackGround_Jobs的修改
LOCK_JOBCATALOG = Lock()


class SQLCli(object):
    # 数据库Jar包 [[abs_file, full_file], ]
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
    db_conn = None

    # SQLCli的初始化参数
    logon = None
    logfilename = None
    sqlscript = None
    sqlmap = None
    nologo = None

    # 执行者
    SQLMappingHandler = None
    SQLExecuteHandler = None

    # 程序输出日志
    logfile = None

    # 屏幕控制程序
    prompt_app = None

    # 目前程序运行的当前SQL
    m_Current_RunningSQL = None
    m_Current_RunningStarted = None

    # 目前程序的终止状态
    m_Current_RunningStatus = None

    # 程序停止标志
    m_Shutdown_Flag = False             # 在程序的每个语句，Sleep间歇中都会判断这个标志
    m_Abort_Flag = False                # 应用被强行终止，可能会有SQL错误

    # 后台进程队列
    # JOB#, Script_Name, Status, Current_SQL, SQL_Started, Conn, LogFile
    m_BackGround_Jobs = None
    m_Max_JobID = 0            # 当前的最大JOBID

    # 屏幕输出
    NoConsole = False

    # 性能分析日志输出
    m_SQLPerf = None

    def __init__(
            self,
            logon=None,
            logfilename=None,
            sqlscript=None,
            sqlmap=None,
            nologo=None,
            breakwitherror=False,
            sqlperf=None,
            noconsole=False,
            WorkerName=None
    ):
        # 初始化SQLExecute和SQLMap
        self.SQLExecuteHandler = SQLExecute()
        self.SQLMappingHandler = SQLMapping()

        # 传递各种参数
        self.sqlscript = sqlscript
        self.sqlmap = sqlmap
        self.nologo = nologo
        self.logon = logon
        self.logfilename = logfilename
        self.NoConsole = noconsole
        self.m_SQLPerf = sqlperf

        # 设置其他的变量
        self.SQLExecuteHandler.sqlscript = sqlscript
        self.SQLExecuteHandler.SQLMappingHandler = self.SQLMappingHandler
        self.SQLExecuteHandler.logfile = self.logfile
        self.SQLExecuteHandler.NoConsole = self.NoConsole
        self.SQLExecuteHandler.SQLPerfFile = self.m_SQLPerf
        self.SQLExecuteHandler.m_Worker_Name = WorkerName

        # 默认的输出格式
        self.formatter = TabularOutputFormatter(format_name='ascii')
        self.formatter.sqlcli = self
        self.syntax_style = 'default'
        self.output_style = None

        # 加载一些特殊的命令
        self.register_special_commands()

        # 设置WHENEVER_SQLERROR
        if breakwitherror:
            self.SQLExecuteHandler.options["WHENEVER_SQLERROR"] = "EXIT"

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

    # 设置停止标志。 在程序Sleep间隙或者语句执行间隙，如果收到Shutdown标志，就会停止程序继续运行
    def Abort(self):
        self.SQLExecuteHandler.m_Abort_Flag = True
        self.m_Abort_Flag = True

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
                for m_BackGround_Job in self.m_BackGround_Jobs:
                    if m_BackGround_Job["EndTime"] is None and m_BackGround_Job["StartedTime"] is not None:
                        m_ExitStauts = False
                        break
                if m_ExitStauts:
                    # 所有子进程都已经退出了
                    break
                time.sleep(3)

        # 检查是否所有进程都已经退出
        m_ExitStauts = True
        # 如果后面还有需要的作业完成没有完成，拒绝退出应用程序
        for m_BackGround_Job in self.m_BackGround_Jobs:
            if m_BackGround_Job["EndTime"] is None and m_BackGround_Job["StartedTime"] is not None:
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
            while True:
                m_bFoundJob = False
                # 如果后面还有需要的作业完成没有完成，等待
                for m_BackGround_Job in self.m_BackGround_Jobs:
                    if m_BackGround_Job["EndTime"] is None and m_BackGround_Job["StartedTime"] is not None:
                        m_bFoundJob = True
                        break
                if not m_bFoundJob:
                    yield (
                        None,
                        None,
                        None,
                        "All Job Completed")
                    return
                else:
                    time.sleep(3)

        if m_Parameters[0].upper().isnumeric():
            m_Job_ID = int(m_Parameters[0].upper())
        else:
            raise SQLCliException("Argument error. waitjob [all|job#].")

        while True:
            m_bFoundJob = False
            for m_BackGround_Job in self.m_BackGround_Jobs:
                if int(m_BackGround_Job["JOB#"]) == m_Job_ID:
                    m_bFoundJob = True
                    if m_BackGround_Job["EndTime"] is None and m_BackGround_Job["StartedTime"] is not None:
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
                time.sleep(3)
                continue

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
            for m_BackGround_Job in self.m_BackGround_Jobs:
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
        for m_BackGround_Job in self.m_BackGround_Jobs:
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
        m_nPos = 0            # 标记当前需要完成的Job
        m_CurrentJob = None
        for m_nPos in range(0, len(p_Job)):
            if str(p_Job[m_nPos]["JOB#"]) == m_JobID:
                m_CurrentJob = p_Job[m_nPos]
                break

        # 设置进程的标题，随后开始运行程序
        setproctitle.setproctitle('SQLCli Worker:' + str(m_JobID) + " Script:" + str(m_CurrentJob["ScriptBaseName"]))

        # 标记JOB开始时间
        m_CurrentJob["StartedTime"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
        if m_CurrentJob["Status"] != "Not Started":
            # 任务不需要启动，对共享的进程状态信息解锁
            LOCK_JOBCATALOG.release()
            return

        m_CurrentJob["Status"] = "RUNNING"
        m_CurrentLoop = int(m_CurrentJob["Finished"])
        p_Job[m_nPos] = m_CurrentJob

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
            m_SQLCli = SQLCli(
                sqlscript=m_CurrentJob["ScriptFullName"],
                logon=p_args["logon"],
                logfilename=m_logfilename,
                sqlmap=p_args["sqlmap"],
                nologo=p_args["nologo"],
                breakwitherror=p_args["breakwitherror"],
                noconsole=True,
                sqlperf=p_args["sqlperf"],
                WorkerName=m_JobID
            )
            m_WorkerThread = threading.Thread(target=runJobInThread, args=(m_SQLCli,))
            m_WorkerThread.start()

            # 循环检查Worker的状态
            m_OldCurrentSQL = ""
            while m_WorkerThread.is_alive():
                # 对共享的进程状态信息加锁
                LOCK_JOBCATALOG.acquire()

                # 如果要求程序停止，则设置停止标志
                if p_Job[m_nPos]["Status"] == "WAITINGFOR_SHUTDOWN":
                    m_SQLCli.Shutdown()        # 标记Shutdown标记
                    m_CurrentJob["Status"] = "SHUTDOWNING"
                    p_Job[m_nPos] = m_CurrentJob
                # 如果要求强制关闭程序，则直接关闭Connection
                if p_Job[m_nPos]["Status"] == "WAITINGFOR_ABORT":
                    if m_SQLCli.db_conn is not None:
                        m_SQLCli.db_conn.close()   # 强行关闭数据库连接
                    m_SQLCli.Abort()           # 标记Close标记
                    m_CurrentJob["Status"] = "ABORTING"
                    p_Job[m_nPos] = m_CurrentJob
                if p_Job[m_nPos]["Status"] == "WAITINGFOR_CLOSE":
                    m_CurrentJob["Status"] = "CLOSING"
                    p_Job[m_nPos] = m_CurrentJob
                # 更新Current_SQL
                m_CurrentSQL = m_SQLCli.m_Current_RunningSQL
                if m_OldCurrentSQL != m_CurrentSQL:
                    m_CurrentJob["Current_SQL"] = m_CurrentSQL
                    m_OldCurrentSQL = m_CurrentSQL
                    p_Job[m_nPos] = m_CurrentJob

                # 对共享的进程状态信息解锁
                LOCK_JOBCATALOG.release()

                # 3秒后再检查
                time.sleep(3)

            # 任务已经完成，检查Loop次数
            m_CurrentLoopCount = int(m_CurrentJob["Finished"])
            m_ToDoLoopCount = int(m_CurrentJob["LoopCount"])
            m_CurrentJob["Finished"] = str(m_CurrentLoopCount + 1)
            if (m_CurrentLoopCount + 1) >= m_ToDoLoopCount:              # 所有任务都已经完成
                # 对共享的进程状态信息加锁
                LOCK_JOBCATALOG.acquire()

                m_CurrentJob["EndTime"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
                m_CurrentJob["Status"] = "STOPPED"
                p_Job[m_nPos] = m_CurrentJob

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
                    p_Job[m_nPos] = m_CurrentJob

                    # 对共享的进程状态信息解锁
                    LOCK_JOBCATALOG.release()
                    break  # 运行已经全部结束

                # 如果已经关闭，则直接退出，标志ABORTED
                if m_CurrentJob["Status"] == "ABORTING":
                    # 对共享的进程状态信息加锁
                    LOCK_JOBCATALOG.acquire()

                    m_CurrentJob["EndTime"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
                    m_CurrentJob["Status"] = "ABORTED"
                    p_Job[m_nPos] = m_CurrentJob

                    # 对共享的进程状态信息解锁
                    LOCK_JOBCATALOG.release()
                    break  # 运行已经全部结束

                # 如果已经关闭，则直接退出，标志SHUTDOWN
                if m_CurrentJob["Status"] == "SHUTDOWNING":
                    # 对共享的进程状态信息加锁
                    LOCK_JOBCATALOG.acquire()

                    m_CurrentJob["EndTime"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
                    m_CurrentJob["Status"] = "SHUTDOWN"
                    p_Job[m_nPos] = m_CurrentJob

                    # 对共享的进程状态信息解锁
                    LOCK_JOBCATALOG.release()
                    break  # 运行已经全部结束

                # 继续新的一轮运行
                m_CurrentLoop = m_CurrentLoop + 1
                continue                       # 继续当前JOB的下一次运行

    # 启动后台SQL任务
    def startjob(self, arg, **_):
        if arg is None or len(str(arg).strip()) == 0:
            raise SQLCliException("Missing required argument. startjob [job#|all].")

        # 对共享的进程状态信息加锁
        LOCK_JOBCATALOG.acquire()

        # 如果参数中有all信息，启动全部的JOB，否则启动输入的JOB信息
        m_JobLists = []
        m_Parameters = str(arg).split()
        for m_Parameter in m_Parameters:
            if m_Parameter.upper() == 'ALL':
                if self.m_BackGround_Jobs is None:
                    raise SQLCliException("No Jobs to start.")
                m_JobLists.clear()
                for m_BackGround_Job in self.m_BackGround_Jobs:
                    if m_BackGround_Job["Status"] in "Not Started":
                        m_JobLists.append(m_BackGround_Job)
                break
            else:
                for m_BackGround_Job in self.m_BackGround_Jobs:
                    if str(m_BackGround_Job["JOB#"]) == str(m_Parameter):
                        if m_BackGround_Job["Status"] in "Not Started":
                            m_JobLists.append(m_BackGround_Job)

        # 启动JOB
        for m_Job in m_JobLists:
            m_args = {"JOB#": m_Job["JOB#"], "logon": self.logon, "sqlmap": self.sqlmap, "nologo": self.nologo,
                      "breakwitherror": (self.SQLExecuteHandler.options["WHENEVER_SQLERROR"] == "EXIT"),
                      "logfilename": self.logfilename, "sqlperf": self.m_SQLPerf}
            m_Process = Process(target=self.runJob, args=(m_args, self.m_BackGround_Jobs))
            m_Process.start()

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

        # 记录到任务列表中
        if self.m_BackGround_Jobs is None:
            self.m_BackGround_Jobs = list(Manager().list())

        # 给所有的Job增加一个编号，随后放入到数组中
        for m_nPos in range(0, m_Execute_Copies):
            self.m_Max_JobID = self.m_Max_JobID + 1
            self.m_BackGround_Jobs.append(
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

            for m_nPos in range(0, len(self.m_BackGround_Jobs)):
                m_BackGround_Job = self.m_BackGround_Jobs[m_nPos]
                # 所有RUNNING、WAITINGOFOR_CLOSE状态的程序，都标记为WAITINGFOR_ABORT
                if str(m_BackGround_Job["Status"]) in ("RUNNING", "WAITINGFOR_CLOSE"):
                    m_BackGround_Job["Status"] = "WAITINGFOR_SHUTDOWN"
                    m_nCount = m_nCount + 1
                    self.m_BackGround_Jobs[m_nPos] = m_BackGround_Job
                if str(m_BackGround_Job["Status"]) in "Not Started":
                    # 对于没有启动的JOB，直接关闭
                    m_BackGround_Job["Status"] = "SHUTDOWN"
                    m_nCount = m_nCount + 1
                    self.m_BackGround_Jobs[m_nPos] = m_BackGround_Job

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

        for m_nPos in range(0, len(self.m_BackGround_Jobs)):
            m_BackGround_Job = self.m_BackGround_Jobs[m_nPos]
            if int(m_BackGround_Job["JOB#"]) == m_Job_ID:
                if str(m_BackGround_Job["Status"]) in ("RUNNING", "WAITINGFOR_CLOSE"):
                    m_BackGround_Job["Status"] = "WAITINGFOR_SHUTDOWN"
                    self.m_BackGround_Jobs[m_nPos] = m_BackGround_Job
                    m_Result = True
                if str(m_BackGround_Job["Status"]) in "Not Started":
                    # 对于没有启动的JOB，直接关闭
                    m_BackGround_Job["Status"] = "SHUTDOWN"
                    self.m_BackGround_Jobs[m_nPos] = m_BackGround_Job
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
        m_nCount = 0
        if m_Parameters[0].upper() == "ALL":
            # 对共享的进程状态信息加锁
            LOCK_JOBCATALOG.acquire()

            for m_nPos in range(0, len(self.m_BackGround_Jobs)):
                m_BackGround_Job = self.m_BackGround_Jobs[m_nPos]
                # 所有RUNNING、WAITINGOFOR_CLOSE，WAITINGFOR_SHUTDOWN状态的程序，都标记为WAITINGFOR_ABORT
                if str(m_BackGround_Job["Status"]) in ("RUNNING", "WAITINGFOR_CLOSE", "WAITINGFOR_SHUTDOWN"):
                    m_BackGround_Job["Status"] = "WAITINGFOR_ABORT"
                    m_nCount = m_nCount + 1
                    self.m_BackGround_Jobs[m_nPos] = m_BackGround_Job
                if str(m_BackGround_Job["Status"]) in "Not Started":
                    # 对于没有启动的JOB，直接关闭
                    m_BackGround_Job["Status"] = "ABORTED"
                    m_nCount = m_nCount + 1
                    self.m_BackGround_Jobs[m_nPos] = m_BackGround_Job

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

        for m_nPos in range(0, len(self.m_BackGround_Jobs)):
            m_BackGround_Job = self.m_BackGround_Jobs[m_nPos]
            if int(m_BackGround_Job["JOB#"]) == m_Job_ID:
                if str(m_BackGround_Job["Status"]) in ("RUNNING", "WAITINGFOR_CLOSE", "WAITINGFOR_SHUTDOWN"):
                    m_BackGround_Job["Status"] = "WAITINGFOR_ABORT"
                    self.m_BackGround_Jobs[m_nPos] = m_BackGround_Job
                    m_Result = True
                if str(m_BackGround_Job["Status"]) in "Not Started":
                    # 对于没有启动的JOB，直接关闭
                    m_BackGround_Job["Status"] = "ABORTED"
                    self.m_BackGround_Jobs[m_nPos] = m_BackGround_Job
                    m_Result = True
                break

        # 对共享的进程状态信息解锁
        LOCK_JOBCATALOG.release()

        if m_Result:
            yield (
                None,
                None,
                None,
                "Job " + str(m_Job_ID) + " will abort.")
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

            for m_nPos in range(0, len(self.m_BackGround_Jobs)):
                m_BackGround_Job = self.m_BackGround_Jobs[m_nPos]
                # 所有RUNNING状态的程序，都标记为WAITINGFOR_STOP
                if str(m_BackGround_Job["Status"]) in "RUNNING":
                    m_BackGround_Job["Status"] = "WAITINGFOR_CLOSE"
                    m_nCount = m_nCount + 1
                    self.m_BackGround_Jobs[m_nPos] = m_BackGround_Job
                if str(m_BackGround_Job["Status"]) in "Not Started":
                    # 对于没有启动的JOB，直接关闭
                    m_BackGround_Job["Status"] = "CLOSED"
                    m_nCount = m_nCount + 1
                    self.m_BackGround_Jobs[m_nPos] = m_BackGround_Job

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

        for m_nPos in range(0, len(self.m_BackGround_Jobs)):
            m_BackGround_Job = self.m_BackGround_Jobs[m_nPos]
            if int(m_BackGround_Job["JOB#"]) == m_Job_ID:
                if str(m_BackGround_Job["Status"]) in "RUNNING":
                    m_BackGround_Job["Status"] = "WAITINGFOR_CLOSE"
                    self.m_BackGround_Jobs[m_nPos] = m_BackGround_Job
                    m_Result = True
                if str(m_BackGround_Job["Status"]) in "Not Started":
                    # 对于没有启动的JOB，直接关闭
                    m_BackGround_Job["Status"] = "CLOSED"
                    self.m_BackGround_Jobs[m_nPos] = m_BackGround_Job
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
        if arg is None:
            raise SQLCliException(
                "Missing required argument\n." + "connect [user name]/[password]@" +
                "jdbc:[db type]:[driver type]://[host]:[port]/[service name]")
        elif arg == "":
            raise SQLCliException(
                "Missing required argument\n." + "connect [user name]/[password]@" +
                "jdbc:[db type]:[driver type]://[host]:[port]/[service name]")
        elif self.jar_file is None:
            raise SQLCliException("Please load driver first.")

        # 去掉为空的元素
        # connect_parameters = [var for var in re.split(r'//|:|@| |/', arg) if var]
        connect_parameters = re.split(r'://|:|@|/', arg)
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
                    raise SQLCliException("Missing required argument\n." + "connect [user name]/[password]@" +
                                          "jdbc:[db type]:[driver type]://[host]:[port]/[service name]")
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
                        raise SQLCliException("Missing required argument\n." + "connect [user name]/[password]@" +
                                              "jdbc:[db type]:[driver type]://[host]:[port]/[service name]")
        else:
            # 不知道的参数写法
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
                if self.m_Abort_Flag:
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
            raise Exception("Missing required argument. set parameter parameter_value.")
        elif arg == "":
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
                raise Exception("Missing required argument. set parameter parameter_value.")

            # 处理DEBUG选项
            if options_parameters[0].upper() == "DEBUG":
                if options_parameters[1].upper() == 'ON':
                    os.environ['SQLCLI_DEBUG'] = "1"
                else:
                    if 'SQLCLI_DEBUG' in os.environ:
                        del os.environ['SQLCLI_DEBUG']

            # 如果不是已知的选项，则直接抛出到SQL引擎
            if options_parameters[0].upper() in self.SQLExecuteHandler.options:
                self.SQLExecuteHandler.options[options_parameters[0].upper()] = options_parameters[1].upper()
                yield (
                    None,
                    None,
                    None,
                    '')
            else:
                raise CommandNotFound

    # 执行特殊的命令
    def execute_internal_command(self, arg, **_):
        # 去掉回车换行符，以及末尾的空格
        strSQL = str(arg).replace('\r', '').replace('\n', '').strip()

        # 创建数据文件
        matchObj = re.match(r"create\s+file\s+(.*?)\((.*)\)(\s+)?rows\s+([1-9]\d*)(\s+)?(;)?$",
                            strSQL, re.IGNORECASE)
        if matchObj:
            # create file command  将根据格式要求创建需要的文件
            Create_file(p_filename=str(matchObj.group(1)),
                        p_formula_str=str(matchObj.group(2)),
                        p_rows=int(matchObj.group(4)),
                        p_options=self.SQLExecuteHandler.options)
            yield (
                None,
                None,
                None,
                str(matchObj.group(4)) + ' rows created Successful.')
            return

        # 创建随机数Seed的缓存文件
        matchObj = re.match(r"create\s+seeddatafile(\s+)?;$",
                            strSQL, re.IGNORECASE)
        if matchObj:
            Create_SeedCacheFile()
            yield (
                None,
                None,
                None,
                'file created Successful.')
            return

        # 不认识的internal命令
        raise SQLCliException("Unknown internal Command. Please double check.")

    # 逐条处理SQL语句
    # 如果执行成功，返回true
    # 如果执行失败，返回false
    def DoSQL(self, text=None):
        # 如果程序被要求退出，则立即退出，不再执行任何SQL
        if self.m_Shutdown_Flag or self.m_Abort_Flag:
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
            self.SQLMappingHandler.Load_SQL_Mappings(self.sqlscript, os.environ["SQLCLI_SQLMAPPING"])
        else:  # 任何地方都没有sql mapping信息，设置QUERYREWRITE为OFF
            self.SQLExecuteHandler.options["SQLREWRITE"] = "OFF"

        # 给Page做准备，PAGE显示的默认换页方式.
        if not os.environ.get("LESS"):
            os.environ["LESS"] = "-RXF"

        # 如果参数要求不显示版本，则不再显示版本
        if not self.nologo:
            self.echo("SQLCli Release " + __version__)

        # 如果运行在脚本方式下，不在调用PromptSession
        # 调用PromptSession会导致程序在IDE下无法运行
        # 对于脚本程序，在执行脚本完成后就会自动退出
        if self.sqlscript is None and not self.NoConsole:
            self.prompt_app = PromptSession()

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
                if not self.DoSQL('start ' + self.sqlscript):
                    m_runCli_Result = False
                    raise EOFError
                self.DoSQL('exit')

            # 循环从控制台读取命令
            if not self.NoConsole:
                while True:
                    if not self.DoSQL():
                        m_runCli_Result = False
                        raise EOFError
        except (SQLCliException, EOFError):
            # SQLCliException只有在被设置了WHENEVER_SQLERROR为EXIT的时候，才会被捕获到
            self.echo("Disconnected.")
            return m_runCli_Result

    def log_output(self, output):
        if self.logfile:
            click.echo(output, file=self.logfile)

    def echo(self, s, **kwargs):
        self.log_output(s)
        if not self.NoConsole:
            click.secho(s, **kwargs)

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
                self.log_output(line)       # 输出文件中总是不考虑分页问题
                if not self.NoConsole:
                    if fits or output_via_pager:
                        # buffering
                        buf.append(line)
                        if len(line) > m_size_columns or i > (m_size_rows - margin):
                            # 如果行超过页要求，或者行内容过长，且没有分页要求的话，直接显示
                            fits = False
                            if not output_via_pager:
                                # doesn't fit, flush buffer
                                for bufline in buf:
                                    click.secho(bufline)
                                buf = []
                    else:
                        click.secho(line)

            if buf:
                if not self.NoConsole:
                    if output_via_pager:
                        click.echo_via_pager("\n".join(buf))
                    else:
                        for line in buf:
                            click.secho(line)

        if status:
            self.log_output(status)
            if not self.NoConsole:
                click.secho(status)

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
            # 列的数据类型，如果不存在，按照None来处理
            column_types = None
            if hasattr(cur, "description"):
                def get_col_type(col):
                    # col_type = FIELD_TYPES.get(col[1], text_type)
                    # return col_type if type(col_type) is type else text_type
                    return str

                column_types = [get_col_type(col) for col in cur.description]

            if max_width is not None:
                cur = list(cur)

            formatted = self.formatter.format_output(
                cur,
                headers,
                format_name=p_format_name,
                column_types=column_types,
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
@click.option(
    "--logon",
    type=str,
    help="logon user name and password. user/pass",
)
@click.option(
    "--logfile",
    type=str,
    help="Log every query and its results to a file.",
)
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
        sqlperf=sqlperf,
        WorkerName="MAIN"
    )

    # 运行主程序
    sqlcli.run_cli()


if __name__ == "__main__":
    cli()
