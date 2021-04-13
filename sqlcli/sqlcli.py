# -*- coding: utf-8 -*-
import os
import sys
import traceback
import re
import time
import setproctitle
import shlex
import click
import configparser
import hashlib
import codecs
import subprocess
import pyparsing
import unicodedata
import itertools
import requests
import json
import asyncio
import websockets
import urllib3
import shutil
from multiprocessing import Lock
from time import strftime, localtime
from urllib.error import URLError
from prompt_toolkit.shortcuts import PromptSession
from multiprocessing.managers import BaseManager

# 加载JDBC驱动和ODBC驱动
from .sqlclijdbcapi import connect as jdbcconnect
from SQLCliODBC import connect as odbcconnect
from SQLCliODBC import SQLCliODBCException
import jpype

from .sqlclijobmanager import JOBManager
from .sqlclitransactionmanager import TransactionManager
from .sqlexecute import SQLExecute
from .sqlparse import SQLMapping
from .kafkawrapper import KafkaWrapper
from .testwrapper import TestWrapper
from .hdfswrapper import HDFSWrapper
from .sqlcliexception import SQLCliException
from .sqlclisga import SQLCliGlobalSharedMemory
from .datawrapper import DataWrapper
from .commandanalyze import register_special_command
from .commandanalyze import CommandNotFound
from .sqloption import SQLOptions
from .__init__ import __version__
from .sqlparse import SQLAnalyze

OFLAG_LOGFILE = 1
OFLAG_LOGGER = 2
OFLAG_CONSOLE = 4
OFLAG_SPOOL = 8
OFLAG_ECHO = 16


class SQLCli(object):
    # 连接配置信息
    # Database
    # ClassName
    # FullName
    # JDBCURL
    # ODBCURL
    # JDBCProp
    connection_configs = None

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

    # 屏幕输出
    Console = None  # 程序的控制台显示
    logfile = None  # 程序输出日志文件
    HeadlessMode = False  # 没有显示输出，即不需要回显，用于子进程的显示
    logger = None  # 程序的输出日志

    def __init__(
            self,
            logon=None,                             # 默认登录信息，None表示不需要
            logfilename=None,                       # 程序输出文件名，None表示不需要
            sqlscript=None,                         # 脚本文件名，None表示命令行模式
            sqlmap=None,                            # SQL映射文件名，None表示不存在
            nologo=False,                           # 是否不打印登陆时的Logo信息，True的时候不打印
            breakwitherror=False,                   # 遇到SQL错误，是否中断脚本后续执行，立刻退出
            sqlperf=None,                           # SQL审计文件输出名，None表示不需要
            Console=sys.stdout,                     # 控制台输出，默认为sys.stdout,即标准输出
            HeadlessMode=False,                     # 是否为无终端模式，无终端模式下，任何屏幕信息都不会被输出
            WorkerName='MAIN',                      # 程序别名，可用来区分不同的应用程序
            logger=None,                            # 程序输出日志句柄
            clientcharset='UTF-8',                  # 客户端字符集，在读取SQL文件时，采纳这个字符集，默认为UTF-8
            resultcharset='UTF-8',                  # 输出字符集，在打印输出文件，日志的时候均采用这个字符集
            EnableJobManager=True,                  # 是否开启后台调度程序管理模块，否则无法使用JOB类相关命令
            SharedProcessInfo=None,                 # 共享内存信息。内部变量，不对外书写
            profile=None                            # 程序初始化执行脚本
    ):
        self.db_saved_conn = {}                         # 数据库Session备份
        self.SQLMappingHandler = SQLMapping()           # 函数句柄，处理SQLMapping信息
        self.SQLExecuteHandler = SQLExecute()           # 函数句柄，具体来执行SQL
        self.SQLOptions = SQLOptions()                  # 程序运行中各种参数
        self.KafkaHandler = KafkaWrapper()              # Kafka消息管理器
        self.TestHandler = TestWrapper()                # 测试管理
        self.HdfsHandler = HDFSWrapper()                # HDFS文件操作
        self.JobHandler = JOBManager()                  # 并发任务管理器
        self.TransactionHandler = TransactionManager()  # 事务管理器
        self.DataHandler = DataWrapper()                # 随机临时数处理
        self.SpoolFileHandler = []                      # Spool文件句柄, 是一个数组，可能发生嵌套
        self.EchoFileHandler = None                     # 当前回显文件句柄
        self.AppOptions = None                          # 应用程序的配置参数
        self.Encoding = None                            # 应用程序的Encoding信息
        self.prompt_app = None                          # PromptKit控制台
        self.db_conn = None                             # 当前应用的数据库连接句柄
        self.SessionName = None                         # 当前会话的Session的名字
        self.db_conntype = None                         # 数据库连接方式，  JDBC或者是ODBC
        self.echofilename = None                        # 当前回显文件的文件名称
        self.Version = __version__                      # 当前程序版本
        self.ClientID = None                            # 远程连接时的客户端ID
        self.SQLPerfFile = None                         # SQLPerf文件名
        self.SQLPerfFileHandle = None                   # SQLPerf文件句柄
        self.PerfFileLocker = None                      # 进程锁, 用来在输出perf文件的时候控制并发写文件

        if clientcharset is None:                       # 客户端字符集
            self.Client_Charset = 'UTF-8'
        else:
            self.Client_Charset = clientcharset
        if resultcharset is None:
            self.Result_Charset = self.Client_Charset  # 结果输出字符集
        else:
            self.Result_Charset = resultcharset
        self.WorkerName = WorkerName                    # 当前进程名称. 如果有参数传递，以参数为准
        self.MultiProcessManager = None                 # 进程间共享消息管理器， 如果为子进程，该参数为空
        self.profile = []                               # 程序的初始化脚本文件

        self.m_LastComment = None                       # 如果当前SQL之前的内容完全是注释，则注释带到这里

        # 传递各种参数
        self.sqlscript = sqlscript
        self.sqlmap = sqlmap
        self.nologo = nologo
        self.logon = logon
        self.logfilename = logfilename
        self.Console = Console
        self.HeadlessMode = HeadlessMode
        self.SQLPerfFile = sqlperf
        if HeadlessMode:
            HeadLessConsole = open(os.devnull, "w")
            self.Console = HeadLessConsole
        self.logger = logger
        if SharedProcessInfo is None:
            # 启动共享内存管理，注册服务
            # 注册后台共享进程管理
            if EnableJobManager:
                self.MultiProcessManager = BaseManager()
                self.MultiProcessManager.register('SQLCliGlobalSharedMemory', callable=SQLCliGlobalSharedMemory)
                self.MultiProcessManager.start()
                obj = getattr(self.MultiProcessManager, 'SQLCliGlobalSharedMemory')
                self.SharedProcessInfo = obj()
            else:
                self.SharedProcessInfo = None
        else:
            self.SharedProcessInfo = SharedProcessInfo

        # profile的顺序， <PYTHON_PACKAGE>/sqlcli/profile/default， SQLCLI_HOME/profile/default , user define
        if os.path.isfile(os.path.join(os.path.dirname(__file__), "profile", "default")):
            if os.path.getsize(os.path.join(os.path.dirname(__file__), "profile", "default")) > 0:
                self.profile.append(os.path.join(os.path.dirname(__file__), "profile", "default"))
        if "SQLCLI_HOME" in os.environ:
            if os.path.isfile(os.path.join(os.environ["SQLCLI_HOME"], "profile", "default")):
                self.profile.append(os.path.join(os.environ["SQLCLI_HOME"], "profile", "default"))
        if profile is not None:
            if os.path.isfile(profile):
                self.profile.append(profile)
            else:
                if "SQLCLI_DEBUG" in os.environ:
                    print("Profile does not exist ! Will ignore it. [" + str(os.path.abspath(profile)) + "]")
        if "SQLCLI_DEBUG" in os.environ:
            for m_Profile in self.profile:
                print("Profile = [" + str(m_Profile) + "]")

        # 设置self.JobHandler， 默认情况下，子进程启动的进程进程信息来自于父进程
        self.JobHandler.setProcessContextInfo("logon", self.logon)
        self.JobHandler.setProcessContextInfo("nologo", self.nologo)
        self.JobHandler.setProcessContextInfo("sqlmap", self.sqlmap)
        self.JobHandler.setProcessContextInfo("sqlperf", sqlperf)
        self.JobHandler.setProcessContextInfo("logfilename", self.logfilename)
        self.JobHandler.setProcessContextInfo("sqlscript", self.sqlscript)
        self.JobHandler.setProcessContextInfo("sga", self.SharedProcessInfo)
        self.TransactionHandler.setSharedProcessInfo(self.SharedProcessInfo)
        self.TransactionHandler.SQLExecuteHandler = self.SQLExecuteHandler

        # 设置其他的变量
        self.SQLExecuteHandler.SQLCliHandler = self
        self.SQLExecuteHandler.sqlscript = sqlscript
        self.SQLExecuteHandler.SQLMappingHandler = self.SQLMappingHandler
        self.SQLExecuteHandler.SQLOptions = self.SQLOptions
        self.SQLExecuteHandler.WorkerName = self.WorkerName

        # 加载一些特殊的命令
        self.register_special_commands()

        # 设置WHENEVER_SQLERROR
        if breakwitherror:
            self.SQLOptions.set("WHENEVER_SQLERROR", "EXIT")

        # 加载程序的配置文件
        self.AppOptions = configparser.ConfigParser()
        m_conf_filename = os.path.join(os.path.dirname(__file__), "conf", "sqlcli.ini")
        if os.path.exists(m_conf_filename):
            self.AppOptions.read(m_conf_filename)
        else:
            raise SQLCliException("Can not open inifile for read [" + m_conf_filename + "]")

        # 打开输出日志, 如果打开失败，就直接退出
        try:
            if self.logfilename is not None:
                self.logfile = open(self.logfilename, mode="w", encoding=self.Result_Charset)
                self.SQLExecuteHandler.logfile = self.logfile
        except IOError as e:
            if "SQLCLI_DEBUG" in os.environ:
                print('traceback.print_exc():\n%s' % traceback.print_exc())
                print('traceback.format_exc():\n%s' % traceback.format_exc())
            raise SQLCliException("Can not open logfile for write [" + self.logfilename + "]")

        # 加载已经被隐式包含的数据库驱动，文件放置在SQLCli\jlib下
        m_jlib_directory = os.path.join(os.path.dirname(__file__), "jlib")
        if self.connection_configs is None:
            self.connection_configs = []
        if self.AppOptions is not None:
            for row in self.AppOptions.items("driver"):
                m_DriverName = None
                m_JarFullFileName = []
                m_JDBCURL = None
                m_ODBCURL = None
                m_JDBCProp = None
                m_jar_filename = None
                m_DatabaseType = row[0].strip()
                for m_driversection in str(row[1]).split(','):
                    m_driversection = m_driversection.strip()
                    if m_ODBCURL is None:
                        try:
                            m_ODBCURL = self.AppOptions.get(m_driversection, "odbcurl")
                        except (configparser.NoSectionError, configparser.NoOptionError):
                            m_ODBCURL = None
                    if m_DriverName is None:
                        try:
                            m_DriverName = self.AppOptions.get(m_driversection, "driver")
                        except (configparser.NoSectionError, configparser.NoOptionError):
                            m_DriverName = None
                    if m_JDBCURL is None:
                        try:
                            m_JDBCURL = self.AppOptions.get(m_driversection, "jdbcurl")
                        except (configparser.NoSectionError, configparser.NoOptionError):
                            m_JDBCURL = None
                    if m_JDBCProp is None:
                        try:
                            m_JDBCProp = self.AppOptions.get(m_driversection, "jdbcprop")
                        except (configparser.NoSectionError, configparser.NoOptionError):
                            m_JDBCProp = None
                    if m_jar_filename is None:
                        try:
                            m_jar_filename = self.AppOptions.get(m_driversection, "filename")
                            if os.path.exists(os.path.join(m_jlib_directory, m_jar_filename)):
                                m_JarFullFileName.append(os.path.join(m_jlib_directory, m_jar_filename))
                                if "SQLCLI_DEBUG" in os.environ:
                                    print("Load jar ..! [" +
                                          os.path.join(m_jlib_directory, m_jar_filename) + "]")
                                m_jar_filename = None
                            else:
                                if "SQLCLI_DEBUG" in os.environ:
                                    print("Driver file does not exist! [" +
                                          os.path.join(m_jlib_directory, m_jar_filename) + "]")
                        except (configparser.NoSectionError, configparser.NoOptionError):
                            m_jar_filename = None
                m_Jar_Config = {"ClassName": m_DriverName,
                                "FullName": m_JarFullFileName,
                                "JDBCURL": m_JDBCURL,
                                "JDBCProp": m_JDBCProp,
                                "Database": m_DatabaseType,
                                "ODBCURL": m_ODBCURL}
                self.connection_configs.append(m_Jar_Config)

        # 处理传递的映射文件, 首先加载参数的部分，如果环境变量里头有设置，则环境变量部分会叠加参数部分
        self.SQLOptions.set("SQLREWRITE", "OFF")
        if self.sqlmap is not None:  # 如果传递的参数，有Mapping，以参数为准，先加载参数中的Mapping文件
            self.SQLMappingHandler.Load_SQL_Mappings(self.sqlscript, self.sqlmap)
            self.SQLOptions.set("SQLREWRITE", "ON")
        if "SQLCLI_SQLMAPPING" in os.environ:  # 如果没有参数，则以环境变量中的信息为准
            if len(os.environ["SQLCLI_SQLMAPPING"].strip()) > 0:
                self.SQLMappingHandler.Load_SQL_Mappings(self.sqlscript, os.environ["SQLCLI_SQLMAPPING"])
                self.SQLOptions.set("SQLREWRITE", "ON")

        # 给Page做准备，PAGE显示的默认换页方式.
        if not os.environ.get("LESS"):
            os.environ["LESS"] = "-RXF"

        # 如果需要连接到远程，则创建连接
        if "SQLCLI_REMOTESERVER" in os.environ:
            try:
                request_data = json.dumps({})
                headers = {'Content-Type': 'application/json', 'accept': 'application/json'}
                ret = requests.post("http://" + os.environ["SQLCLI_REMOTESERVER"] + "/DoLogin",
                                    data=request_data,
                                    headers=headers)
                self.ClientID = json.loads(ret.text)["clientid"]
            except Exception as e:
                raise SQLCliException("Connect to RemoteServer failed. " + repr(e))

        # 处理初始化启动文件，如果需要的话，在处理的过程中不打印任何日志信息
        if len(self.profile) != 0:
            if "SQLCLI_DEBUG" not in os.environ:
                self.SQLOptions.set("SILENT", "ON")
            for m_Profile in self.profile:
                if "SQLCLI_DEBUG" in os.environ:
                    print("DEBUG:: Begin SQL profile [" + m_Profile + "] ...")
                self.DoSQL('start ' + m_Profile)
                if "SQLCLI_DEBUG" in os.environ:
                    print("DEBUG:: End SQL profile [" + m_Profile + "]")
            if "SQLCLI_DEBUG" not in os.environ:
                self.SQLOptions.set("SILENT", "OFF")

    def __del__(self):
        # 如果已经连接到远程，则断开连接
        if "SQLCLI_REMOTESERVER" in os.environ:
            if self.ClientID is not None:
                try:
                    request_data = json.dumps(
                        {
                            "clientid": self.ClientID
                        })
                    headers = {'Content-Type': 'application/json', 'accept': 'application/json'}
                    try:
                        ret = requests.post("http://" + os.environ["SQLCLI_REMOTESERVER"] + "/DoLogout",
                                            data=request_data,
                                            headers=headers,
                                            timeout=3)
                    except (requests.exceptions.ConnectionError, requests.exceptions.ReadTimeout):
                        # 已经无法联系，直接退出，不再报告错误
                        pass
                    self.ClientID = None
                except Exception as e:
                    print("e:" + str(type(e)))
                    raise SQLCliException("Logout from RemoteServer failed. " + repr(e))

        # 如果打开了多进程管理，则关闭多进程管理
        if self.MultiProcessManager is not None:
            self.MultiProcessManager.shutdown()

        # 关闭LogFile
        if self.logfile is not None:
            self.logfile.flush()
            self.logfile.close()
            self.logfile = None

    # 加载CLI的各种特殊命令集
    def register_special_commands(self):

        # 加载SQL映射文件
        register_special_command(
            handler=self.load_driver,
            command="loaddriver",
            description="加载数据库驱动文件",
            hidden=False
        )

        # 加载SQL映射文件
        register_special_command(
            handler=self.load_sqlmap,
            command="loadsqlmap",
            description="加载SQL映射文件",
            hidden=False
        )

        # 连接数据库
        register_special_command(
            handler=self.connect_db,
            command="connect",
            description="连接到指定的数据库",
            hidden=False
        )

        # 连接数据库
        register_special_command(
            handler=self.session_manage,
            command="session",
            description="数据库连接会话管理",
            hidden=False
        )

        # 断开连接数据库
        register_special_command(
            handler=self.disconnect_db,
            command="disconnect",
            description="断开数据库连接",
            hidden=False
        )

        # 从文件中执行脚本
        register_special_command(
            handler=self.execute_from_file,
            command="start",
            description="执行指定的测试脚本",
            hidden=False
        )

        # sleep一段时间
        register_special_command(
            handler=self.sleep,
            command="sleep",
            description="程序休眠(单位是秒)",
            hidden=False
        )

        # 设置各种参数选项
        register_special_command(
            handler=self.set_options,
            command="set",
            description="设置运行时选项",
            hidden=False
        )

        # 将SQL信息Spool到指定的文件中
        register_special_command(
            handler=self.spool,
            command="spool",
            description="将输出打印到指定文件",
            hidden=False
        )

        # ECHO 回显信息到指定的文件中
        register_special_command(
            handler=self.echo_input,
            command="echo",
            description="回显输入到指定的文件",
            hidden=False
        )

        # HOST 执行主机的各种命令
        register_special_command(
            handler=self.host,
            command="host",
            description="执行操作系统命令",
            hidden=False
        )

        # 执行特殊的命令
        register_special_command(
            handler=self.execute_internal_command,
            command="__internal__",
            description="执行内部操作命令：\n"
                        "    __internal__ hdfs          HDFS文件操作\n"
                        "    __internal__ kafka         kafka队列操作\n"
                        "    __internal__ data          随机测试数据管理\n"
                        "    __internal__ test          测试管理\n"
                        "    __internal__ job           后台并发测试任务管理\n"
                        "    __internal__ transaction   后台并发测试事务管理",
            hidden=False
        )

        # 退出当前应用程序
        register_special_command(
            handler=self.exit,
            command="exit",
            description="正常退出当前应用程序",
            hidden=False
        )

    # 退出当前应用程序
    @staticmethod
    def exit(cls, arg, **_):
        if arg:
            # 不处理任何exit的参数信息
            pass

        if cls.sqlscript is not None:
            # 运行在脚本模式下，会一直等待所有子进程退出后，再退出本程序
            while True:
                if not cls.JobHandler.isAllJobClosed():
                    # 如果还有没有退出的进程，则不会直接退出程序，会继续等待进程退出
                    time.sleep(3)
                else:
                    break
            # 等待那些已经Running的进程完成， 但是只有Submitted的不考虑在内
            cls.JobHandler.waitjob("all")
            # 断开数据库连接
            if cls.db_conn:
                cls.db_conn.close()
            cls.db_conn = None
            cls.SQLExecuteHandler.conn = None
            # 断开之前保存的其他数据库连接
            for m_conn in cls.db_saved_conn.values():
                m_conn[0].close()
            # 取消进程共享服务的注册信息
            cls.JobHandler.unregister()
            # 退出应用程序
            raise EOFError
        else:
            # 运行在控制台模式下
            if not cls.JobHandler.isAllJobClosed():
                yield {
                    "title": None,
                    "rows": None,
                    "headers": None,
                    "columntypes": None,
                    "status": "Please wait all background process complete."
                }
            else:
                # 退出应用程序
                raise EOFError

    # 加载数据库驱动
    # 标准的默认驱动程序并不需要使用这个函数，这个函数是用来覆盖标准默认驱动程序的加载信息
    @staticmethod
    def load_driver(cls, arg, **_):
        if arg == "":  # 显示当前的Driver配置
            m_Result = []
            for row in cls.connection_configs:
                m_Result.append([row["Database"], row["ClassName"], row["FullName"],
                                 row["JDBCURL"], row["ODBCURL"], row["JDBCProp"]])
            yield {
                "title": "Current Drivers: ",
                "rows": m_Result,
                "headers": ["Database", "ClassName", "FileName", "JDBCURL", "ODBCURL", "JDBCProp"],
                "columntypes": None,
                "status": "Driver loaded."
            }
            return

        # 解析命令参数
        options_parameters = str(arg).split()

        # 只有一个参数，打印当前Database的Driver情况
        if len(options_parameters) == 1:
            m_DriverName = str(options_parameters[0])
            m_Result = []
            for row in cls.connection_configs:
                if row["Database"] == m_DriverName:
                    m_Result.append([row["Database"], row["ClassName"], row["FullName"],
                                     row["JDBCURL"], row["ODBCURL"], row["JDBCProp"]])
                    break
            yield {
                "title": "Current Drivers: ",
                "rows": m_Result,
                "headers": ["Database", "ClassName", "FileName", "JDBCURL", "ODBCURL", "JDBCProp"],
                "columntypes": None,
                "status": "Driver loaded."
            }
            return

        # 两个参数，替换当前Database的Driver
        if len(options_parameters) == 2:
            m_DriverName = str(options_parameters[0])
            m_DriverFullName = str(options_parameters[1])
            if cls.sqlscript is None:
                m_DriverFullName = os.path.join(sys.path[0], m_DriverFullName)
            else:
                m_DriverFullName = os.path.abspath(os.path.join(os.path.dirname(cls.sqlscript), m_DriverFullName))
            if not os.path.isfile(m_DriverFullName):
                raise SQLCliException("Driver not loaded. file [" + m_DriverFullName + "] does not exist!")
            bFound = False
            for nPos in range(0, len(cls.connection_configs)):
                if cls.connection_configs[nPos]["Database"].upper() == m_DriverName.strip().upper():
                    m_Config = cls.connection_configs[nPos]
                    m_Config["FullName"] = [m_DriverFullName, ]
                    bFound = True
                    cls.connection_configs[nPos] = m_Config
            if not bFound:
                raise SQLCliException("Driver not loaded. Please config it in configfile first.")
            yield {
                "title": None,
                "rows": None,
                "headers": None,
                "columntypes": None,
                "status": "Driver [" + m_DriverName.strip() + "] loaded."
            }
            return

        raise SQLCliException("Bad command.  loaddriver [database] [new jar name]")

    # 加载数据库SQL映射
    @staticmethod
    def load_sqlmap(cls, arg, **_):
        cls.SQLOptions.set("SQLREWRITE", "ON")
        cls.SQLMappingHandler.Load_SQL_Mappings(cls.sqlscript, arg)
        cls.sqlmap = arg
        yield {
            "title": None,
            "rows": None,
            "headers": None,
            "columntypes": None,
            "status": 'Mapping file loaded.'
        }

    # 连接数据库
    @staticmethod
    def connect_db(cls, arg, **_):
        # 如果当前的连接存在，且当前连接没有被保存，则断开当前的连接
        if cls.db_conn is not None:
            if cls.SessionName is None:
                # 如果之前数据库连接没有被保存，则强制断开连接
                cls.db_conn.close()
                cls.db_conn = None
                cls.SQLExecuteHandler.conn = None
            else:
                if cls.db_saved_conn[cls.SessionName][0] is None:
                    # 之前并没有保留数据库连接
                    cls.db_conn.close()
                    cls.db_conn = None
                    cls.SQLExecuteHandler.conn = None

        # 一旦开始数据库连接，则当前连接会被置空，以保证连接错误的影响能够对后续的语句产生作用
        cls.db_conn = None
        cls.SQLExecuteHandler.conn = None

        if arg is None or len(str(arg)) == 0:
            raise SQLCliException(
                "Missed required argument\n." + "connect [user name]/[password]@" +
                "jdbc|odbc:[db type]:[driver type]://[host]:[port]/[service name]")

        if cls.connection_configs is None:
            raise SQLCliException("Please load driver first.")

        # 如果连接内容仅仅就一个mem，则连接到内置的memory db
        if arg.strip().upper() == "MEM":
            arg = "X/X@jdbc:h2:mem://0.0.0.0:0/X"

        # 分割字符串，可能用单引号或者双引号包括, 单词中不能包含分割符
        # -- 1 首先找到@符号
        try:
            content = pyparsing.originalTextFor(
                pyparsing.OneOrMore(pyparsing.quotedString | pyparsing.Word(pyparsing.printables.replace('@', ''))))
            m_connect_parameterlist = \
                pyparsing.delimitedList(content, '@').parseString(arg)
            if len(m_connect_parameterlist) == 0:
                raise SQLCliException("Missed connect string in connect command.")
        except pyparsing.ParseException:
            raise SQLCliException("SQLCLI-00000: Connect failed. "
                                  "Please make use use correct string, quote multibyte string.")

        # -- 2 @符号之前的为用户名和密码
        m_userandpasslist = shlex.shlex(m_connect_parameterlist[0])
        m_userandpasslist.whitespace = '/'
        m_userandpasslist.quotes = '"'
        m_userandpasslist.whitespace_split = True
        m_userandpasslist = list(m_userandpasslist)
        if len(m_userandpasslist) == 0:
            raise SQLCliException("Missed user or password in connect command.")
        elif len(m_userandpasslist) == 1:
            # 用户写法是connect user xxx password xxxx; 密码可能包含引号
            m_userandpasslist = shlex.shlex(m_connect_parameterlist[0])
            m_userandpasslist.whitespace = ' '
            m_userandpasslist.quotes = '"'
            m_userandpasslist.whitespace_split = True
            m_userandpasslist = list(m_userandpasslist)
            if len(m_userandpasslist) == 4 and \
                    m_userandpasslist[0].upper() == "USER" and \
                    m_userandpasslist[2].upper() == "PASSWORD":
                cls.db_username = m_userandpasslist[1]
                cls.db_password = m_userandpasslist[3].replace("'", "").replace('"', "")
            else:
                raise SQLCliException("Missed user or password in connect command.")
        else:
            cls.db_username = m_userandpasslist[0].strip()
            if len(m_userandpasslist) == 2:
                cls.db_password = m_userandpasslist[1]
            else:
                cls.db_password = ""

        # 判断连接字符串中是否含有服务器信息
        if len(m_connect_parameterlist) == 2:
            m_serverurl = m_connect_parameterlist[1]
        else:
            if "SQLCLI_CONNECTION_URL" in os.environ:
                m_serverurl = os.environ['SQLCLI_CONNECTION_URL']
            else:
                raise SQLCliException("Missed SQLCLI_CONNECTION_URL in env.")

        # 初始化连接参数
        cls.db_service_name = ""
        cls.db_host = ""
        cls.db_port = ""
        m_jdbcprop = ""

        # 在serverurl中查找//
        m_serverurllist = shlex.shlex(m_serverurl)
        m_serverurllist.whitespace = '/'
        m_serverurllist.quotes = '"'
        m_serverurllist.whitespace_split = True
        m_serverurllist = list(m_serverurllist)
        if len(m_serverurllist) == 0:
            raise SQLCliException("Missed correct url in connect command.")
        if len(m_serverurllist) == 3:
            # //protocol/IP:Port/Service
            m_jdbcprop = m_serverurllist[0]
            cls.db_service_name = m_serverurllist[2]
            m_ipandhost = m_serverurllist[1]
            m_serverparameter = m_ipandhost.split(':')
            if len(m_serverparameter) > 2:
                # IP:Port|IP:Port|IP:Port
                cls.db_host = m_ipandhost
                cls.db_port = ""
            elif len(m_serverparameter) == 2:
                # IP:Port
                cls.db_host = m_serverparameter[0]
                cls.db_port = m_serverparameter[1]
            elif len(m_serverparameter) == 1:
                # IP  sqlserver/teradata
                cls.db_host = m_serverparameter[0]
                cls.db_port = ""
        elif len(m_serverurllist) == 2:
            # //protocol/IP:Port:Service
            m_jdbcprop = m_serverurllist[0]
            m_serverparameter = m_serverurllist[1].split(':')
            if len(m_serverparameter) == 3:
                # IP:Port:Service, Old oracle version
                cls.db_host = m_serverparameter[0]
                cls.db_port = m_serverparameter[1]
                cls.db_service_name = m_serverparameter[2]

        # 处理JDBC属性
        m_jdbcproplist = shlex.shlex(m_jdbcprop)
        m_jdbcproplist.whitespace = ':'
        m_jdbcproplist.quotes = '"'
        m_jdbcproplist.whitespace_split = True
        m_jdbcproplist = list(m_jdbcproplist)
        if len(m_jdbcproplist) < 2:
            raise SQLCliException("Missed jdbc prop in connect command.")
        cls.db_conntype = m_jdbcproplist[0].upper()
        if cls.db_conntype not in ['ODBC', 'JDBC']:
            raise SQLCliException("Unexpected connection type. Please use ODBC or JDBC.")
        cls.db_type = m_jdbcproplist[1]
        if len(m_jdbcproplist) == 3:
            cls.db_driver_type = m_jdbcproplist[2]
        else:
            cls.db_driver_type = ""

        # 连接数据库
        try:
            if cls.db_conntype == 'JDBC':  # JDBC 连接数据库
                # 加载所有的Jar包， 根据class的名字加载指定的文件
                m_JarList = []
                m_driverclass = ""
                m_JDBCURL = ""
                m_JDBCProp = ""
                for m_Jar_Config in cls.connection_configs:
                    m_JarList.extend(m_Jar_Config["FullName"])
                for m_Jar_Config in cls.connection_configs:
                    if m_Jar_Config["Database"].upper() == cls.db_type.upper():
                        m_driverclass = m_Jar_Config["ClassName"]
                        m_JDBCURL = m_Jar_Config["JDBCURL"]
                        m_JDBCProp = m_Jar_Config["JDBCProp"]
                        break
                if "SQLCLI_DEBUG" in os.environ:
                    print("Driver Jar List: " + str(m_JarList))
                if m_JDBCURL is None:
                    raise SQLCliException("Unknown database [" + cls.db_type.upper() + "]. Connect Failed. \n" +
                                          "Maybe you forgot download jlib files. ")
                m_JDBCURL = m_JDBCURL.replace("${host}", cls.db_host)
                m_JDBCURL = m_JDBCURL.replace("${driver_type}", cls.db_driver_type)
                if cls.db_port == "":
                    m_JDBCURL = m_JDBCURL.replace(":${port}", "")
                else:
                    m_JDBCURL = m_JDBCURL.replace("${port}", cls.db_port)
                m_JDBCURL = m_JDBCURL.replace("${service}", cls.db_service_name)
                if m_driverclass is None:
                    raise SQLCliException(
                        "Missed driver [" + cls.db_type.upper() + "] in config. Database Connect Failed. ")
                m_jdbcconn_prop = {'user': cls.db_username, 'password': cls.db_password}
                if m_JDBCProp is not None:
                    for row in m_JDBCProp.strip().split(','):
                        props = row.split(':')
                        if len(props) == 2:
                            m_PropName = str(props[0]).strip()
                            m_PropValue = str(props[1]).strip()
                            m_jdbcconn_prop[m_PropName] = m_PropValue
                # 将当前DB的连接字符串备份到变量中
                cls.SQLOptions.set("CONNURL", str(cls.db_url))
                cls.SQLOptions.set("CONNPROP", str(m_JDBCProp))
                if "SQLCLI_DEBUG" in os.environ:
                    print("Connect to [" + m_JDBCURL + "]...")
                cls.db_conn = jdbcconnect(
                    jclassname=m_driverclass,
                    url=m_JDBCURL, driver_args=m_jdbcconn_prop,
                    jars=m_JarList, sqloptions=cls.SQLOptions)
                cls.db_url = m_JDBCURL
                cls.SQLExecuteHandler.conn = cls.db_conn
            if cls.db_conntype == 'ODBC':  # ODBC 连接数据库
                m_ODBCURL = ""
                for m_Connection_Config in cls.connection_configs:
                    if m_Connection_Config["Database"].upper() == cls.db_type.upper():
                        m_ODBCURL = m_Connection_Config["ODBCURL"]
                        break
                if m_ODBCURL is None:
                    raise SQLCliException("Unknown database [" + cls.db_type.upper() + "]. Connect Failed. \n" +
                                          "Maybe you have a bad config file. ")
                m_ODBCURL = m_ODBCURL.replace("${host}", cls.db_host)
                m_ODBCURL = m_ODBCURL.replace("${port}", cls.db_port)
                m_ODBCURL = m_ODBCURL.replace("${service}", cls.db_service_name)
                m_ODBCURL = m_ODBCURL.replace("${username}", cls.db_username)
                m_ODBCURL = m_ODBCURL.replace("${password}", cls.db_password)

                cls.db_conn = odbcconnect(m_ODBCURL)
                cls.db_url = m_ODBCURL
                cls.SQLExecuteHandler.conn = cls.db_conn
                # 将当前DB的连接字符串备份到变量中
                cls.SQLOptions.set("CONNURL", str(cls.db_url))
        except SQLCliException as se:  # Connecting to a database fail.
            raise se
        except SQLCliODBCException as soe:
            raise soe
        except Exception as e:  # Connecting to a database fail.
            if "SQLCLI_DEBUG" in os.environ:
                print('traceback.print_exc():\n%s' % traceback.print_exc())
                print('traceback.format_exc():\n%s' % traceback.format_exc())
                print("db_user = [" + str(cls.db_username) + "]")
                print("db_pass = [" + str(cls.db_password) + "]")
                print("db_type = [" + str(cls.db_type) + "]")
                print("db_host = [" + str(cls.db_host) + "]")
                print("db_port = [" + str(cls.db_port) + "]")
                print("db_service_name = [" + str(cls.db_service_name) + "]")
                print("db_url = [" + str(cls.db_url) + "]")
                print("jar_file = [" + str(cls.connection_configs) + "]")
            if str(e).find("SQLInvalidAuthorizationSpecException") != -1:
                raise SQLCliException(str(jpype.java.sql.SQLInvalidAuthorizationSpecException(e).getCause()))
            else:
                raise SQLCliException(str(e))
        yield {
            "title": None,
            "rows": None,
            "headers": None,
            "columntypes": None,
            "status": 'Database connected.'
        }

    # 断开数据库连接
    @staticmethod
    def disconnect_db(cls, arg, **_):
        if arg:
            return [(None, None, None, None, "unnecessary parameter")]
        if cls.db_conn:
            cls.db_conn.close()
        cls.db_conn = None
        cls.SQLExecuteHandler.conn = None
        yield {
            "title": None,
            "rows": None,
            "headers": None,
            "columntypes": None,
            "status": 'Database disconnected.'
        }

    # 执行主机的操作命令
    @staticmethod
    def host(cls, arg, **_):
        if arg is None or len(str(arg)) == 0:
            raise SQLCliException(
                "Missing OS command\n." + "host xxx")
        Commands = str(arg)

        if 'win32' in str(sys.platform).lower():
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags = subprocess.CREATE_NEW_CONSOLE | subprocess.STARTF_USESHOWWINDOW
            startupinfo.wShowWindow = subprocess.SW_HIDE
            p = subprocess.Popen(Commands,
                                 shell=True,
                                 startupinfo=startupinfo,
                                 stdin=subprocess.PIPE,
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
        else:
            p = subprocess.Popen(Commands,
                                 shell=True,
                                 stdin=subprocess.PIPE,
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
        try:
            (stdoutdata, stderrdata) = p.communicate()
            yield {
                "title": None,
                "rows": None,
                "headers": None,
                "columntypes": None,
                "status": str(stdoutdata.decode(encoding=cls.Result_Charset))
            }
            yield {
                "title": None,
                "rows": None,
                "headers": None,
                "columntypes": None,
                "status": str(stderrdata.decode(encoding=cls.Result_Charset))
            }
        except UnicodeDecodeError:
            raise SQLCliException("The character set [" + cls.Result_Charset + "]" +
                                  " does not match the terminal character set, " +
                                  "so the terminal information cannot be output correctly.")

    # 数据库会话管理
    @staticmethod
    def session_manage(cls, arg, **_):
        if arg is None or len(str(arg)) == 0:
            raise SQLCliException(
                "Missing required argument: " + "Session save/saveurl/restore/release/show [session name]")
        m_Parameters = str(arg).split()

        # Session_Context:
        #   0:   Connection
        #   1:   UserName
        #   2:   Password
        #   3:   URL
        if len(m_Parameters) == 1 and m_Parameters[0] == 'show':
            m_Result = []
            for m_Session_Name, m_Connection in cls.db_saved_conn.items():
                if m_Connection[0] is None:
                    m_Result.append(['None', str(m_Session_Name), str(m_Connection[1]), '******', str(m_Connection[3])])
                else:
                    m_Result.append(['Connection', str(m_Session_Name), str(m_Connection[1]),
                                     '******', str(m_Connection[3])])
            if cls.db_conn is not None:
                m_Result.append(['Current', str(cls.SessionName), str(cls.db_username), '******', str(cls.db_url)])
            if len(m_Result) == 0:
                yield {
                    "title": None,
                    "rows": None,
                    "headers": None,
                    "columntypes": None,
                    "status": "No saved sesssions."
                }
            else:
                yield {
                    "title": "Saved Sessions:",
                    "rows": m_Result,
                    "headers": ["Session", "Sesssion Name", "User Name", "Password", "URL"],
                    "columntypes": None,
                    "status": "Total " + str(len(m_Result)) + " saved sesssions."
                }
            return

        # 要求两个参数 save/restore [session_name]
        if len(m_Parameters) != 2:
            raise SQLCliException(
                "Wrong argument : " + "Session save/restore/release [session name]")

        if m_Parameters[0] == 'release':
            if cls.db_conn is None:
                raise SQLCliException(
                    "You don't have a saved session.")
            m_Session_Name = m_Parameters[1]
            del cls.db_saved_conn[m_Session_Name]
            cls.SessionName = None
        elif m_Parameters[0] == 'save':
            if cls.db_conn is None:
                raise SQLCliException(
                    "Please connect session first before save.")
            m_Session_Name = m_Parameters[1]
            cls.db_saved_conn[m_Session_Name] = [cls.db_conn, cls.db_username, cls.db_password, cls.db_url]
            cls.SessionName = m_Session_Name
        elif m_Parameters[0] == 'saveurl':
            if cls.db_conn is None:
                raise SQLCliException(
                    "Please connect session first before save.")
            m_Session_Name = m_Parameters[1]
            cls.db_saved_conn[m_Session_Name] = [None, cls.db_username, cls.db_password, cls.db_url]
            cls.SessionName = m_Session_Name
        elif m_Parameters[0] == 'restore':
            m_Session_Name = m_Parameters[1]
            if m_Session_Name in cls.db_saved_conn:
                cls.db_username = cls.db_saved_conn[m_Session_Name][1]
                cls.db_password = cls.db_saved_conn[m_Session_Name][2]
                cls.db_url = cls.db_saved_conn[m_Session_Name][3]
                if cls.db_saved_conn[m_Session_Name][0] is None:
                    result = cls.connect_db(cls.db_username + "/" + cls.db_password + "@" + cls.db_url)
                    for title, cur, headers, columntypes, status in result:
                        yield {
                            "title": title,
                            "rows": cur,
                            "headers": headers,
                            "columntypes": columntypes,
                            "status": status
                        }
                else:
                    cls.db_conn = cls.db_saved_conn[m_Session_Name][0]
                    cls.SQLExecuteHandler.conn = cls.db_conn
                    cls.SessionName = m_Session_Name
            else:
                raise SQLCliException(
                    "Session [" + m_Session_Name + "] does not exist. Please save it first.")
        else:
            raise SQLCliException(
                "Wrong argument : " + "Session save/restore [session name]")
        if m_Parameters[0] == 'save':
            yield {
                "title": None,
                "rows": None,
                "headers": None,
                "columntypes": None,
                "status": "Session saved Successful."
            }
        if m_Parameters[0] == 'release':
            yield {
                "title": None,
                "rows": None,
                "headers": None,
                "columntypes": None,
                "status": "Session release Successful."
            }
        if m_Parameters[0] == 'restore':
            cls.SQLOptions.set("CONNURL", cls.db_url)
            yield {
                "title": None,
                "rows": None,
                "headers": None,
                "columntypes": None,
                "status": "Session restored Successful."
            }

    # 休息一段时间, 如果收到SHUTDOWN信号的时候，立刻终止SLEEP
    @staticmethod
    def sleep(cls, arg, **_):
        if not arg:
            message = "Missing required argument, sleep [seconds]."
            return [{
                "title": None,
                "rows": None,
                "headers": None,
                "columntypes": None,
                "status": message
            }]
        try:
            m_Sleep_Time = int(arg)
            if m_Sleep_Time <= 0:
                message = "Parameter must be a valid number, sleep [seconds]."
                return [{
                    "title": None,
                    "rows": None,
                    "headers": None,
                    "columntypes": None,
                    "status": message
                }]
            time.sleep(m_Sleep_Time)
        except ValueError:
            message = "Parameter must be a number, sleep [seconds]."
            return [{
                "title": None,
                "rows": None,
                "headers": None,
                "columntypes": None,
                "status": message
            }]
        return [{
            "title": None,
            "rows": None,
            "headers": None,
            "columntypes": None,
            "status": None
        }]

    # 从文件中执行SQL
    @staticmethod
    def execute_from_file(cls, arg, **_):
        if not arg:
            message = "Missing required argument, filename1,filename2,filename,3 [loop <loop times>]."
            yield {
                "title": None,
                "rows": None,
                "headers": None,
                "columntypes": None,
                "status": "Session restored Successful."
            }
            return

        # 分割字符串，考虑SQL文件名中包含空格的问题
        temp1 = str(arg).strip()
        temp1 = shlex.shlex(temp1)
        temp1.whitespace = ','
        temp1.whitespace_split = True
        m_SQLFileList = list(temp1)

        m_nLoop = 1
        if len(m_SQLFileList) >= 3 and \
                m_SQLFileList[len(m_SQLFileList) - 2].lower() == 'loop' and \
                m_SQLFileList[len(m_SQLFileList) - 1].isnumeric():
            m_nLoop = int(m_SQLFileList[len(m_SQLFileList) - 1])
            m_SQLFileList = m_SQLFileList[:-2]
        for m_curLoop in range(0, m_nLoop):
            for m_SQLFile in m_SQLFileList:
                try:
                    if str(m_SQLFile).startswith("'"):
                        m_SQLFile = m_SQLFile[1:]
                    if str(m_SQLFile).endswith("'"):
                        m_SQLFile = m_SQLFile[:-1]
                    with open(os.path.expanduser(m_SQLFile), encoding=cls.Client_Charset) as f:
                        query = f.read()

                    # 处理NLS文档头数据
                    if ord(query[0]) == 0xFEFF:
                        # 去掉SQL文件可能包含的UTF-BOM
                        query = query[1:]
                    if query[:3] == codecs.BOM_UTF8:
                        # 去掉SQL文件可能包含的UTF-BOM
                        query = query[3:]

                    # Scenario等Hint信息不会带入到下一个SQL文件中
                    cls.SQLExecuteHandler.SQLScenario = ''
                    cls.SQLExecuteHandler.SQLTransaction = ''

                    # 执行指定的SQL文件
                    for m_ExecuteResult in \
                            cls.SQLExecuteHandler.run(query, os.path.expanduser(m_SQLFile)):
                        # 记录命令结束的时间
                        yield m_ExecuteResult
                except IOError as e:
                    yield {
                        "title": None,
                        "rows": None,
                        "headers": None,
                        "columntypes": None,
                        "status": str(e)
                    }

    # 将当前及随后的输出打印到指定的文件中
    @staticmethod
    def spool(cls, arg, **_):
        if not arg:
            message = "Missing required argument, spool [filename]|spool off."
            return [(None, None, None, None, message)]
        parameters = str(arg).split()
        if parameters[0].strip().upper() == 'OFF':
            # close spool file
            if len(cls.SpoolFileHandler) == 0:
                yield {
                    "title": None,
                    "rows": None,
                    "headers": None,
                    "columntypes": None,
                    "status": "not spooling currently"
                }
                return
            else:
                cls.SpoolFileHandler[-1].close()
                cls.SpoolFileHandler.pop()
                yield {
                    "title": None,
                    "rows": None,
                    "headers": None,
                    "columntypes": None,
                    "status": None
                }
                return

        if cls.logfilename is not None:
            # 如果当前主程序启用了日志，则spool日志的默认输出目录为logfile的目录
            m_FileName = os.path.join(os.path.dirname(cls.logfilename), parameters[0].strip())
        else:
            # 如果主程序没有启用日志，则输出为当前目录
            m_FileName = parameters[0].strip()

        # 如果当前有打开的Spool文件，关闭它
        try:
            cls.SpoolFileHandler.append(open(m_FileName, "w", encoding=cls.Result_Charset))
        except IOError as e:
            raise SQLCliException("SQLCLI-00000: IO Exception " + repr(e))
        yield {
            "title": None,
            "rows": None,
            "headers": None,
            "columntypes": None,
            "status": None
        }
        return

    # 将当前及随后的屏幕输入存放到脚本文件中
    @staticmethod
    def echo_input(cls, arg, **_):
        if not arg:
            message = "Missing required argument, echo [filename]|echo off."
            yield {
                "title": None,
                "rows": None,
                "headers": None,
                "columntypes": None,
                "status": message
            }
            return
        parameters = str(arg).split()
        if parameters[0].strip().upper() == 'OFF':
            # close echo file
            if cls.EchoFileHandler is None:
                message = "not echo currently"
                return [{
                    "title": None,
                    "rows": None,
                    "headers": None,
                    "columntypes": None,
                    "status": message
                }]
            else:
                cls.EchoFileHandler.close()
                cls.EchoFileHandler = None
                cls.SQLExecuteHandler.echofile = None
                return [{
                    "title": None,
                    "rows": None,
                    "headers": None,
                    "columntypes": None,
                    "status": None
                }]

        # ECHO的输出默认为程序的工作目录
        m_FileName = parameters[0].strip()

        # 如果当前有打开的Echo文件，关闭它
        if cls.EchoFileHandler is not None:
            cls.EchoFileHandler.close()
        cls.EchoFileHandler = open(m_FileName, "w", encoding=cls.Result_Charset)
        cls.SQLExecuteHandler.echofile = cls.EchoFileHandler
        return [{
            "title": None,
            "rows": None,
            "headers": None,
            "columntypes": None,
            "status": None
        }]

    # 设置一些选项
    @staticmethod
    def set_options(cls, arg, **_):
        if arg is None:
            raise SQLCliException("Missing required argument. set parameter parameter_value.")
        elif arg == "":  # 显示所有的配置
            m_Result = []
            for row in cls.SQLOptions.getOptionList():
                if not row["Hidden"]:
                    m_Result.append([row["Name"], row["Value"], row["Comments"]])
            yield {
                "title": "Current Options: ",
                "rows": m_Result,
                "headers": ["Name", "Value", "Comments"],
                "columntypes": None,
                "status": None
            }
        else:
            options_parameters = str(arg).split()
            if len(options_parameters) == 1:
                # 如果没有设置参数，则补充一个None作为参数的值
                options_parameters.append("")

            # 处理DEBUG选项
            if options_parameters[0].upper() == "DEBUG":
                if options_parameters[1].upper() == 'ON':
                    os.environ['SQLCLI_DEBUG'] = "1"
                elif options_parameters[1].upper() == 'OFF':
                    if 'SQLCLI_DEBUG' in os.environ:
                        del os.environ['SQLCLI_DEBUG']
                else:
                    raise SQLCliException("SQLCLI-00000: "
                                          "Unknown option [" + str(options_parameters[1]) + "] for debug. ON/OFF only.")

            # 如果不是已知的选项，则直接抛出到SQL引擎
            if options_parameters[0].startswith('@'):
                m_ParameterValue = " ".join(options_parameters[1:])
                options_values = shlex.shlex(m_ParameterValue)
                options_values.whitespace = ' '
                options_values.quotes = '^'
                options_values.whitespace_split = True
                options_values = list(options_values)
                if len(options_values) == 1:
                    if options_values[0][0] == '^':
                        options_values[0] = options_values[0][1:]
                    if options_values[0][-1] == '^':
                        options_values[0] = options_values[0][:-1]
                    try:
                        option_value = str(eval(str(options_values[0])))
                    except (NameError, ValueError, SyntaxError):
                        option_value = str(options_values[0])
                    cls.SQLOptions.set(options_parameters[0], option_value)
                elif len(options_values) == 2:
                    if options_values[0][0] == '^':
                        options_values[0] = options_values[0][1:]
                    if options_values[0][-1] == '^':
                        options_values[0] = options_values[0][:-1]
                    if options_values[1][0] == '^':
                        options_values[1] = options_values[1][1:]
                    if options_values[1][-1] == '^':
                        options_values[1] = options_values[1][:-1]
                    try:
                        option_value1 = str(eval(str(options_values[0])))
                    except (NameError, ValueError, SyntaxError):
                        option_value1 = str(options_values[0])
                    try:
                        option_value2 = str(eval(str(options_values[1])))
                    except (NameError, ValueError, SyntaxError):
                        option_value2 = str(options_values[1])
                    cls.SQLOptions.set(options_parameters[0], option_value1, option_value2)
                else:
                    raise SQLCliException("SQLCLI-00000: "
                                          "Wrong set command. Please use [set @parameter_name parametervalue]")
                yield {
                    "title": None,
                    "rows": None,
                    "headers": None,
                    "columntypes": None,
                    "status": None
                }
            elif cls.SQLOptions.get(options_parameters[0].upper()) is not None:
                cls.SQLOptions.set(options_parameters[0].upper(), options_parameters[1])
                yield {
                    "title": None,
                    "rows": None,
                    "headers": None,
                    "columntypes": None,
                    "status": None
                }
            else:
                raise CommandNotFound

    # 执行特殊的命令
    @staticmethod
    def execute_internal_command(cls, arg, **_):
        # 处理并发JOB
        matchObj = re.match(r"(\s+)?job(.*)$", arg, re.IGNORECASE | re.DOTALL)
        if matchObj:
            (title, result, headers, columntypes, status) = cls.JobHandler.Process_Command(arg)
            yield {
                "title": title,
                "rows": result,
                "headers": headers,
                "columntypes": columntypes,
                "status": status
            }
            return

        # 处理Transaction
        matchObj = re.match(r"(\s+)?transaction(.*)$", arg, re.IGNORECASE | re.DOTALL)
        if matchObj:
            (title, result, headers, columntypes, status) = cls.TransactionHandler.Process_Command(arg)
            yield {
                "title": title,
                "rows": result,
                "headers": headers,
                "columntypes": columntypes,
                "status": status
            }
            return

        # 处理kafka数据
        matchObj = re.match(r"(\s+)?kafka(.*)$", arg, re.IGNORECASE | re.DOTALL)
        if matchObj:
            (title, result, headers, columntypes, status) = cls.KafkaHandler.Process_SQLCommand(arg)
            yield {
                "title": title,
                "rows": result,
                "headers": headers,
                "columntypes": columntypes,
                "status": status
            }
            return

        # 测试管理
        matchObj = re.match(r"(\s+)?test(.*)$", arg, re.IGNORECASE | re.DOTALL)
        if matchObj:
            (title, result, headers, columntypes, status) = cls.TestHandler.Process_SQLCommand(arg)
            yield {
                "title": title,
                "rows": result,
                "headers": headers,
                "columntypes": columntypes,
                "status": status
            }
            return

        # 处理HDFS数据
        matchObj = re.match(r"(\s+)?hdfs(.*)$", arg, re.IGNORECASE | re.DOTALL)
        if matchObj:
            (title, result, headers, columntypes, status) = cls.HdfsHandler.Process_SQLCommand(arg)
            yield {
                "title": title,
                "rows": result,
                "headers": headers,
                "columntypes": columntypes,
                "status": status
            }
            return

        # 处理随机数据文件
        matchObj = re.match(r"(\s+)?data(.*)$", arg, re.IGNORECASE | re.DOTALL)
        if matchObj:
            for (title, result, headers, columntypes, status) in \
                    cls.DataHandler.Process_SQLCommand(arg, cls.Result_Charset):
                yield {
                    "title": title,
                    "rows": result,
                    "headers": headers,
                    "columntypes": columntypes,
                    "status": status
                }
            return

        # 不认识的internal命令
        raise SQLCliException("Unknown internal Command [" + str(arg) + "]. Please double check.")

    # 逐条处理SQL语句
    # 如果执行成功，返回true
    # 如果执行失败，返回false
    def DoSQL(self, text=None):
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
                except PermissionError:
                    self.echo("SQLCli Can't work without valid terminal. "
                              "Use \"--execute\" in case you need run script", err=True, fg="red")
                    return False

                # 拼接SQL语句
                if full_text is None:
                    full_text = text
                else:
                    full_text = full_text + '\n' + text
                # 判断SQL语句是否已经结束
                (ret_bSQLCompleted, ret_SQLSplitResults, ret_SQLSplitResultsWithComments, _) = \
                    SQLAnalyze(full_text)
                if ret_bSQLCompleted:
                    # SQL 语句已经结束
                    break

            if len("".join(ret_SQLSplitResults).strip()) == 0 and \
                    len("".join(ret_SQLSplitResultsWithComments).strip()) != 0:
                # 如果一行完全是注释，则记录改行信息，传递给下一行可执行的SQL
                if self.m_LastComment is not None:
                    self.m_LastComment = self.m_LastComment + "\n" + "\n".join(ret_SQLSplitResultsWithComments)
                else:
                    self.m_LastComment = "\n".join(ret_SQLSplitResultsWithComments)
                return True
            else:
                # 如果文本是空行，直接跳过
                if not text.strip():
                    return True
                # 记录需要执行的SQL，包含之前保留的注释部分，传递给执行程序
                if self.m_LastComment is None:
                    text = full_text
                else:
                    text = self.m_LastComment + '\n' + full_text
                    self.m_LastComment = None

        try:
            def show_result(p_result):
                # 输出显示结果
                if "type" in p_result.keys():
                    if p_result["type"] == "echo":
                        message = p_result["message"]
                        m_EchoFlag = OFLAG_LOGFILE | OFLAG_LOGGER | OFLAG_CONSOLE | OFLAG_ECHO | OFLAG_SPOOL
                        if re.match(r'echo\s+off', p_result["message"], re.IGNORECASE):
                            # Echo Off这个语句，不打印到Echo文件中
                            m_EchoFlag = m_EchoFlag & ~OFLAG_ECHO
                        if p_result["script"] is None:
                            # 控制台应用，不再打印SQL语句到控制台（因为用户已经输入了)
                            m_EchoFlag = m_EchoFlag & ~OFLAG_CONSOLE
                        self.echo(message, m_EchoFlag)
                    elif p_result["type"] == "parse":
                        # 首先打印原始SQL
                        m_EchoFlag = OFLAG_LOGFILE | OFLAG_LOGGER | OFLAG_CONSOLE | OFLAG_SPOOL
                        if re.match(r'spool\s+.*', p_result["rawsql"], re.IGNORECASE):
                            # Spool off这个语句，不打印到Spool中
                            m_EchoFlag = m_EchoFlag & ~OFLAG_SPOOL
                        if p_result["script"] is None:
                            # 控制台应用，不再打印SQL语句到控制台（因为用户已经输入了)
                            m_EchoFlag = m_EchoFlag & ~OFLAG_CONSOLE
                        if p_result["formattedsql"] is not None:
                            self.echo(p_result["formattedsql"], m_EchoFlag)
                        # 打印改写后的SQL
                        if len(p_result["rewrotedsql"]) != 0:
                            m_EchoFlag = OFLAG_LOGFILE | OFLAG_LOGGER | OFLAG_CONSOLE | OFLAG_SPOOL
                            message = "\n".join(p_result["rewrotedsql"])
                            self.echo(message, m_EchoFlag)
                    elif p_result["type"] == "result":
                        title = p_result["title"]
                        cur = p_result["rows"]
                        headers = p_result["headers"]
                        columntypes = p_result["columntypes"]
                        status = p_result["status"]

                        # 不控制每行的长度
                        max_width = None

                        # title 包含原有语句的SQL信息，如果ECHO打开的话
                        # headers 包含原有语句的列名
                        # cur 是语句的执行结果
                        # output_format 输出格式
                        #   ascii              默认，即表格格式(第三方工具实现，暂时保留以避免不兼容现象)
                        #   csv                csv格式显示
                        #   tab                表格形式（用format_output_tab自己编写)
                        formatted = self.format_output(
                            title, cur, headers, columntypes,
                            self.SQLOptions.get("OUTPUT_FORMAT").lower(),
                            max_width
                        )

                        # 输出显示信息
                        try:
                            self.output(formatted, status)
                        except KeyboardInterrupt:
                            # 显示过程中用户按下了CTRL+C
                            pass
                    elif p_result["type"] == "error":
                        self.echo(p_result["message"])
                    elif p_result["type"] == "statistics":
                        self.Log_Statistics(p_result)
                    else:
                        raise SQLCliException("internal error. unknown sql type error. " + str(p_result["type"]))
                else:
                    raise SQLCliException("internal error. incomplete return. missed type" + str(p_result))

            # End Of show_result

            if "SQLCLI_REMOTESERVER" in os.environ and \
                    text.strip().upper() not in ("EXIT", "QUIT"):
                def run_remote():
                    async def test_ws_quote():
                        async with websockets.connect("ws://" + os.environ["SQLCLI_REMOTESERVER"] + "/DoCommand") \
                                as websocket:
                            request_data = json.dumps(
                                {
                                    'clientid': str(self.ClientID),
                                    'op': 'execute',
                                    'command': str(text)
                                })
                            await websocket.send(request_data)
                            while True:
                                try:
                                    ret = await websocket.recv()
                                    json_ret = json.loads(ret)
                                    show_result(json_ret)
                                except websockets.ConnectionClosedOK as wc:
                                    return True

                    asyncio.get_event_loop().run_until_complete(test_ws_quote())
                    return True

                result = run_remote()
            else:   # 本地执行脚本
                # 执行指定的SQL
                for result in self.SQLExecuteHandler.run(text):
                    # 打印结果
                    show_result(result)
                    if result["type"] == "statistics":
                        if self.SQLOptions.get('TIMING').upper() == 'ON':
                            self.echo('Running time elapsed: %9.2f Seconds' % result["elapsed"])
                        if self.SQLOptions.get('TIME').upper() == 'ON':
                            self.echo('Current clock time  :' + strftime("%Y-%m-%d %H:%M:%S", localtime()))

            # 返回正确执行的消息
            return True
        except EOFError as e:
            # 当调用了exit或者quit的时候，会收到EOFError，这里直接抛出
            raise e
        except SQLCliException as e:
            if "SQLCLI_DEBUG" in os.environ:
                print('traceback.print_exc():\n%s' % traceback.print_exc())
                print('traceback.format_exc():\n%s' % traceback.format_exc())
            # 用户执行的SQL出了错误, 由于SQLExecute已经打印了错误消息，这里直接退出
            self.output(None, e.message)
            if self.SQLOptions.get("WHENEVER_SQLERROR").upper() == "EXIT":
                raise e
        except Exception as e:
            if "SQLCLI_DEBUG" in os.environ:
                print('traceback.print_exc():\n%s' % traceback.print_exc())
                print('traceback.format_exc():\n%s' % traceback.format_exc())
            self.echo(repr(e), err=True, fg="red")
            return False

    # 下载程序所需要的各种Jar包
    def syncdriver(self):
        # 加载程序的配置文件
        self.AppOptions = configparser.ConfigParser()
        m_conf_filename = os.path.join(os.path.dirname(__file__), "conf", "sqlcli.ini")
        if os.path.exists(m_conf_filename):
            self.AppOptions.read(m_conf_filename)

        # 下载运行需要的各种Jar包
        for row in self.AppOptions.items("driver"):
            print("Checking driver [" + row[0] + "] ... ")
            for m_driversection in str(row[1]).split(','):
                m_driversection = m_driversection.strip()
                try:
                    m_driver_filename = self.AppOptions.get(m_driversection, "filename")
                    m_driver_downloadurl = self.AppOptions.get(m_driversection, "downloadurl")
                    m_driver_filemd5 = self.AppOptions.get(m_driversection, "md5")

                    m_LocalJarFile = os.path.join(os.path.dirname(__file__), "jlib", m_driver_filename)
                    m_LocalJarPath = os.path.join(os.path.dirname(__file__), "jlib")
                    if not os.path.isdir(m_LocalJarPath):
                        os.makedirs(m_LocalJarPath)

                    if os.path.exists(m_LocalJarFile):
                        with open(m_LocalJarFile, 'rb') as fp:
                            data = fp.read()
                        file_md5 = hashlib.md5(data).hexdigest()
                        if "SQLCLI_DEBUG" in os.environ:
                            print("File=[" + m_driver_filename + "], MD5=[" + file_md5 + "]")
                    else:
                        if "SQLCLI_DEBUG" in os.environ:
                            print("File=[" + m_driver_filename + "] does not exist!")
                        file_md5 = ""
                    if file_md5 != m_driver_filemd5.strip():
                        print("Driver [" + m_driversection + "], need upgrade ...")
                        # 重新下载新的文件到本地
                        try:
                            http = urllib3.PoolManager()
                            with http.request('GET', m_driver_downloadurl, preload_content=False) as r, \
                                    open(m_LocalJarFile, 'wb') as out_file:
                                shutil.copyfileobj(r, out_file)
                        except URLError:
                            print('traceback.print_exc():\n%s' % traceback.print_exc())
                            print('traceback.format_exc():\n%s' % traceback.format_exc())
                            print("")
                            print("Driver [" + m_driversection + "] download failed.")
                            continue
                        with open(m_LocalJarFile, 'rb') as fp:
                            data = fp.read()
                        file_md5 = hashlib.md5(data).hexdigest()
                        if file_md5 != m_driver_filemd5.strip():
                            print("Driver [" + m_driversection + "] consistent check failed. "
                                                                 "Remote MD5=[" + str(file_md5) + "]")
                        else:
                            print("Driver [" + m_driversection + "] is up-to-date.")
                    else:
                        print("Driver [" + m_driversection + "] is up-to-date.")
                except (configparser.NoSectionError, configparser.NoOptionError):
                    print("Bad driver config [" + m_driversection + "], Skip it ...")

    # 主程序
    def run_cli(self):
        # 如果参数要求不显示版本，则不再显示版本
        if not self.nologo:
            self.echo("SQLCli Release " + __version__)

        # 如果运行在脚本方式下，不在调用PromptSession, 调用PromptSession会导致程序在IDE下无法运行
        # 运行在无终端的模式下，也不会调用PromptSession, 调用PromptSession会导致程序出现Console错误
        # 对于脚本程序，在执行脚本完成后就会自动退出
        if self.sqlscript is None and not self.HeadlessMode:
            self.prompt_app = PromptSession()

        # 设置主程序的标题，随后开始运行程序
        m_Cli_ProcessTitleBak = setproctitle.getproctitle()
        setproctitle.setproctitle('SQLCli MAIN ' + " Script:" + str(self.sqlscript))

        # 开始依次处理SQL语句
        try:
            # 如果用户指定了用户名，口令，尝试直接进行数据库连接
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
            else:
                # 循环从控制台读取命令
                while True:
                    if not self.DoSQL():
                        m_runCli_Result = False
                        raise EOFError
        except (SQLCliException, EOFError):
            # 如果还有活动的事务，标记事务为失败信息
            m_TransactionNames = []
            for transaction_name in self.TransactionHandler.transactions.keys():
                m_TransactionNames.append(transaction_name)
            for transaction_name in m_TransactionNames:
                self.TransactionHandler.TransactionFail(transaction_name)
            # SQLCliException只有在被设置了WHENEVER_SQLERROR为EXIT的时候，才会被捕获到

        # 退出进程
        self.echo("Disconnected.")

        # 关闭LogFile
        if self.logfile is not None:
            self.logfile.flush()
            self.logfile.close()
            self.logfile = None

        # 还原进程标题
        setproctitle.setproctitle(m_Cli_ProcessTitleBak)

    def echo(self, s,
             Flags=OFLAG_LOGFILE | OFLAG_LOGGER | OFLAG_CONSOLE | OFLAG_SPOOL | OFLAG_ECHO,
             **kwargs):
        # 输出目的地
        # 1：  程序日志文件 logfile
        # 2：  程序的logger，用于在第三方调用时候的Console显示
        # 3：  当前屏幕控制台
        # 4：  程序的Spool文件
        # 5:   程序的ECHO回显文件
        if self.SQLOptions.get("SILENT").upper() != 'ON':
            if Flags & OFLAG_LOGFILE:
                if self.SQLOptions.get("ECHO").upper() == 'ON' and self.logfile is not None:
                    print(s, file=self.logfile)
                    self.logfile.flush()
            if Flags & OFLAG_SPOOL:
                if self.SQLOptions.get("ECHO").upper() == 'ON' and self.SpoolFileHandler is not None:
                    for m_SpoolFileHandler in self.SpoolFileHandler:
                        print(s, file=m_SpoolFileHandler)
                        m_SpoolFileHandler.flush()
            if Flags & OFLAG_LOGGER:
                if self.logger is not None:
                    self.logger.info(s)
            if Flags & OFLAG_ECHO:
                if self.EchoFileHandler is not None:
                    print(s, file=self.EchoFileHandler)
                    self.EchoFileHandler.flush()
            if Flags & OFLAG_CONSOLE:
                try:
                    click.secho(s, **kwargs, file=self.Console)
                except UnicodeEncodeError as ue:
                    # Unicode Error, This is console issue, Skip
                    if "SQLCLI_DEBUG" in os.environ:
                        print("Console output error:: " + repr(ue))

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
            output_via_pager = (self.SQLOptions.get("PAGE").upper() == "ON")
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

    def format_output_csv(self, headers, columntypes, cur):
        # 将屏幕输出按照CSV格式进行输出
        m_csv_delimiter = self.SQLOptions.get("CSV_DELIMITER")
        m_csv_quotechar = self.SQLOptions.get("CSV_QUOTECHAR")
        if m_csv_delimiter.find("\\t") != -1:
            m_csv_delimiter = m_csv_delimiter.replace("\\t", '\t')
        if m_csv_delimiter.find("\\s") != -1:
            m_csv_delimiter = m_csv_delimiter.replace("\\s", ' ')

        # 打印字段名称
        if self.SQLOptions.get("CSV_HEADER") == "ON":
            m_row = ""
            for m_nPos in range(0, len(headers)):
                m_row = m_row + str(headers[m_nPos])
                if m_nPos != len(headers) - 1:
                    m_row = m_row + m_csv_delimiter
            yield str(m_row)

        # 打印字段内容
        for row in cur:
            m_row = ""
            for m_nPos in range(0, len(row)):
                if row[m_nPos] is None:
                    if columntypes is not None:
                        if columntypes[m_nPos] == "str":
                            m_row = m_row + m_csv_quotechar + m_csv_quotechar
                else:
                    if columntypes is None:
                        m_row = m_row + str(row[m_nPos])
                    else:
                        if columntypes[m_nPos] == "str":
                            m_row = m_row + m_csv_quotechar + str(row[m_nPos]) + m_csv_quotechar
                        else:
                            m_row = m_row + str(row[m_nPos])
                if m_nPos != len(row) - 1:
                    m_row = m_row + m_csv_delimiter
            yield str(m_row)

    def format_output_leagcy(self, headers, columntypes, cur):
        # 这个函数完全是为了兼容旧的tab格式
        def wide_chars(s):
            # 判断字符串中包含的中文字符数量
            if isinstance(s, str):
                # W  宽字符
                # F  全角字符
                # H  半角字符
                # Na  窄字符
                # A   不明确的
                # N   正常字符
                return sum(unicodedata.east_asian_width(x) in ['W', 'F'] for x in s)
            else:
                return 0

        if self:
            pass
        # 将屏幕输出按照表格进行输出
        # 记录每一列的最大显示长度
        m_ColumnLength = []
        # 首先将表头的字段长度记录其中
        for m_Header in headers:
            m_ColumnLength.append(len(m_Header) + wide_chars(m_Header))
        # 查找列的最大字段长度
        for m_Row in cur:
            for m_nPos in range(0, len(m_Row)):
                if m_Row[m_nPos] is None:
                    # 空值打印为<null>
                    if m_ColumnLength[m_nPos] < len('<null>'):
                        m_ColumnLength[m_nPos] = len('<null>')
                elif isinstance(m_Row[m_nPos], str):
                    m_PrintValue = repr(m_Row[m_nPos])
                    if m_PrintValue.startswith("'") and m_PrintValue.endswith("'"):
                        m_PrintValue = m_PrintValue[1:-1]
                    elif m_PrintValue.startswith('"') and m_PrintValue.endswith('"'):
                        m_PrintValue = m_PrintValue[1:-1]
                    if len(m_PrintValue) + wide_chars(m_PrintValue) > m_ColumnLength[m_nPos]:
                        # 为了保持长度一致，长度计算的时候扣掉中文的显示长度
                        m_ColumnLength[m_nPos] = len(m_PrintValue) + wide_chars(m_PrintValue)
                else:
                    if len(str(m_Row[m_nPos])) + wide_chars(m_Row[m_nPos]) > m_ColumnLength[m_nPos]:
                        m_ColumnLength[m_nPos] = len(str(m_Row[m_nPos])) + wide_chars(m_Row[m_nPos])
        # 打印表格上边框
        # 计算表格输出的长度, 开头有一个竖线，随后每个字段内容前有一个空格，后有一个空格加上竖线
        # 1 + [（字段长度+3） *]
        m_TableBoxLine = '+'
        for m_Length in m_ColumnLength:
            m_TableBoxLine = m_TableBoxLine + (m_Length + 2) * '-' + '+'
        yield m_TableBoxLine
        # 打印表头以及表头下面的分割线
        m_TableContentLine = '|'
        for m_nPos in range(0, len(headers)):
            m_TableContentLine = m_TableContentLine + ' ' + \
                                 str(headers[m_nPos]).ljust(m_ColumnLength[m_nPos] - wide_chars(headers[m_nPos])) + ' |'
        yield m_TableContentLine
        yield m_TableBoxLine
        # 打印字段内容
        m_RowNo = 0
        for m_Row in cur:
            m_output = []
            m_output.append(m_Row)
            for m_iter in m_output:
                m_TableContentLine = '|'
                for m_nPos in range(0, len(m_iter)):
                    if m_iter[m_nPos] is None:
                        m_PrintValue = '<null>'
                    elif isinstance(m_iter[m_nPos], str):
                        m_PrintValue = repr(m_iter[m_nPos])
                        if m_PrintValue.startswith("'") and m_PrintValue.endswith("'"):
                            m_PrintValue = m_PrintValue[1:-1]
                        elif m_PrintValue.startswith('"') and m_PrintValue.endswith('"'):
                            m_PrintValue = m_PrintValue[1:-1]
                    else:
                        m_PrintValue = str(m_iter[m_nPos])
                    # 所有内容字符串左对齐
                    m_TableContentLine = \
                        m_TableContentLine + ' ' + \
                        m_PrintValue.ljust(m_ColumnLength[m_nPos] - wide_chars(m_PrintValue)) + ' |'
                yield m_TableContentLine
        # 打印表格下边框
        yield m_TableBoxLine

    def format_output_tab(self, headers, columntypes, cur):
        def wide_chars(s):
            # 判断字符串中包含的中文字符数量
            if isinstance(s, str):
                # W  宽字符
                # F  全角字符
                # H  半角字符
                # Na  窄字符
                # A   不明确的
                # N   正常字符
                return sum(unicodedata.east_asian_width(x) in ['W', 'F'] for x in s)
            else:
                return 0

        if self:
            pass
        # 将屏幕输出按照表格进行输出
        # 记录每一列的最大显示长度
        m_ColumnLength = []
        # 首先将表头的字段长度记录其中
        for m_Header in headers:
            m_ColumnLength.append(len(m_Header) + wide_chars(m_Header))
        # 查找列的最大字段长度
        for m_Row in cur:
            for m_nPos in range(0, len(m_Row)):
                if m_Row[m_nPos] is None:
                    # 空值打印为<null>
                    if m_ColumnLength[m_nPos] < len('<null>'):
                        m_ColumnLength[m_nPos] = len('<null>')
                elif isinstance(m_Row[m_nPos], str):
                    for m_iter in m_Row[m_nPos].split('\n'):
                        if len(m_iter) + wide_chars(m_iter) > m_ColumnLength[m_nPos]:
                            # 为了保持长度一致，长度计算的时候扣掉中文的显示长度
                            m_ColumnLength[m_nPos] = len(m_iter) + wide_chars(m_iter)
                else:
                    if len(str(m_Row[m_nPos])) + wide_chars(m_Row[m_nPos]) > m_ColumnLength[m_nPos]:
                        m_ColumnLength[m_nPos] = len(str(m_Row[m_nPos])) + wide_chars(m_Row[m_nPos])
        # 打印表格上边框
        # 计算表格输出的长度, 开头有一个竖线，随后每个字段内容前有一个空格，后有一个空格加上竖线
        # 1 + [（字段长度+3） *]
        m_TableBoxLine = '+--------+'
        for m_Length in m_ColumnLength:
            m_TableBoxLine = m_TableBoxLine + (m_Length + 2) * '-' + '+'
        yield m_TableBoxLine
        # 打印表头以及表头下面的分割线
        m_TableContentLine = '|   ##   |'
        for m_nPos in range(0, len(headers)):
            m_TableContentLine = \
                m_TableContentLine + \
                ' ' + str(headers[m_nPos]).center(m_ColumnLength[m_nPos] - wide_chars(headers[m_nPos])) + ' |'
        yield m_TableContentLine
        yield m_TableBoxLine
        # 打印字段内容
        m_RowNo = 0
        for m_Row in cur:
            m_RowNo = m_RowNo + 1
            # 首先计算改行应该打印的高度（行中的内容可能右换行符号）
            m_RowHeight = 1
            for m_nPos in range(0, len(m_Row)):
                if isinstance(m_Row[m_nPos], str):
                    if len(m_Row[m_nPos].split('\n')) > m_RowHeight:
                        m_RowHeight = len(m_Row[m_nPos].split('\n'))
            # 首先构造一个空的结果集，行数为计划打印的行高
            m_output = []
            if m_RowHeight == 1:
                m_output.append(m_Row)
            else:
                for m_iter in range(0, m_RowHeight):
                    m_output.append(())
                # 依次填入数据
                for m_nPos in range(0, len(m_Row)):
                    m_SplitRow = ()
                    if isinstance(m_Row[m_nPos], str):
                        m_SplitColumnValue = m_Row[m_nPos].split('\n')
                    else:
                        m_SplitColumnValue = [m_Row[m_nPos], ]
                    for m_iter in range(0, m_RowHeight):
                        if len(m_SplitColumnValue) > m_iter:
                            if str(m_SplitColumnValue[m_iter]).endswith('\r'):
                                m_SplitColumnValue[m_iter] = m_SplitColumnValue[m_iter][:-1]
                            m_output[m_iter] = m_output[m_iter] + (m_SplitColumnValue[m_iter],)
                        else:
                            m_output[m_iter] = m_output[m_iter] + ("",)
            m_RowNoPrinted = False
            for m_iter in m_output:
                m_TableContentLine = '|'
                if not m_RowNoPrinted:
                    m_TableContentLine = m_TableContentLine + str(m_RowNo).rjust(7) + ' |'
                    m_RowNoPrinted = True
                else:
                    m_TableContentLine = m_TableContentLine + '        |'
                for m_nPos in range(0, len(m_iter)):
                    if m_iter[m_nPos] is None:
                        m_PrintValue = '<null>'
                    else:
                        m_PrintValue = str(m_iter[m_nPos])
                    if columntypes is not None:
                        if columntypes[m_nPos] == "str":
                            # 字符串左对齐
                            m_TableContentLine = \
                                m_TableContentLine + ' ' + \
                                m_PrintValue.ljust(m_ColumnLength[m_nPos] - wide_chars(m_PrintValue)) + ' |'
                        else:
                            # 数值类型右对齐
                            m_TableContentLine = m_TableContentLine + ' ' + \
                                                 m_PrintValue.rjust(m_ColumnLength[m_nPos]) + ' |'
                    else:
                        # 没有返回columntype, 按照字符串处理
                        m_TableContentLine = \
                            m_TableContentLine + ' ' + \
                            m_PrintValue.ljust(m_ColumnLength[m_nPos] - wide_chars(m_PrintValue)) + ' |'
                yield m_TableContentLine
        # 打印表格下边框
        yield m_TableBoxLine

    def format_output(self, title, cur, headers, columntypes, p_format_name, max_width=None):
        output = []

        if title:  # Only print the title if it's not None.
            output = itertools.chain(output, [title])

        if cur:
            if max_width is not None:
                cur = list(cur)

            if p_format_name.upper() == 'CSV':
                # 按照CSV格式输出查询结果
                formatted = self.format_output_csv(headers, columntypes, cur)
            elif p_format_name.upper() == 'TAB':
                # 按照TAB格式输出查询结果
                formatted = self.format_output_tab(headers, columntypes, cur)
            elif p_format_name.upper() == 'LEGACY':
                # 按照TAB格式输出查询结果
                formatted = self.format_output_leagcy(headers, columntypes, cur)
            else:
                raise SQLCliException("SQLCLI-0000: Unknown output_format. CSV|TAB|LEGACY only")
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

    def Log_Statistics(self, p_SQLResult):
        # 开始时间         StartedTime
        # 消耗时间         elapsed
        # SQL的前20个字母  SQLPrefix
        # 运行状态         SQLStatus
        # 错误日志         ErrorMessage
        # 线程名称         thread_name

        # 如果没有打开性能日志记录文件，直接跳过
        if self.SQLPerfFile is None:
            return

        # 初始化文件加锁机制
        if self.PerfFileLocker is None:
            self.PerfFileLocker = Lock()

        # 多进程，多线程写入，考虑锁冲突
        try:
            self.PerfFileLocker.acquire()
            if not os.path.exists(self.SQLPerfFile):
                # 如果文件不存在，创建文件，并写入文件头信息
                self.SQLPerfFileHandle = open(self.SQLPerfFile, "a", encoding="utf-8")
                self.SQLPerfFileHandle.write("Script\tStarted\telapsed\tRAWSQL\tSQL\t"
                                             "SQLStatus\tErrorMessage\tworker_name\t"
                                             "Scenario\tTransaction\n")
                self.SQLPerfFileHandle.close()

            # 对于多线程运行，这里的thread_name格式为JOB_NAME#副本数-完成次数
            # 对于单线程运行，这里的thread_name格式为固定的MAIN
            m_ThreadName = str(p_SQLResult["thread_name"])

            # 打开Perf文件
            self.SQLPerfFileHandle = open(self.SQLPerfFile, "a", encoding="utf-8")
            # 写入内容信息
            if self.sqlscript is None:
                m_SQL_Script = "Console"
            else:
                m_SQL_Script = str(os.path.basename(self.sqlscript))
            self.SQLPerfFileHandle.write(
                m_SQL_Script + "\t" +
                time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(p_SQLResult["StartedTime"])) + "\t" +
                "%8.2f" % p_SQLResult["elapsed"] + "\t" +
                str(p_SQLResult["RAWSQL"]).replace("\n", " ").replace("\t", "    ") + "\t" +
                str(p_SQLResult["SQL"]).replace("\n", " ").replace("\t", "    ") + "\t" +
                str(p_SQLResult["SQLStatus"]) + "\t" +
                str(p_SQLResult["ErrorMessage"]).replace("\n", " ").replace("\t", "    ") + "\t" +
                str(m_ThreadName) + "\t" +
                str(p_SQLResult["Scenario"]) + "\t" +
                str(p_SQLResult["Transaction"]) +
                "\n"
            )
            self.SQLPerfFileHandle.flush()
            self.SQLPerfFileHandle.close()
        except Exception as ex:
            print("Internal error:: perf file write not complete. " + repr(ex))
        finally:
            self.PerfFileLocker.release()
