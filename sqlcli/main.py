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
import wget
import hashlib
import codecs
import subprocess
import pyparsing
from urllib.error import URLError
import itertools
from cli_helpers.tabular_output import TabularOutputFormatter, preprocessors
from prompt_toolkit.shortcuts import PromptSession
from multiprocessing.managers import BaseManager
import unicodedata

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
            WorkerName='MAIN',
            logger=None,
            clientcharset='UTF-8',
            resultcharset='UTF-8',
            SharedProcessInfo=None,
            profile=None,
    ):
        self.db_saved_conn = {}                          # 数据库Session备份
        self.SQLMappingHandler = SQLMapping()            # 函数句柄，处理SQLMapping信息
        self.SQLExecuteHandler = SQLExecute()            # 函数句柄，具体来执行SQL
        self.SQLOptions = SQLOptions()                   # 程序运行中各种参数
        self.KafkaHandler = KafkaWrapper()               # Kafka消息管理器
        self.TestHandler = TestWrapper()                 # 测试管理
        self.HdfsHandler = HDFSWrapper()                 # HDFS文件操作
        self.JobHandler = JOBManager()                   # 并发任务管理器
        self.TransactionHandler = TransactionManager()   # 事务管理器
        self.DataHandler = DataWrapper()                 # 随机临时数处理
        self.SpoolFileHandler = []                       # Spool文件句柄, 是一个数组，可能发生嵌套
        self.EchoFileHandler = None                      # 当前回显文件句柄
        self.AppOptions = None                           # 应用程序的配置参数
        self.Encoding = None                             # 应用程序的Encoding信息
        self.prompt_app = None                           # PromptKit控制台
        self.db_conn = None                              # 当前应用的数据库连接句柄
        self.SessionName = None                          # 当前会话的Session的名字
        self.db_conntype = None                          # 数据库连接方式，  JDBC或者是ODBC
        self.echofilename = None                         # 当前回显文件的文件名称
        self.Version = __version__                       # 当前程序版本
        if clientcharset is None:                        # 客户端字符集
            self.Client_Charset = 'UTF-8'
        else:
            self.Client_Charset = clientcharset
        if resultcharset is None:
            self.Result_Charset = self.Client_Charset    # 结果输出字符集
        else:
            self.Result_Charset = resultcharset
        self.WorkerName = WorkerName                     # 当前进程名称. 如果有参数传递，以参数为准
        self.MultiProcessManager = None                  # 进程间共享消息管理器， 如果为子进程，该参数为空
        self.profile = []                                # 程序的初始化日志文件

        self.m_LastComment = None                        # 如果当前SQL之前的内容完全是注释，则注释带到这里

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
        if SharedProcessInfo is None:
            # 启动共享内存管理，注册服务
            # 注册后台共享进程管理
            self.MultiProcessManager = BaseManager()
            self.MultiProcessManager.register('SQLCliGlobalSharedMemory', callable=SQLCliGlobalSharedMemory)
            self.MultiProcessManager.start()
            obj = getattr(self.MultiProcessManager, 'SQLCliGlobalSharedMemory')
            self.SharedProcessInfo = obj()
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
        self.SQLExecuteHandler.sqlscript = sqlscript
        self.SQLExecuteHandler.SQLMappingHandler = self.SQLMappingHandler
        self.SQLExecuteHandler.SQLOptions = self.SQLOptions
        self.SQLExecuteHandler.logfile = self.logfile
        self.SQLExecuteHandler.Console = self.Console
        self.SQLExecuteHandler.SQLPerfFile = self.m_SQLPerf
        self.SQLExecuteHandler.logger = self.logger
        self.SQLMappingHandler.Console = self.Console
        self.SQLExecuteHandler.WorkerName = self.WorkerName

        # 默认的输出格式
        self.formatter = TabularOutputFormatter(format_name='ascii')
        self.formatter.sqlcli = self
        self.syntax_style = 'default'
        self.output_style = None

        # 加载一些特殊的命令
        self.register_special_commands()

        # 设置WHENEVER_SQLERROR
        if breakwitherror:
            self.SQLOptions.set("WHENEVER_SQLERROR", "EXIT")

    def __del__(self):
        # 如果打开了多进程管理，则关闭多进程管理
        if self.MultiProcessManager is not None:
            self.MultiProcessManager.shutdown()

    # 加载CLI的各种特殊命令集
    def register_special_commands(self):

        # 加载SQL映射文件
        register_special_command(
            self.load_driver,
            command="loaddriver",
            description="加载数据库驱动文件",
            hidden=False
        )

        # 加载SQL映射文件
        register_special_command(
            self.load_sqlmap,
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
            self.execute_from_file,
            command="start",
            description="执行指定的测试脚本",
            hidden=False
        )

        # sleep一段时间
        register_special_command(
            self.sleep,
            command="sleep",
            description="程序休眠(单位是秒)",
            hidden=False
        )

        # 设置各种参数选项
        register_special_command(
            self.set_options,
            command="set",
            description="设置运行时选项",
            hidden=False
        )

        # 将SQL信息Spool到指定的文件中
        register_special_command(
            self.spool,
            command="spool",
            description="将输出打印到指定文件",
            hidden=False
        )

        # ECHO 回显信息到指定的文件中
        register_special_command(
            self.echo_input,
            command="echo",
            description="回显输入到指定的文件",
            hidden=False
        )

        # HOST 执行主机的各种命令
        register_special_command(
            self.host,
            command="host",
            description="执行操作系统命令",
            hidden=False
        )

        # 执行特殊的命令
        register_special_command(
            self.execute_internal_command,
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
            self.exit,
            command="exit",
            description="正常退出当前应用程序",
            hidden=False
        )

    # 退出当前应用程序
    def exit(self, arg, **_):
        if arg:
            # 不处理任何exit的参数信息
            pass

        if self.sqlscript is not None:
            # 运行在脚本模式下，会一直等待所有子进程退出后，再退出本程序
            while True:
                if not self.JobHandler.isAllJobClosed():
                    # 如果还有没有退出的进程，则不会直接退出程序，会继续等待进程退出
                    time.sleep(3)
                else:
                    break
            # 等待那些已经Running的进程完成， 但是只有Submitted的不考虑在内
            self.JobHandler.waitjob("all")
            # 断开数据库连接
            if self.db_conn:
                self.db_conn.close()
            self.db_conn = None
            self.SQLExecuteHandler.conn = None
            # 取消进程共享服务的注册信息
            self.JobHandler.unregister()
            # 退出应用程序
            raise EOFError
        else:
            # 运行在控制台模式下
            if not self.JobHandler.isAllJobClosed():
                yield (
                    None,
                    None,
                    None,
                    None,
                    "Please wait all background process complete.")
            else:
                # 退出应用程序
                raise EOFError

    # 加载数据库驱动
    # 标准的默认驱动程序并不需要使用这个函数，这个函数是用来覆盖标准默认驱动程序的加载信息
    def load_driver(self, arg, **_):
        if arg == "":      # 显示当前的Driver配置
            m_Result = []
            for row in self.connection_configs:
                m_Result.append([row["Database"], row["ClassName"], row["FullName"],
                                 row["JDBCURL"], row["ODBCURL"], row["JDBCProp"]])
            yield (
                "Current Drivers: ",
                m_Result,
                ["Database", "ClassName", "FileName", "JDBCURL", "ODBCURL", "JDBCProp"],
                None,
                "Driver loaded."
            )
            return

        # 解析命令参数
        options_parameters = str(arg).split()

        # 只有一个参数，打印当前Database的Driver情况
        if len(options_parameters) == 1:
            m_DriverName = str(options_parameters[0])
            m_Result = []
            for row in self.connection_configs:
                if row["Database"] == m_DriverName:
                    m_Result.append([row["Database"], row["ClassName"], row["FullName"],
                                     row["JDBCURL"], row["ODBCURL"], row["JDBCProp"]])
                    break
            yield (
                "Current Drivers: ",
                m_Result,
                ["Database", "ClassName", "FileName", "JDBCURL", "ODBCURL", "JDBCProp"],
                None,
                "Driver loaded."
            )
            return

        # 两个参数，替换当前Database的Driver
        if len(options_parameters) == 2:
            m_DriverName = str(options_parameters[0])
            m_DriverFullName = str(options_parameters[1])
            if self.sqlscript is None:
                m_DriverFullName = os.path.join(sys.path[0], m_DriverFullName)
            else:
                m_DriverFullName = os.path.abspath(os.path.join(os.path.dirname(self.sqlscript), m_DriverFullName))
            if not os.path.isfile(m_DriverFullName):
                raise SQLCliException("Driver not loaded. file [" + m_DriverFullName + "] does not exist!")
            bFound = False
            for nPos in range(0, len(self.connection_configs)):
                if self.connection_configs[nPos]["Database"].upper() == m_DriverName.strip().upper():
                    m_Config = self.connection_configs[nPos]
                    m_Config["FullName"] = [m_DriverFullName, ]
                    bFound = True
                    self.connection_configs[nPos] = m_Config
            if not bFound:
                raise SQLCliException("Driver not loaded. Please config it in configfile first.")
            yield (
                None,
                None,
                None,
                None,
                "Driver [" + m_DriverName.strip() + "] loaded."
            )
            return

        raise SQLCliException("Bad command.  loaddriver [database] [new jar name]")

    # 加载数据库SQL映射
    def load_sqlmap(self, arg, **_):
        self.SQLOptions.set("SQLREWRITE", "ON")
        self.SQLMappingHandler.Load_SQL_Mappings(self.sqlscript, arg)
        self.sqlmap = arg
        yield (
            None,
            None,
            None,
            None,
            'Mapping file loaded.'
        )

    # 连接数据库
    def connect_db(self, arg, **_):
        # 一旦开始数据库连接，则当前连接会被置空，以保证连接错误的影响能够对后续的语句产生作用
        self.db_conn = None
        self.SQLExecuteHandler.conn = None

        if arg is None or len(str(arg)) == 0:
            raise SQLCliException(
                "Missing required argument\n." + "connect [user name]/[password]@" +
                "jdbc|odbc:[db type]:[driver type]://[host]:[port]/[service name]")
        elif self.connection_configs is None:
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
                self.db_username = m_userandpasslist[1]
                self.db_password = m_userandpasslist[3].replace("'", "").replace('"', "")
            else:
                raise SQLCliException("Missed user or password in connect command.")
        else:
            self.db_username = m_userandpasslist[0].strip()
            if len(m_userandpasslist) == 2:
                self.db_password = m_userandpasslist[1]
            else:
                self.db_password = ""

        # 判断连接字符串中是否含有服务器信息
        if len(m_connect_parameterlist) == 2:
            m_serverurl = m_connect_parameterlist[1]
        else:
            if "SQLCLI_CONNECTION_URL" in os.environ:
                m_serverurl = os.environ['SQLCLI_CONNECTION_URL']
            else:
                raise SQLCliException("Missed SQLCLI_CONNECTION_URL in env.")

        # 初始化连接参数
        self.db_service_name = ""
        self.db_host = ""
        self.db_port = ""
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
            self.db_service_name = m_serverurllist[2]
            m_ipandhost = m_serverurllist[1]
            m_serverparameter = m_ipandhost.split(':')
            if len(m_serverparameter) > 2:
                # IP:Port|IP:Port|IP:Port
                self.db_host = m_ipandhost
                self.db_port = ""
            elif len(m_serverparameter) == 2:
                # IP:Port
                self.db_host = m_serverparameter[0]
                self.db_port = m_serverparameter[1]
            elif len(m_serverparameter) == 1:
                # IP  sqlserver/teradata
                self.db_host = m_serverparameter[0]
                self.db_port = ""
        elif len(m_serverurllist) == 2:
            # //protocol/IP:Port:Service
            m_jdbcprop = m_serverurllist[0]
            m_serverparameter = m_serverurllist[1].split(':')
            if len(m_serverparameter) == 3:
                # IP:Port:Service, Old oracle version
                self.db_host = m_serverparameter[0]
                self.db_port = m_serverparameter[1]
                self.db_service_name = m_serverparameter[2]

        # 处理JDBC属性
        m_jdbcproplist = shlex.shlex(m_jdbcprop)
        m_jdbcproplist.whitespace = ':'
        m_jdbcproplist.quotes = '"'
        m_jdbcproplist.whitespace_split = True
        m_jdbcproplist = list(m_jdbcproplist)
        if len(m_jdbcproplist) < 2:
            raise SQLCliException("Missed jdbc prop in connect command.")
        self.db_conntype = m_jdbcproplist[0].upper()
        if self.db_conntype not in ['ODBC', 'JDBC']:
            raise SQLCliException("Unexpected connection type. Please use ODBC or JDBC.")
        self.db_type = m_jdbcproplist[1]
        if len(m_jdbcproplist) == 3:
            self.db_driver_type = m_jdbcproplist[2]
        else:
            self.db_driver_type = ""

        # 连接数据库
        try:
            if self.db_conntype == 'JDBC':   # JDBC 连接数据库
                # 加载所有的Jar包， 根据class的名字加载指定的文件
                m_JarList = []
                m_driverclass = ""
                m_JDBCURL = ""
                m_JDBCProp = ""
                for m_Jar_Config in self.connection_configs:
                    m_JarList.extend(m_Jar_Config["FullName"])
                for m_Jar_Config in self.connection_configs:
                    if m_Jar_Config["Database"].upper() == self.db_type.upper():
                        m_driverclass = m_Jar_Config["ClassName"]
                        m_JDBCURL = m_Jar_Config["JDBCURL"]
                        m_JDBCProp = m_Jar_Config["JDBCProp"]
                        break
                if "SQLCLI_DEBUG" in os.environ:
                    print("Driver Jar List: " + str(m_JarList))
                if m_JDBCURL is None:
                    raise SQLCliException("Unknown database [" + self.db_type.upper() + "]. Connect Failed. \n" +
                                          "Maybe you forgot download jlib files. ")
                m_JDBCURL = m_JDBCURL.replace("${host}", self.db_host)
                m_JDBCURL = m_JDBCURL.replace("${driver_type}", self.db_driver_type)
                if self.db_port == "":
                    m_JDBCURL = m_JDBCURL.replace(":${port}", "")
                else:
                    m_JDBCURL = m_JDBCURL.replace("${port}", self.db_port)
                m_JDBCURL = m_JDBCURL.replace("${service}", self.db_service_name)
                if m_driverclass is None:
                    raise SQLCliException("Missed driver [" + self.db_type.upper() + "] in config. "
                                                                                     "Database Connect Failed. ")
                m_jdbcconn_prop = {'user': self.db_username, 'password': self.db_password}
                if m_JDBCProp is not None:
                    for row in m_JDBCProp.strip().split(','):
                        props = row.split(':')
                        if len(props) == 2:
                            m_PropName = str(props[0]).strip()
                            m_PropValue = str(props[1]).strip()
                            m_jdbcconn_prop[m_PropName] = m_PropValue
                # 将当前DB的连接字符串备份到变量中
                self.SQLOptions.set("CONNURL", str(self.db_url))
                self.SQLOptions.set("CONNPROP", str(m_JDBCProp))
                # 数据库连接
                if self.db_conn is not None:
                    if self.SessionName is None:
                        # 如果之前数据库连接没有被保存，则强制断开连接
                        self.db_conn.close()
                        self.db_conn = None
                        self.SQLExecuteHandler.conn = None
                    else:
                        if self.db_saved_conn[self.SessionName][0] is None:
                            # 之前并没有保留数据库连接
                            self.db_conn.close()
                            self.db_conn = None
                            self.SQLExecuteHandler.conn = None
                if "SQLCLI_DEBUG" in os.environ:
                    print("Connect to [" + m_JDBCURL + "]...")
                self.db_conn = jdbcconnect(jclassname=m_driverclass,
                                           url=m_JDBCURL,
                                           driver_args=m_jdbcconn_prop,
                                           jars=m_JarList,
                                           sqloptions=self.SQLOptions)
                self.db_url = m_JDBCURL
                self.SQLExecuteHandler.conn = self.db_conn
            if self.db_conntype == 'ODBC':   # ODBC 连接数据库
                m_ODBCURL = ""
                for m_Connection_Config in self.connection_configs:
                    if m_Connection_Config["Database"].upper() == self.db_type.upper():
                        m_ODBCURL = m_Connection_Config["ODBCURL"]
                        break
                if m_ODBCURL is None:
                    raise SQLCliException("Unknown database [" + self.db_type.upper() + "]. Connect Failed. \n" +
                                          "Maybe you have a bad config file. ")
                m_ODBCURL = m_ODBCURL.replace("${host}", self.db_host)
                m_ODBCURL = m_ODBCURL.replace("${port}", self.db_port)
                m_ODBCURL = m_ODBCURL.replace("${service}", self.db_service_name)
                m_ODBCURL = m_ODBCURL.replace("${username}", self.db_username)
                m_ODBCURL = m_ODBCURL.replace("${password}", self.db_password)

                self.db_conn = odbcconnect(m_ODBCURL)
                self.db_url = m_ODBCURL
                self.SQLExecuteHandler.conn = self.db_conn
                # 将当前DB的连接字符串备份到变量中
                self.SQLOptions.set("CONNURL", str(self.db_url))
        except SQLCliException as se:  # Connecting to a database fail.
            raise se
        except SQLCliODBCException as soe:
            raise soe
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
                print("jar_file = [" + str(self.connection_configs) + "]")
            if str(e).find("SQLInvalidAuthorizationSpecException") != -1:
                raise SQLCliException(str(jpype.java.sql.SQLInvalidAuthorizationSpecException(e).getCause()))
            else:
                raise SQLCliException(repr(e))
        yield (
            None,
            None,
            None,
            None,
            'Database connected.'
        )

    # 断开数据库连接
    def disconnect_db(self, arg, **_):
        if arg:
            return [(None, None, None, None, "unnecessary parameter")]
        if self.db_conn:
            self.db_conn.close()
        self.db_conn = None
        self.SQLExecuteHandler.conn = None
        yield (
            None,
            None,
            None,
            None,
            'Database disconnected.'
        )

    # 执行主机的操作命令
    def host(self, arg, **_):
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
            yield (
                None,
                None,
                None,
                None,
                str(stdoutdata.decode(encoding=self.Result_Charset))
            )
            yield (
                None,
                None,
                None,
                None,
                str(stderrdata.decode(encoding=self.Result_Charset))
            )
        except UnicodeDecodeError:
            raise SQLCliException("The character set [" + self.Result_Charset + "]" +
                                  " does not match the terminal character set, " +
                                  "so the terminal information cannot be output correctly.")

    # 数据库会话管理
    def session_manage(self, arg, **_):
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
            for m_Session_Name, m_Connection in self.db_saved_conn.items():
                if m_Connection[0] is None:
                    m_Result.append(['None', str(m_Session_Name), str(m_Connection[1]), '******', str(m_Connection[3])])
                else:
                    m_Result.append(['Connection', str(m_Session_Name), str(m_Connection[1]),
                                     '******', str(m_Connection[3])])
            if self.db_conn is not None:
                m_Result.append(['Current', str(self.SessionName), str(self.db_username), '******', str(self.db_url)])
            if len(m_Result) == 0:
                yield (
                    None,
                    None,
                    None,
                    None,
                    "No saved sesssions."
                )
            else:
                yield (
                    "Saved Sessions:",
                    m_Result,
                    ["Session", "Sesssion Name", "User Name", "Password", "URL"],
                    None,
                    "Total " + str(len(m_Result)) + " saved sesssions."
                )
            return

        # 要求两个参数 save/restore [session_name]
        if len(m_Parameters) != 2:
            raise SQLCliException(
                "Wrong argument : " + "Session save/restore/release [session name]")

        if m_Parameters[0] == 'release':
            if self.db_conn is None:
                raise SQLCliException(
                    "You don't have a saved session.")
            m_Session_Name = m_Parameters[1]
            del self.db_saved_conn[m_Session_Name]
            self.SessionName = None
        elif m_Parameters[0] == 'save':
            if self.db_conn is None:
                raise SQLCliException(
                    "Please connect session first before save.")
            m_Session_Name = m_Parameters[1]
            self.db_saved_conn[m_Session_Name] = [self.db_conn, self.db_username, self.db_password, self.db_url]
            self.SessionName = m_Session_Name
        elif m_Parameters[0] == 'saveurl':
            if self.db_conn is None:
                raise SQLCliException(
                    "Please connect session first before save.")
            m_Session_Name = m_Parameters[1]
            self.db_saved_conn[m_Session_Name] = [None, self.db_username, self.db_password, self.db_url]
            self.SessionName = m_Session_Name
        elif m_Parameters[0] == 'restore':
            m_Session_Name = m_Parameters[1]
            if m_Session_Name in self.db_saved_conn:
                self.db_username = self.db_saved_conn[m_Session_Name][1]
                self.db_password = self.db_saved_conn[m_Session_Name][2]
                self.db_url = self.db_saved_conn[m_Session_Name][3]
                if self.db_saved_conn[m_Session_Name][0] is None:
                    result = self.connect_db(self.db_username + "/" + self.db_password + "@" + self.db_url)
                    for title, cur, headers, columntypes, status in result:
                        yield title, cur, headers, columntypes, status
                else:
                    self.db_conn = self.db_saved_conn[m_Session_Name][0]
                    self.SQLExecuteHandler.conn = self.db_conn
                    self.SessionName = m_Session_Name
            else:
                raise SQLCliException(
                    "Session [" + m_Session_Name + "] does not exist. Please save it first.")
        else:
            raise SQLCliException(
                "Wrong argument : " + "Session save/restore [session name]")
        if m_Parameters[0] == 'save':
            yield None, None, None, None, "Session saved Successful."
        if m_Parameters[0] == 'release':
            yield None, None, None, None, "Session release Successful."
        if m_Parameters[0] == 'restore':
            self.SQLOptions.set("CONNURL", self.db_url)
            yield None, None, None, None, "Session restored Successful."

    # 休息一段时间, 如果收到SHUTDOWN信号的时候，立刻终止SLEEP
    @staticmethod
    def sleep(arg, **_):
        if not arg:
            message = "Missing required argument, sleep [seconds]."
            return [(None, None, None, None, message)]
        try:
            m_Sleep_Time = int(arg)
            if m_Sleep_Time <= 0:
                message = "Parameter must be a valid number, sleep [seconds]."
                return [(None, None, None, None, message)]
            time.sleep(m_Sleep_Time)
        except ValueError:
            message = "Parameter must be a number, sleep [seconds]."
            return [(None, None, None, None, message)]
        return [(None, None, None, None, None)]

    # 从文件中执行SQL
    def execute_from_file(self, arg, **_):
        if not arg:
            message = "Missing required argument, filename1,filename2,filename,3 [loop <loop times>]."
            return [(None, None, None, None, message)]
        m_SQLFileList = str(arg).split()
        m_nLoop = 1
        if len(m_SQLFileList) >= 3 and \
                m_SQLFileList[len(m_SQLFileList) - 2].lower() == 'loop' and \
                m_SQLFileList[len(m_SQLFileList) - 1].isnumeric():
            m_nLoop = int(m_SQLFileList[len(m_SQLFileList) - 1])
            m_SQLFileList = m_SQLFileList[:-2]
        for m_curLoop in range(0, m_nLoop):
            for m_SQLFile in m_SQLFileList:
                try:
                    with open(os.path.expanduser(m_SQLFile), encoding=self.Client_Charset) as f:
                        query = f.read()

                    # 处理NLS文档头数据
                    if ord(query[0]) == 0xFEFF:
                        # 去掉SQL文件可能包含的UTF-BOM
                        query = query[1:]
                    if query[:3] == codecs.BOM_UTF8:
                        # 去掉SQL文件可能包含的UTF-BOM
                        query = query[3:]

                    # Scenario等Hint信息不会带入到下一个SQL文件中
                    self.SQLExecuteHandler.SQLScenario = ''
                    self.SQLExecuteHandler.SQLTransaction = ''

                    # 执行指定的SQL文件
                    for title, cur, headers, columntypes, status in \
                            self.SQLExecuteHandler.run(query, os.path.expanduser(m_SQLFile)):
                        yield title, cur, headers, columntypes, status

                except IOError as e:
                    yield None, None, None, None, str(e)

    # 将当前及随后的输出打印到指定的文件中
    def spool(self, arg, **_):
        if not arg:
            message = "Missing required argument, spool [filename]|spool off."
            return [(None, None, None, None, message)]
        parameters = str(arg).split()
        if parameters[0].strip().upper() == 'OFF':
            # close spool file
            if len(self.SpoolFileHandler) == 0:
                message = "not spooling currently"
                return [(None, None, None, None, message)]
            else:
                self.SpoolFileHandler[-1].close()
                self.SpoolFileHandler.pop()
                self.SQLExecuteHandler.spoolfile = None
                return [(None, None, None, None, None)]

        if self.logfilename is not None:
            # 如果当前主程序启用了日志，则spool日志的默认输出目录为logfile的目录
            m_FileName = os.path.join(os.path.dirname(self.logfilename), parameters[0].strip())
        else:
            # 如果主程序没有启用日志，则输出为当前目录
            m_FileName = parameters[0].strip()

        # 如果当前有打开的Spool文件，关闭它
        try:
            self.SpoolFileHandler.append(open(m_FileName, "w", encoding=self.Result_Charset))
        except IOError as e:
            raise SQLCliException("SQLCLI-00000: IO Exception " + repr(e))
        self.SQLExecuteHandler.spoolfile = self.SpoolFileHandler
        return [(None, None, None, None, None)]

    # 将当前及随后的屏幕输入存放到脚本文件中
    def echo_input(self, arg, **_):
        if not arg:
            message = "Missing required argument, echo [filename]|echo off."
            return [(None, None, None, None, message)]
        parameters = str(arg).split()
        if parameters[0].strip().upper() == 'OFF':
            # close echo file
            if self.EchoFileHandler is None:
                message = "not echo currently"
                return [(None, None, None, None, message)]
            else:
                self.EchoFileHandler.close()
                self.EchoFileHandler = None
                self.SQLExecuteHandler.echofile = None
                return [(None, None, None, None, None)]

        # ECHO的输出默认为程序的工作目录
        m_FileName = parameters[0].strip()

        # 如果当前有打开的Echo文件，关闭它
        if self.EchoFileHandler is not None:
            self.EchoFileHandler.close()
        self.EchoFileHandler = open(m_FileName, "w", encoding=self.Result_Charset)
        self.SQLExecuteHandler.echofile = self.EchoFileHandler
        return [(None, None, None, None, None)]

    # 设置一些选项
    def set_options(self, arg, **_):
        if arg is None:
            raise SQLCliException("Missing required argument. set parameter parameter_value.")
        elif arg == "":      # 显示所有的配置
            m_Result = []
            for row in self.SQLOptions.getOptionList():
                if not row["Hidden"]:
                    m_Result.append([row["Name"], row["Value"], row["Comments"]])
            yield (
                "Current Options: ",
                m_Result,
                ["Name", "Value", "Comments"],
                None,
                ""
            )
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
                    self.SQLOptions.set(options_parameters[0], option_value)
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
                    self.SQLOptions.set(options_parameters[0], option_value1, option_value2)
                else:
                    raise SQLCliException("SQLCLI-00000: "
                                          "Wrong set command. Please use [set @parameter_name parametervalue]")
                yield (
                    None,
                    None,
                    None,
                    None,
                    '')
            elif self.SQLOptions.get(options_parameters[0].upper()) is not None:
                self.SQLOptions.set(options_parameters[0].upper(), options_parameters[1])
                yield (
                    None,
                    None,
                    None,
                    None,
                    '')
            else:
                raise CommandNotFound

    # 执行特殊的命令
    def execute_internal_command(self, arg, **_):
        # 处理并发JOB
        matchObj = re.match(r"(\s+)?job(.*)$", arg, re.IGNORECASE | re.DOTALL)
        if matchObj:
            (title, result, headers, columntypes, status) = self.JobHandler.Process_Command(arg)
            yield title, result, headers, columntypes, status
            return

        # 处理Transaction
        matchObj = re.match(r"(\s+)?transaction(.*)$", arg, re.IGNORECASE | re.DOTALL)
        if matchObj:
            (title, result, headers, columntypes, status) = self.TransactionHandler.Process_Command(arg)
            yield title, result, headers, columntypes, status
            return

        # 处理kafka数据
        matchObj = re.match(r"(\s+)?kafka(.*)$", arg, re.IGNORECASE | re.DOTALL)
        if matchObj:
            (title, result, headers, columntypes, status) = self.KafkaHandler.Process_SQLCommand(arg)
            yield title, result, headers, columntypes, status
            return

        # 测试管理
        matchObj = re.match(r"(\s+)?test(.*)$", arg, re.IGNORECASE | re.DOTALL)
        if matchObj:
            (title, result, headers, columntypes, status) = self.TestHandler.Process_SQLCommand(arg)
            yield title, result, headers, columntypes, status
            return

        # 处理HDFS数据
        matchObj = re.match(r"(\s+)?hdfs(.*)$", arg, re.IGNORECASE | re.DOTALL)
        if matchObj:
            (title, result, headers, columntypes, status) = self.HdfsHandler.Process_SQLCommand(arg)
            yield title, result, headers, columntypes, status
            return

        # 处理随机数据文件
        matchObj = re.match(r"(\s+)?data(.*)$", arg, re.IGNORECASE | re.DOTALL)
        if matchObj:
            for (title, result, headers, columntypes, status) in \
                    self.DataHandler.Process_SQLCommand(arg, self.Result_Charset):
                yield title, result, headers, columntypes, status
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
            result = self.SQLExecuteHandler.run(text)

            # 输出显示结果
            self.formatter.query = text
            for title, cur, headers, columntypes, status in result:
                # 不控制每行的长度
                max_width = None

                # title 包含原有语句的SQL信息，如果ECHO打开的话
                # headers 包含原有语句的列名
                # cur 是语句的执行结果
                # output_format 输出格式
                #   ascii              默认，即表格格式(第三方工具实现，暂时保留以避免不兼容现象)
                #   vertical           分行显示，每行、每列都分行
                #   csv                csv格式显示
                #   tab                表格形式（用format_output_tab自己编写)
                formatted = self.format_output(
                    title, cur, headers, columntypes,
                    self.SQLOptions.get("OUTPUT_FORMAT").lower(),
                    max_width
                )

                # 输出显示信息
                try:
                    if self.SQLOptions.get("SILENT").upper() == 'OFF':
                        self.output(formatted, status)
                except KeyboardInterrupt:
                    # 显示过程中用户按下了CTRL+C
                    pass

            # 返回正确执行的消息
            return True
        except EOFError as e:
            # 当调用了exit或者quit的时候，会收到EOFError，这里直接抛出
            raise e
        except SQLCliException as e:
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
                            wget.download(m_driver_downloadurl, out=m_LocalJarFile)
                            print("")
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
        # 程序运行的结果
        m_runCli_Result = True

        # 加载程序的配置文件
        self.AppOptions = configparser.ConfigParser()
        m_conf_filename = os.path.join(os.path.dirname(__file__), "conf", "sqlcli.ini")
        if os.path.exists(m_conf_filename):
            self.AppOptions.read(m_conf_filename)
        else:
            self.echo("Can not open inifile for read [" + m_conf_filename + "]", err=True, fg="red")
            return False

        # 打开输出日志, 如果打开失败，就直接退出
        try:
            if self.logfilename is not None:
                self.logfile = open(self.logfilename, mode="w", encoding=self.Result_Charset)
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
        if self.sqlmap is not None:   # 如果传递的参数，有Mapping，以参数为准，先加载参数中的Mapping文件
            self.SQLMappingHandler.Load_SQL_Mappings(self.sqlscript, self.sqlmap)
            self.SQLOptions.set("SQLREWRITE", "ON")
        if "SQLCLI_SQLMAPPING" in os.environ:     # 如果没有参数，则以环境变量中的信息为准
            if len(os.environ["SQLCLI_SQLMAPPING"].strip()) > 0:
                self.SQLMappingHandler.Load_SQL_Mappings(self.sqlscript, os.environ["SQLCLI_SQLMAPPING"])
                self.SQLOptions.set("SQLREWRITE", "ON")

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

        # 如果多进程管理器工作中，退出多进程管理器
        if self.MultiProcessManager is not None:
            self.MultiProcessManager.shutdown()

        # 还原进程标题
        setproctitle.setproctitle(m_Cli_ProcessTitleBak)

        # 返回运行结果, True 运行成功。 False运行失败
        return m_runCli_Result

    def log_output(self, output):
        if self.logfile:
            click.echo(output, file=self.logfile)

    def echo(self, s, **kwargs):
        # 输出目的地
        # 1：  程序日志文件 logfile
        # 2：  程序的logger，用于在第三方调用时候的Console显示
        # 3：  当前屏幕控制台
        # 4：  程序的Spool文件
        if self.logfile:
            click.echo(s, file=self.logfile)
        if self.logger is not None:
            self.logger.info(s)
        try:
            click.secho(s, **kwargs, file=self.Console)
        except UnicodeEncodeError as ue:
            # Unicode Error, This is console issue, Skip
            if "SQLCLI_DEBUG" in os.environ:
                print("Console output error:: " + repr(ue))
        if len(self.SpoolFileHandler) != 0:
            for m_SpoolFileHandler in self.SpoolFileHandler:
                click.echo(s, file=m_SpoolFileHandler)

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
                        if columntypes[m_nPos] == str:
                            m_row = m_row + m_csv_quotechar + m_csv_quotechar
                else:
                    if columntypes is None:
                        m_row = m_row + str(row[m_nPos])
                    else:
                        if columntypes[m_nPos] == str:
                            m_row = m_row + m_csv_quotechar + str(row[m_nPos]) + m_csv_quotechar
                        else:
                            m_row = m_row + str(row[m_nPos])
                if m_nPos != len(row) - 1:
                    m_row = m_row + m_csv_delimiter
            yield str(m_row)

    def format_output_tab(self, headers, columntypes, cur):
        def wide_chars(s):
            # 判断字符串中包含的中文字符数量
            if isinstance(s, str):
                return sum(unicodedata.east_asian_width(x) == 'W' for x in s)
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
                if isinstance(m_Row[m_nPos], str):
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
            m_TableContentLine = m_TableContentLine + ' ' + \
                                 str(headers[m_nPos]).center(m_ColumnLength[m_nPos]) + ' |'
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
                    if columntypes[m_nPos] == str:
                        # 字符串左对齐
                        m_TableContentLine = \
                            m_TableContentLine + ' ' + \
                            str(m_iter[m_nPos]).ljust(m_ColumnLength[m_nPos] - wide_chars(m_iter[m_nPos])) + ' |'
                    else:
                        # 数值类型右对齐
                        m_TableContentLine = m_TableContentLine + ' ' + \
                                             str(m_iter[m_nPos]).rjust(m_ColumnLength[m_nPos]) + ' |'
                yield m_TableContentLine
        # 打印表格下边框
        yield m_TableBoxLine

    def format_output(self, title, cur, headers, columntypes, p_format_name, max_width=None):
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

            if p_format_name.upper() == 'CSV':
                # 按照CSV格式输出查询结果
                formatted = self.format_output_csv(headers, columntypes, cur)
            elif p_format_name.upper() == 'TAB':
                # 按照TAB格式输出查询结果
                formatted = self.format_output_tab(headers, columntypes, cur)
            else:
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
@click.option("--nologo", is_flag=True, help="Execute with nologo mode.")
@click.option("--sqlperf", type=str, help="SQL performance Log.")
@click.option("--syncdriver", is_flag=True, help="Download jdbc jar from file server.")
@click.option("--clientcharset", type=str, help="Set client charset. Default is UTF-8.")
@click.option("--resultcharset", type=str, help="Set result charset. Default is same to clientcharset.")
@click.option("--profile", type=str, help="Init profile.")
def cli(
        version,
        logon,
        logfile,
        execute,
        sqlmap,
        nologo,
        sqlperf,
        syncdriver,
        clientcharset,
        resultcharset,
        profile,
):
    if version:
        print("Version:", __version__)
        sys.exit(0)

    # 从服务器下下载程序需要的各种jar包
    if syncdriver:
        sqlcli = SQLCli(
            logfilename=logfile,
            logon=logon,
            sqlscript=execute,
            sqlmap=sqlmap,
            nologo=nologo,
            sqlperf=sqlperf
        )
        sqlcli.syncdriver()
        sys.exit(0)

    sqlcli = SQLCli(
        logfilename=logfile,
        logon=logon,
        sqlscript=execute,
        sqlmap=sqlmap,
        nologo=nologo,
        sqlperf=sqlperf,
        clientcharset=clientcharset,
        resultcharset=resultcharset,
        profile=profile
    )

    # 运行主程序
    sqlcli.run_cli()


if __name__ == "__main__":
    try:
        cli()
    except Exception as ge:
        if "SQLCLI_DEBUG" in os.environ:
            print('traceback.print_exc():\n%s' % traceback.print_exc())
            print('traceback.format_exc():\n%s' % traceback.format_exc())
        print("Fatal Exception: " + repr(ge))
