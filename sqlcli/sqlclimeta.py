# -*- coding: utf-8 -*-
import os
import configparser
import traceback
import jpype
import socket
from multiprocessing import Lock
from .sqlclijdbcapi import connect as jdbcconnect
from .sqlcliexception import SQLCliException


# SQLCli的Meta管理，使用H2数据库作为管理方式
class SQLCliMeta(object):
    def __init__(self):
        self.JobManagerEnabled = False
        self.db_conn = None
        self.MetaServer = None
        self.MetaURL = None
        self.MetaPort = 0
        self.JarList = None

    def setJVMJarList(self, p_JarList):
        self.JarList = p_JarList

    def DisConnect(self):
        if self.db_conn is not None:
            self.db_conn.close()

    def ShutdownServer(self):
        if self.MetaServer is not None:
            # 先停止数据库连接
            if self.db_conn is not None:
                self.db_conn.close()
            # 再停止数据库服务
            try:
                self.MetaServer.stop()
                self.MetaServer.shutdown()
            except Exception:
                if "SQLCLI_DEBUG" in os.environ:
                    print('traceback.print_exc():\n%s' % traceback.print_exc())
                    print('traceback.format_exc():\n%s' % traceback.format_exc())
            self.MetaServer = None

    def StartAsServer(self, p_ServerParameter):
        # 检查SQLCli_HOME是否存在
        try:
            # 读取配置文件，并连接数据库
            m_AppOptions = configparser.ConfigParser()
            m_conf_filename = os.path.join(os.path.dirname(__file__), "conf", "sqlcli.ini")
            m_AppOptions.read(m_conf_filename)
            m_MetaClass = m_AppOptions.get("meta_driver", "driver")
            m_MetaDriverFile = os.path.join(os.path.dirname(__file__),
                                            "jlib", m_AppOptions.get("meta_driver", "filename"))
            if not os.path.exists(m_MetaDriverFile):
                raise SQLCliException("SQLCLI-00000: "
                                      "SQLCliMeta:: Driver file [" + m_MetaDriverFile +
                                      "] does not exist! JobManager Aborted!")
            m_MetaDriverURL = m_AppOptions.get("meta_driver", "jdbcurl")
            self.db_conn = jdbcconnect(jclassname=m_MetaClass, url=m_MetaDriverURL,
                                       driver_args={'user': 'sa', 'password': 'sa'},
                                       jars=self.JarList)
            if self.db_conn is None:
                raise SQLCliException("SQLCLI-00000: "
                                      "SQLCliMeta:: Connect to meta failed! JobManager Aborted!")
            # 设置AutoCommit为False
            self.db_conn.setAutoCommit(False)

            # 获得一个可用的端口
            if "SQLCLI_METAPORT" in os.environ:
                self.MetaPort = int(os.environ["SQLCLI_METAPORT"])
            else:
                m_PortFileLocker = Lock()
                m_PortFileLocker.acquire()
                self.MetaPort = 0
                try:
                    sock = socket.socket()
                    sock.bind(('', 0))
                    self.MetaPort = sock.getsockname()[1]
                    sock.close()
                except Exception:
                    if "SQLCLI_DEBUG" in os.environ:
                        print('traceback.print_exc():\n%s' % traceback.print_exc())
                        print('traceback.format_exc():\n%s' % traceback.format_exc())
                    self.MetaPort = 0
                m_PortFileLocker.release()
                if self.MetaPort == 0:
                    raise SQLCliException("SQLCLI-00000: "
                                          "SQLCliMeta:: Can't get avalable port! JobManager Aborted!")

            # 启动一个TCP Server，用来给其他后续的H2连接（主要是子进程）
            # 端口号为随机获得，或者利用固定的参数
            self.MetaServer = jpype.JClass("org.h2.tools.Server").\
                createTcpServer("-tcp", "-tcpAllowOthers", "-tcpPort", str(self.MetaPort))
            self.MetaServer.start()
            self.MetaURL = self.MetaServer.getURL()

            # 初始化Meta数据库表
            m_db_cursor = self.db_conn.cursor()

            m_SQL = "Create Table IF Not Exists SQLCLI_ServerInfo" \
                    "(" \
                    "ProcessID       Integer," \
                    "ParentProcessID Integer," \
                    "ProcessPath     VARCHAR(500)," \
                    "Parameter       VARCHAR(500)," \
                    "StartTime       TimeStamp," \
                    "EndTime         TimeStamp" \
                    ")"
            m_db_cursor.execute(m_SQL)
            m_SQL = "CREATE TABLE IF Not Exists SQLCLI_JOBS" \
                    "(" \
                    "JOB_ID                     Integer," \
                    "JOB_Name                   VARCHAR(500)," \
                    "JOB_TAG                    VARCHAR(500)," \
                    "Starter_Interval           Integer," \
                    "Starter_Last_Active_Time   TimeStamp," \
                    "Parallel                   Integer," \
                    "Loop                       Integer," \
                    "Started_JOBS               Integer," \
                    "Failed_JOBS                Integer," \
                    "Finished_JOBS              Integer," \
                    "Active_JOBS                Integer," \
                    "Error_Message              VARCHAR(500)," \
                    "Script                     VARCHAR(500)," \
                    "Script_FullName            VARCHAR(500)," \
                    "Think_Time                 Integer," \
                    "Timeout                    Integer," \
                    "Submit_Time                TimeStamp," \
                    "Start_Time                 TimeStamp," \
                    "End_Time                   TimeStamp," \
                    "Blowout_Threshold_Count    Integer," \
                    "Status                     VARCHAR(500)" \
                    ")"
            m_db_cursor.execute(m_SQL)
            m_SQL = "CREATE TABLE IF Not Exists SQLCLI_WORKERS" \
                    "(" \
                    "JOB_ID                     Integer," \
                    "JOB_Name                   VARCHAR(500)," \
                    "JOB_TAG                    VARCHAR(500)," \
                    "WorkerHandler_ID           Integer," \
                    "ProcessID                  Integer," \
                    "start_time                 BIGINT," \
                    "end_time                   BIGINT," \
                    "exit_code                  Integer," \
                    "Finished_Status            VARCHAR(500)," \
                    "Timer_Point                VARCHAR(500)" \
                    ")"
            m_db_cursor.execute(m_SQL)
            m_SQL = "CREATE TABLE IF Not Exists SQLCLI_WORKERS_HISTORY" \
                    "(" \
                    "JOB_ID                     Integer," \
                    "JOB_Name                   VARCHAR(500)," \
                    "JOB_TAG                    VARCHAR(500)," \
                    "WorkerHandler_ID           Integer," \
                    "ProcessID                  Integer," \
                    "start_time                 BIGINT," \
                    "end_time                   BIGINT," \
                    "exit_code                  Integer," \
                    "Finished_Status            VARCHAR(500)," \
                    "Timer_Point                VARCHAR(500)" \
                    ")"
            m_db_cursor.execute(m_SQL)
            m_SQL = "CREATE TABLE IF Not Exists SQLCLI_TRANSACTIONS" \
                    "(" \
                    "Transaction_Name           VARCHAR(500)," \
                    "Transaction_StartTime      Integer," \
                    "Transaction_EndTime        Integer,"\
                    "Transaction_Status         Integer" \
                    ")"
            m_db_cursor.execute(m_SQL)
            m_SQL = "CREATE TABLE IF Not Exists SQLCLI_TRANSACTIONS_STATISTICS" \
                    "(" \
                    "Transaction_Name           VARCHAR(500)," \
                    "Max_Transaction_Time       Integer," \
                    "Min_Transaction_Time       Integer," \
                    "Sum_Transaction_Time       Integer," \
                    "Transaction_Count          Integer," \
                    "Transaction_Failed_Count   Integer" \
                    ")"
            m_db_cursor.execute(m_SQL)
            m_db_cursor.close()

            m_ProcessID = os.getpid()
            m_ParentProcessID = os.getppid()
            m_ProcessPath = os.path.dirname(__file__)
            m_SQL = "Insert Into SQLCLI_ServerInfo(ProcessID, ParentProcessID, ProcessPath, Parameter, StartTime)" \
                    "Values(" + str(m_ProcessID) + "," + str(m_ParentProcessID) + \
                    ",'" + str(m_ProcessPath) + "','" + str(p_ServerParameter) + "', CURRENT_TIMESTAMP())"
            m_db_cursor = self.db_conn.cursor()
            m_db_cursor.execute(m_SQL)
            m_db_cursor.close()
            self.db_conn.commit()

            # 任务调度管理只有在Meta能够成功连接的情况下才可以使用
            self.JobManagerEnabled = False
        except SQLCliException as se:
            raise se
        except Exception as e:
            if "SQLCLI_DEBUG" in os.environ:
                print('traceback.print_exc():\n%s' % traceback.print_exc())
                print('traceback.format_exc():\n%s' % traceback.format_exc())
            raise SQLCliException("SQLCLI-00000: "
                                  "SQLCliMeta:: " + str(e) + " JobManager Aborted!")

    def ConnectServer(self, p_MetaServerURL):
        if p_MetaServerURL is None:
            # 如果没有Meta的连接信息，直接退出
            return

        try:
            # 读取配置文件，并连接数据库
            m_AppOptions = configparser.ConfigParser()
            m_conf_filename = os.path.join(os.path.dirname(__file__), "conf", "sqlcli.ini")
            if os.path.exists(m_conf_filename):
                m_AppOptions.read(m_conf_filename)
            else:
                if "SQLCLI_DEBUG" in os.environ:
                    print("DEBUG:: SQLCliMeta:: sqlcli.ini does not exist! JobManager Connect Failed!")
                    return
            m_MetaClass = m_AppOptions.get("meta_driver", "driver")
            m_MetaDriverFile = os.path.join(os.path.dirname(__file__),
                                            "jlib", m_AppOptions.get("meta_driver", "filename"))
            if not os.path.exists(m_MetaDriverFile):
                if "SQLCLI_DEBUG" in os.environ:
                    print("DEBUG:: SQLCliMeta:: Driver file does not exist! JobManager Connect Failed!")
                    return
            m_MetaDriverURL = m_AppOptions.get("meta_driver", "jdbcurl")
            m_MetaDriverURL = m_MetaDriverURL.replace("mem", p_MetaServerURL + "/mem")
            self.db_conn = jdbcconnect(jclassname=m_MetaClass, url=m_MetaDriverURL,
                                       driver_args={'user': 'sa', 'password': 'sa'},
                                       jars=self.JarList)
            if self.db_conn is None:
                if "SQLCLI_DEBUG" in os.environ:
                    print("DEBUG:: SQLCliMeta:: Connect to meta failed! JobManager Connect Failed!")
                    return

            # 设置AutoCommit为False
            self.db_conn.setAutoCommit(False)
        except Exception:
            if "SQLCLI_DEBUG" in os.environ:
                print('traceback.print_exc():\n%s' % traceback.print_exc())
                print('traceback.format_exc():\n%s' % traceback.format_exc())

    def DisConnectServer(self):
        if self.db_conn:
            self.db_conn.close()
