# -*- coding: utf-8 -*-
import os
import configparser
import traceback
from .sqlclijdbcapi import connect as jdbcconnect
from .sqloption import SQLOptions


# SQLCli的Meta管理，使用H2数据库作为管理方式
class SQLCliMeta(object):
    def __init__(self):
        self.JobManagerEnabled = False
        self.db_conn = None

    def Connect(self):
        # 检查SQLCli_HOME是否存在
        if "SQLCLI_HOME" in os.environ:
            try:
                # 读取配置文件，并连接数据库
                m_AppOptions = configparser.ConfigParser()
                m_conf_filename = os.path.join(os.path.dirname(__file__), "conf", "sqlcli.ini")
                if os.path.exists(m_conf_filename):
                    m_AppOptions.read(m_conf_filename)
                else:
                    if "SQLCLI_DEBUG" in os.environ:
                        print("DEBUG:: SQLCliMeta:: sqlcli.ini does not exist! JobManager Aborted!")
                        return
                m_MetaClass = m_AppOptions.get("meta_driver", "driver")
                m_MetaDriverFile = os.path.join(os.path.dirname(__file__), "jlib",
                                                 m_AppOptions.get("meta_driver", "filename"))
                if not os.path.exists(m_MetaDriverFile):
                    if "SQLCLI_DEBUG" in os.environ:
                        print("DEBUG:: SQLCliMeta:: Driver file does not exist! JobManager Aborted!")
                        return
                m_MetaDriverURL = m_AppOptions.get("meta_driver", "jdbcurl")
                self.db_conn = jdbcconnect(jclassname=m_MetaClass, url=m_MetaDriverURL,
                                           driver_args= {'user': 'sa', 'password': 'sa'},
                                           jars=[m_MetaDriverFile], sqloptions=SQLOptions())
                if self.db_conn is None:
                    if "SQLCLI_DEBUG" in os.environ:
                        print("DEBUG:: SQLCliMeta:: Connect to meta failed! JobManager Aborted!")
                        return

                # 初始化Meta数据库表
                m_SQL = "Create Table SQLCLI_ServerInfo" \
                        "(" \
                        "   ProcessID  Integer, " \
                        "   ProcessPath  VARCHAR(500), )"

                self.JobManagerEnabled = False
            except Exception as ce:
                if "SQLCLI_DEBUG" in os.environ:
                    print('traceback.print_exc():\n%s' % traceback.print_exc())
                    print('traceback.format_exc():\n%s' % traceback.format_exc())
        else:
            if "SQLCLI_DEBUG" in os.environ:
                print("DEBUG:: SQLCliMeta:: Env(SQLCLI_HOME) does not exist! JobManager Aborted!")
                return

    def Update(self):
        pass
