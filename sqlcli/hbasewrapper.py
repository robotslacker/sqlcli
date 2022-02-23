# -*- coding: UTF-8 -*-
import re

from thrift.transport import TSocket, TTransport
from thrift.protocol import TCompactProtocol
from hbase import Hbase
from hbase.ttypes import ColumnDescriptor

from .sqlcliexception import SQLCliException


class HBaseWrapperException(Exception):
    def __init__(self, message):
        Exception.__init__(self)
        self.message = message


class HBaseWrapper(object):

    def __init__(self):
        self.__hbase_server_ip = None
        self.__hbase_server_port = 9090
        self.__socket = None
        self.__transport = None
        self.__client = None

    def __del__(self):
        if self.__hbase_server_ip is not None:
            self.__transport.close()

    def hbase_connect(self):
        """
        建立与thrift server端的连接
        """
        if self.__hbase_server_ip is None:
            raise SQLCliException("Missed hbase server information. Please use as \"hbase connect server 127.0.0.1\" ")
        # server端地址和端口设定
        self.__socket = TSocket.TSocket(self.__hbase_server_ip, self.__hbase_server_port)
        self.__transport = TTransport.TBufferedTransport(self.__socket)
        # 设置传输协议
        protocol = TCompactProtocol.TCompactProtocol(self.__transport)
        # 客户端
        self.__client = Hbase.Client(protocol)
        # 打开连接
        self.__socket.open()

    def hbase_get_tables(self):
        """
        获得所有表
        :return:表名列表
        """
        return self.__client.getTableNames()

    def hbase_create_table(self, table, *columns):
        """
        创建表格
        :param table:表名
        :param columns:列族名
        """
        columns = columns[0]
        column_families = map(lambda col: ColumnDescriptor(col), columns)
        self.__client.createTable(table, list(column_families))
        return "table {} created.".format(table)

    def hbase_delete_table(self, table):
        """
        删除表格
        :param table:表名
        """
        table_list = self.hbase_get_tables()
        if table not in table_list:
            return "table {} not exist.".format(table)
        elif self.__client.isTableEnabled(table):
            self.__client.disableTable(table)
        self.__client.deleteTable(table)
        return "table {} deleted.".format(table)

    def Process_SQLCommand(self, p_szSQL):
        m_szSQL = p_szSQL.strip()
        match_obj = re.match(r"hbase\s+connect\s+server\s+(.*)$", m_szSQL, re.IGNORECASE | re.DOTALL)
        if match_obj:
            m_HbaseServer_list = str(match_obj.group(1)).strip().split(':')
            self.__hbase_server_ip = m_HbaseServer_list[0]
            if len(m_HbaseServer_list) == 2:
                self.__hbase_server_port = int(m_HbaseServer_list[1])
            else:
                self.__hbase_server_port = 9090
            self.hbase_connect()
            return None, None, None, None, "Hbase Server set successful."

        match_obj = re.match(r"hbase\s+create\s+table\s+(.*?)\s+(.*)$", m_szSQL, re.IGNORECASE | re.DOTALL)
        if match_obj:
            m_TableName = str(match_obj.group(1)).strip()
            m_columnFamilies = str(match_obj.group(2)).split()
            m_ReturnMessage = self.hbase_create_table(m_TableName, m_columnFamilies)
            return None, None, None, None, m_ReturnMessage

        match_obj = re.match(r"hbase\s+delete\s+table\s+(.*)$", m_szSQL, re.IGNORECASE | re.DOTALL)
        if match_obj:
            m_TableName = str(match_obj.group(1)).strip()
            m_ReturnMessage = self.hbase_delete_table(m_TableName)
            return None, None, None, None, m_ReturnMessage

        match_obj = re.search(r"hbase\s+get\s+tables", m_szSQL, re.IGNORECASE | re.DOTALL)
        if match_obj:
            m_ReturnMessage = str(self.hbase_get_tables())
            return None, None, None, None, m_ReturnMessage

        return None, None, None, None, "Unknown hbase Command."
