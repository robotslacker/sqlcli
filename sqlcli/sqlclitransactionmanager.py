# -*- coding: utf-8 -*-
import re
import time
import datetime
from .sqlcliexception import SQLCliException


class Transaction(object):
    def __init__(self):
        self.Transaction_Name = ""              # 事务名称
        self.Transaction_StartTime = 0          # 事务开始时间
        self.Transaction_EndTime = 0            # 事务结束时间
        self.Transaction_Status = 0             # 事务完成状态


class TransactionStatistics(object):
    def __init__(self):
        self.Transaction_Name = ""              # 事务名称
        self.Max_Transaction_Time = 0           # 事务最长完成时间
        self.Min_Transaction_Time = 0           # 事务最短完成时间
        self.Sum_Transaction_Time = 0           # 事务累计完成时间
        self.Transaction_Count = 0              # 事务累计完成次数
        self.Transaction_Failed_Count = 0       # 事务累计失败次数


# Transaction事务管理
class TransactionManager(object):
    def __init__(self):
        # SQL执行句柄
        self.SQLExecuteHandler = None

        # 记录Meta的数据库连接信息
        self.MetaConn = None

    # 设置Meta的连接信息
    def setMetaConn(self, p_conn):
        self.MetaConn = p_conn

    def getTransactionStatisticsByName(self, p_szTransaction_Name: str):
        # 返回指定的Transaction统计信息，如果找不到，返回None
        m_SQL = "SELECT Max_Transaction_Time,Min_Transaction_Time,Sum_Transaction_Time," \
                "       Transaction_Count,Transaction_Failed_Count " \
                "FROM   SQLCLI_TRANSACTIONS_STATISTICS " \
                "WHERE  Transaction_Name = '" + p_szTransaction_Name + "'"
        m_db_cursor = self.MetaConn.cursor()
        m_db_cursor.execute(m_SQL)
        m_rs = m_db_cursor.fetchone()
        if m_rs is None:
            m_db_cursor.close()
            return None
        else:
            m_TransactionStatistics = TransactionStatistics()
            m_TransactionStatistics.Transaction_Name = p_szTransaction_Name
            m_TransactionStatistics.Max_Transaction_Time = m_rs[0]
            m_TransactionStatistics.Min_Transaction_Time = m_rs[1]
            m_TransactionStatistics.Sum_Transaction_Time = m_rs[2]
            m_TransactionStatistics.Transaction_Count = m_rs[3]
            m_TransactionStatistics.Transaction_Failed_Count = m_rs[4]
            m_db_cursor.close()
            return m_TransactionStatistics

    def getAllTransactionStatistics(self):
        # 返回全部的Transaction统计信息，如果找不到，返回None
        m_SQL = "SELECT Transaction_Name " \
                "FROM   SQLCLI_TRANSACTIONS_STATISTICS " \
                "ORDER  BY 1"
        m_db_cursor = self.MetaConn.cursor()
        m_db_cursor.execute(m_SQL)
        m_rs = m_db_cursor.fetchone()
        if m_rs is None:
            m_db_cursor.close()
            return None
        else:
            m_TransactionStatisticsList = []
            for m_row in m_rs:
                m_TransactionStatistics = self.getTransactionStatisticsByName(m_row[0])
                m_TransactionStatisticsList.append(m_TransactionStatistics)
            m_db_cursor.close()
            return m_TransactionStatisticsList

    def getTransactionByName(self, p_szTransaction_Name: str):
        # 返回指定的Transaction信息，如果找不到，返回None
        m_SQL = "SELECT Transaction_StartTime,Transaction_EndTime,Transaction_Status " \
                "FROM   SQLCLI_TRANSACTIONS " \
                "WHERE  Transaction_Name = '" + p_szTransaction_Name + "' "
        m_db_cursor = self.MetaConn.cursor()
        m_db_cursor.execute(m_SQL)
        m_rs = m_db_cursor.fetchone()
        if m_rs is None:
            m_db_cursor.close()
            return None
        else:
            m_Transaction = Transaction()
            m_Transaction.Transaction_Name = p_szTransaction_Name
            m_Transaction.Transaction_StartTime = m_rs[0]
            m_Transaction.Transaction_EndTime = m_rs[1]
            m_Transaction.Transaction_Status = m_rs[2]
            m_db_cursor.close()
            return m_Transaction

    def getAllTransactions(self):
        # 返回指定的Transaction信息，如果找不到，返回None
        m_SQL = "SELECT Transaction_Name " \
                "FROM   SQLCLI_TRANSACTIONS "
        m_db_cursor = self.MetaConn.cursor()
        m_db_cursor.execute(m_SQL)
        m_rs = m_db_cursor.fetchone()
        if m_rs is None:
            m_db_cursor.close()
            return []
        else:
            m_TransactionList = []
            for m_row in m_rs:
                m_Transaction = self.getTransactionByName(m_row[0])
                m_TransactionList.append(m_Transaction)
            m_db_cursor.close()
            return m_TransactionList

    def StatisticsTransaction(self, p_objTransaction: Transaction):
        m_TransactionStatistics = self.getTransactionStatisticsByName(p_objTransaction.Transaction_Name)
        if m_TransactionStatistics is None:
            # 之前没有任何统计信息
            m_SQL = "INSERT INTO SQLCLI_TRANSACTIONS_STATISTICS(" \
                    "Transaction_Name, Max_Transaction_Time,Min_Transaction_Time,Sum_Transaction_Time," \
                    "Transaction_Count,Transaction_Failed_Count) VALUES(?,?,?,?,?,?) "
            m_Data = [
                p_objTransaction.Transaction_Name,
                p_objTransaction.Transaction_EndTime - p_objTransaction.Transaction_StartTime,
                p_objTransaction.Transaction_EndTime - p_objTransaction.Transaction_StartTime,
                p_objTransaction.Transaction_EndTime - p_objTransaction.Transaction_StartTime,
                1,
                0 if p_objTransaction.Transaction_Status == 0 else 1
            ]
            m_db_cursor = self.MetaConn.cursor()
            m_db_cursor.execute(m_SQL, parameters=m_Data)
            self.MetaConn.commit()
        else:
            m_TransactionStatistics.Transaction_Count = m_TransactionStatistics.Transaction_Count + 1
            if p_objTransaction.Transaction_Status != 0:
                m_TransactionStatistics.Transaction_Failed_Count = m_TransactionStatistics.Transaction_Failed_Count + 1
            m_TrsansactionElapsedTime = p_objTransaction.Transaction_EndTime - p_objTransaction.Transaction_StartTime
            m_TransactionStatistics.Sum_Transaction_Time = \
                m_TransactionStatistics.Sum_Transaction_Time + m_TrsansactionElapsedTime
            if m_TrsansactionElapsedTime > m_TransactionStatistics.Max_Transaction_Time:
                m_TransactionStatistics.Max_Transaction_Time = m_TrsansactionElapsedTime
            if m_TrsansactionElapsedTime < m_TransactionStatistics.Min_Transaction_Time:
                m_TransactionStatistics.Min_Transaction_Time = m_TrsansactionElapsedTime
            m_SQL = "UPDATE SQLCLI_TRANSACTIONS_STATISTICS " \
                    "SET    Max_Transaction_Time = ?, " \
                    "       Min_Transaction_Time = ?, " \
                    "       Sum_Transaction_Time = ?, " \
                    "       Transaction_Count = ?, " \
                    "       Transaction_Failed_Count = ? " \
                    "WHERE  Transaction_Name = '" + p_objTransaction.Transaction_Name + "'"
            m_Data = [
                p_objTransaction.Transaction_Name,
                m_TransactionStatistics.Max_Transaction_Time,
                m_TransactionStatistics.Min_Transaction_Time,
                m_TransactionStatistics.Sum_Transaction_Time,
                m_TransactionStatistics.Transaction_Count,
                m_TransactionStatistics.Transaction_Failed_Count
            ]
            m_db_cursor = self.MetaConn.cursor()
            m_db_cursor.execute(m_SQL, parameters=m_Data)
            self.MetaConn.commit()

    def SaveTransaction(self, p_objTransaction: Transaction):
        m_SQL = "SELECT COUNT(1) FROM SQLCLI_TRANSACTIONS " \
                "WHERE  Transaction_Name = '" + str(p_objTransaction.Transaction_Name) + "'"
        m_db_cursor = self.MetaConn.cursor()
        m_db_cursor.execute(m_SQL)
        m_rs = m_db_cursor.fetchone()
        if m_rs[0] == 0:
            m_SQL = "Insert Into SQLCLI_TRANSACTIONS(" \
                    "Transaction_Name,Transaction_StartTime,Transaction_EndTime,Transaction_Status) " \
                    "VALUES (?,?,?,?,?)"
            m_Data = [
                p_objTransaction.Transaction_Name,
                p_objTransaction.Transaction_StartTime,
                p_objTransaction.Transaction_EndTime,
                p_objTransaction.Transaction_Status
            ]
            m_db_cursor.execute(m_SQL, parameters=m_Data)
        else:
            m_SQL = "Update SQLCLI_TRANSACTIONS " \
                    "SET    Transaction_StartTime = ?, " \
                    "       Transaction_EndTime = ?, " \
                    "       Transaction_Status = ? " \
                    "WHERE  Transaction_Name = '" + p_objTransaction.Transaction_Name + "'"

            m_Data = [
                p_objTransaction.Transaction_StartTime,
            ]
            m_db_cursor.execute(m_SQL, parameters=m_Data)
        m_db_cursor.close()
        self.MetaConn.commit()

    def TransactionBegin(self, p_TransactionName):
        m_Transaction = self.getTransactionByName(p_TransactionName)
        if m_Transaction is not None:
            raise SQLCliException("SQLCLI-0000: " + "You can't start an existed transaction.")
        m_Transaction = Transaction()
        m_Transaction.Transaction_Name = p_TransactionName
        m_Transaction.Transaction_StartTime = int(time.mktime(datetime.datetime.now().timetuple()))
        self.SaveTransaction(m_Transaction)
        self.SQLExecuteHandler.SQLTransaction = p_TransactionName

    def TransactionEnd(self, p_TransactionName):
        m_Transaction = self.getTransactionByName(p_TransactionName)
        if m_Transaction is None:
            raise SQLCliException("SQLCLI-0000: " + "You can't end a non-existed transaction.")
        m_Transaction.Transaction_EndTime = int(time.mktime(datetime.datetime.now().timetuple()))
        m_Transaction.Transaction_Status = 0
        self.SaveTransaction(m_Transaction)
        self.StatisticsTransaction(m_Transaction)
        self.SQLExecuteHandler.SQLTransaction = ''

    def TransactionFail(self, p_TransactionName):
        m_Transaction = self.getTransactionByName(p_TransactionName)
        if m_Transaction is None:
            raise SQLCliException("SQLCLI-0000: " + "You can't fail a non-existed transaction.")
        m_Transaction.Transaction_EndTime = int(time.mktime(datetime.datetime.now().timetuple()))
        m_Transaction.Transaction_Status = -1
        self.SaveTransaction(m_Transaction)
        self.StatisticsTransaction(m_Transaction)
        self.SQLExecuteHandler.SQLTransaction = ''

    def TransactionShow(self, p_TransactionName):
        m_Header = ["name", "max_time", "min_time", "avg_time", "finished", "failed"]
        m_Result = []

        if p_TransactionName.strip().upper() == "ALL":
            for m_TransactionSatstics in self.getAllTransactionStatistics():
                m_Result.append([m_TransactionSatstics.Transaction_Name,
                                 m_TransactionSatstics.Max_Transaction_Time,
                                 m_TransactionSatstics.Min_Transaction_Time,
                                 m_TransactionSatstics.Sum_Transaction_Time/m_TransactionSatstics.Transaction_Count,
                                 m_TransactionSatstics.Transaction_Count,
                                 m_TransactionSatstics.Transaction_Failed_Count
                                 ])
        else:
            m_TransactionSatstics = self.getTransactionStatisticsByName(p_TransactionName)
            m_Result.append([p_TransactionName,
                             m_TransactionSatstics.Max_Transaction_Time,
                             m_TransactionSatstics.Min_Transaction_Time,
                             m_TransactionSatstics.Sum_Transaction_Time / m_TransactionSatstics.Transaction_Count,
                             m_TransactionSatstics.Transaction_Count,
                             m_TransactionSatstics.Transaction_Failed_Count
                             ])
        return None, m_Result, m_Header, None, "Total [" + str(len(m_Result)) + "] Transactions."

    # 处理Transaction的相关命令
    def Process_Command(self, p_szCommand: str):
        m_szSQL = p_szCommand.strip()

        # 创建新的Transaction
        matchObj = re.match(r"transaction\s+begin\s+(.*)$",
                            m_szSQL, re.IGNORECASE | re.DOTALL)
        if matchObj:
            m_TransactionName = str(matchObj.group(1)).strip()
            self.TransactionBegin(m_TransactionName)
            return None, None, None, None, "Transaction [" + m_TransactionName + "] begin successful."

        # 停止Transaction
        matchObj = re.match(r"transaction\s+end\s+(.*)$",
                            m_szSQL, re.IGNORECASE | re.DOTALL)
        if matchObj:
            m_TransactionName = str(matchObj.group(1)).strip()
            self.TransactionEnd(m_TransactionName)
            return None, None, None, None, "Transaction [" + m_TransactionName + "] end successful."

        # 停止Transaction，并标记为失败
        matchObj = re.match(r"transaction\s+fail\s+(.*)$",
                            m_szSQL, re.IGNORECASE | re.DOTALL)
        if matchObj:
            m_TransactionName = str(matchObj.group(1)).strip()
            self.TransactionFail(m_TransactionName)
            return None, None, None, None, "Transaction [" + m_TransactionName + "] is marked as FAIL."

        # 显示Transaction的统计信息
        matchObj = re.match(r"transaction\s+show\s+(.*)$",
                            m_szSQL, re.IGNORECASE | re.DOTALL)
        if matchObj:
            m_TransactionName = str(matchObj.group(1)).strip()
            return self.TransactionShow(m_TransactionName)

        # 其他未能解析的Transaction命令
        raise SQLCliException("Invalid Transaction Command [" + m_szSQL + "]")
