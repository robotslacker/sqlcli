# -*- coding: utf-8 -*-
import re
import time
import datetime
from multiprocessing import Lock
from .sqlcliexception import SQLCliException


# Transaction事务管理
class TransactionManager(object):
    def __init__(self):
        # 初始化进程锁, 用于对进程信息的共享
        self.TransactionLocker = Lock()

        # 是否已经注册
        self._isRegistered = False

        # 后台共享信息
        self.SharedProcessInfoHandler = None

        # 当前会话的事务性消息
        self.transactions = {}

        # SQL执行句柄
        self.SQLExecuteHandler = None

    # 设置全局共享内存信息
    def setSharedProcessInfo(self, p_objSharedProcessInfo):
        self.SharedProcessInfoHandler = p_objSharedProcessInfo

    def TransactionBegin(self, p_TransactionName):
        if p_TransactionName in self.transactions.keys():
            raise SQLCliException("SQLCLI-0000: " + "You can't begin a existed transaction.")
        self.transactions[p_TransactionName] = {"start_time": int(time.mktime(datetime.datetime.now().timetuple()))}
        if self.SQLExecuteHandler is not None:
            self.SQLExecuteHandler.SQLTransaction = p_TransactionName

    def TransactionEnd(self, p_TransactionName):
        if p_TransactionName not in self.transactions.keys():
            raise SQLCliException("SQLCLI-0000: " + "You can't end a non-existed a transaction.")
        m_TransactionInfo = {"start_time": self.transactions[p_TransactionName]["start_time"],
                             "status": 0}
        self.transactions.pop(p_TransactionName)
        self.SQLExecuteHandler.SQLTransaction = ''

        # 补充事务信息到共享内存中
        self.TransactionLocker.acquire()
        self.SharedProcessInfoHandler.Append_Transaction(p_TransactionName, m_TransactionInfo)
        self.TransactionLocker.release()

    def TransactionFail(self, p_TransactionName):
        if p_TransactionName not in self.transactions.keys():
            raise SQLCliException("SQLCLI-0000: " + "You can't fail a non-existed a transaction.")
        m_TransactionInfo = {"start_time": self.transactions[p_TransactionName]["start_time"],
                             "status": 1}
        self.transactions.pop(p_TransactionName)

        # 补充事务信息到共享内存中
        self.TransactionLocker.acquire()
        self.SharedProcessInfoHandler.Append_Transaction(p_TransactionName, m_TransactionInfo)
        self.TransactionLocker.release()

    def TransactionShow(self, p_TransactionName):
        m_Header = ["name", "max_time", "min_time", "avg_time", "finished", "failed"]
        m_Result = []

        if p_TransactionName.strip().upper() == "ALL":
            for (m_TransactionName, m_TransactionSatstics) in \
                    self.SharedProcessInfoHandler.getAllTransactionStatistics().items():
                m_Result.append([m_TransactionName,
                                 m_TransactionSatstics.max_transaction_time,
                                 m_TransactionSatstics.min_transaction_time,
                                 m_TransactionSatstics.sum_transaction_time/m_TransactionSatstics.transaction_count,
                                 m_TransactionSatstics.transaction_count,
                                 m_TransactionSatstics.transaction_failed_count
                                 ])
        else:
            m_TransactionSatstics = self.SharedProcessInfoHandler.getTransactionStatistics(p_TransactionName)
            m_Result.append([p_TransactionName,
                             m_TransactionSatstics.max_transaction_time,
                             m_TransactionSatstics.min_transaction_time,
                             m_TransactionSatstics.sum_transaction_time / m_TransactionSatstics.transaction_count,
                             m_TransactionSatstics.transaction_count,
                             m_TransactionSatstics.transaction_failed_count
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
