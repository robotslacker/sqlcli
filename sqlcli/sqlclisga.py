# -*- coding: utf-8 -*-
import time
import datetime


class TransactionStaistics:
    def __init__(self):
        self.max_transaction_time = 0                  # transaction 最大耗时
        self.sum_transaction_time = 0                  # transaction 累计耗时
        self.min_transaction_time = 0                  # transaction 最小耗时
        self.transaction_count = 0                     # transaction 次数
        self.transaction_failed_count = 0              # transaction 失败次数


class SQLCliGlobalSharedMemory(object):
    # ================================================
    # SQLCli的所有进程共享此结构对象
    # ================================================

    def __init__(self):
        # 当前的Transaction流水号
        self.m_TransactionID = 1

        # 所有的Transaction统计信息
        self.m_TransactionStaistics = dict()

    # 显示全部的Transaction统计信息
    def getAllTransactionStatistics(self):
        return self.m_TransactionStaistics

    # 显示当前的Transaction统计信息
    def getTransactionStatistics(self, p_TransactionName: str):
        return self.m_TransactionStaistics[p_TransactionName]

    # 添加新的Transaction信息到共享内存中
    def Append_Transaction(self, p_TransactionName: str, p_TransactionInfo: dict):
        m_start_time = p_TransactionInfo["start_time"]

        m_end_time = int(time.mktime(datetime.datetime.now().timetuple()))

        # 合并统计信息
        if p_TransactionName in self.m_TransactionStaistics.keys():
            m_TransactionStaistics = self.m_TransactionStaistics[p_TransactionName]
            if (m_end_time - m_start_time) > m_TransactionStaistics.max_transaction_time:
                m_TransactionStaistics.max_transaction_time = m_end_time - m_start_time
            if (m_end_time - m_start_time) < m_TransactionStaistics.min_transaction_time:
                m_TransactionStaistics.min_transaction_time = m_end_time - m_start_time
            m_TransactionStaistics.sum_transaction_time = \
                m_TransactionStaistics.sum_transaction_time + m_end_time - m_start_time
            m_TransactionStaistics.transaction_count = m_TransactionStaistics.transaction_count + 1
            if p_TransactionInfo['status'] != 0:
                m_TransactionStaistics.transaction_failed_count = \
                    m_TransactionStaistics.transaction_failed_count + 1
            self.m_TransactionStaistics[p_TransactionName] = m_TransactionStaistics
        else:
            m_TransactionStaistics = TransactionStaistics()
            m_TransactionStaistics.max_transaction_time = m_end_time - m_start_time
            m_TransactionStaistics.min_transaction_time = m_end_time - m_start_time
            m_TransactionStaistics.sum_transaction_time = m_end_time - m_start_time
            m_TransactionStaistics.transaction_count = 1
            if p_TransactionInfo['status'] != 0:
                m_TransactionStaistics.transaction_failed_count = 1
            else:
                m_TransactionStaistics.transaction_failed_count = 0
            self.m_TransactionStaistics[p_TransactionName] = m_TransactionStaistics
