# -*- coding: utf-8 -*-
from .sqlparse import SQLAnalyze
from .sqlparse import SQLFormatWithPrefix
from .commandanalyze import execute
from .commandanalyze import CommandNotFound
from .sqlcliexception import SQLCliException
import click
import time
import os
from time import strftime, localtime
from multiprocessing import Lock


class SQLExecute(object):
    conn = None                         # 数据库连接
    options = None                      # 用户设置的各种选项
    logfile = None                      # 打印的日志文件
    sqlscript = None                    # 需要执行的SQL脚本
    SQLMappingHandler = None            # SQL重写处理
    m_Current_RunningSQL = None         # 目前程序运行的当前SQL
    Console = False                     # 屏幕输出Console
    logger = None                       # 日志输出
    m_Worker_Name = None                # 为每个SQLExecute实例起一个名字，便于统计分析

    # 进程锁, 用来在输出perf文件的时候控制并发写文件
    m_PerfFileLocker = Lock()

    # 进程终止标志
    m_Shutdown_Flag = False             # 在程序的每个语句，Sleep间歇中都会判断这个标志
    m_Abort_Flag = False                # 应用被强行终止，可能会有SQL错误

    # SQLPerf文件
    SQLPerfFile = None
    SQLPerfFileHandle = None

    def __init__(self):
        # 设置一些默认的参数
        self.options = {"WHENEVER_SQLERROR": "CONTINUE", "PAGE": "OFF", "OUTPUT_FORMAT": "ASCII", "ECHO": "ON",
                        "LONG": 20, 'TIMING': 'OFF', 'TIME': 'OFF', 'TERMOUT': 'ON',
                        'FEEDBACK': 'ON', "ARRAYSIZE": 10000, 'SQLREWRITE': 'ON', "DEBUG": 'OFF',
                        'HDFS_WEBFSURL': None, 'HDFS_WEBFSROOT': "/", 'KAFKA_SERVERS': None, }

    def set_logfile(self, p_logfile):
        self.logfile = p_logfile

    def set_connection(self, p_conn):
        self.conn = p_conn

    def run(self, statement, p_sqlscript=None):
        """Execute the sql in the database and return the results. The results
        are a list of tuples. Each tuple has 4 values
        (title, rows, headers, status).
        """
        m_SQL_Status = 0             # SQL 运行结果， 0 成功， 1 失败
        m_SQL_ErrorMessage = ""      # 错误日志信息

        # Remove spaces and EOL
        statement = statement.strip()
        if not statement:  # Empty string
            yield None, None, None, None

        # 分析SQL语句
        (ret_bSQLCompleted, ret_SQLSplitResults, ret_SQLSplitResultsWithComments) = SQLAnalyze(statement)
        for m_nPos in range(0, len(ret_SQLSplitResults)):

            sql = ret_SQLSplitResults[m_nPos]
            m_CommentSQL = ret_SQLSplitResultsWithComments[m_nPos]

            # 如果打开了回显，并且指定了输出文件，则在输出文件里显示SQL语句
            if self.options["ECHO"] == 'ON' and len(m_CommentSQL.strip()) != 0 and self.logfile is not None:
                click.echo(SQLFormatWithPrefix(m_CommentSQL), file=self.logfile)

            # 如果运行在脚本模式下，需要在控制台回显SQL
            if p_sqlscript is not None:
                click.secho(SQLFormatWithPrefix(m_CommentSQL), file=self.Console)

            # 如果打开了回显，并且指定了输出文件，且SQL被改写过，输出改写后的SQL
            if self.options["SQLREWRITE"] == 'ON':
                old_sql = sql
                sql = self.SQLMappingHandler.RewriteSQL(p_sqlscript, old_sql)
                if old_sql != sql:
                    if self.options["ECHO"] == 'ON' and self.logfile is not None:
                        # SQL已经发生了改变
                        click.echo(SQLFormatWithPrefix(
                            "Your SQL has been changed to:\n" + sql, 'REWROTED '), file=self.logfile)
                    if p_sqlscript is not None:
                        click.secho(SQLFormatWithPrefix(
                            "Your SQL has been changed to:\n" + sql, 'REWROTED '), file=self.Console)

            # 如果是空语句，不在执行
            if len(sql.strip()) == 0:
                continue

            # 打开游标
            cur = self.conn.cursor() if self.conn else None

            # 记录命令开始时间
            start = time.time()
            self.m_Current_RunningSQL = sql

            # 执行SQL
            try:
                # 首先假设这是一个特殊命令
                for result in execute(cur, sql):
                    yield result
            except CommandNotFound:
                if cur is None:
                    # 进入到SQL执行阶段，但是没有conn，也不是特殊命令
                    if self.options["WHENEVER_SQLERROR"] == "EXIT":
                        raise SQLCliException("Not Connected. ")
                    else:
                        yield None, None, None, "Not connected. "

                # 执行正常的SQL语句
                if cur is not None:
                    try:
                        if "SQLCLI_DEBUG" in os.environ:
                            click.secho("DEBUG-SQL=[" + str(sql) + "]", file=self.logfile)
                        cur.execute(sql)
                        rowcount = 0
                        while True:
                            (title, result, headers, status, m_FetchStatus, m_FetchedRows) = \
                                self.get_result(cur, rowcount)
                            rowcount = m_FetchedRows
                            yield title, result, headers, status
                            if not m_FetchStatus:
                                break
                    except Exception as e:
                        if (
                                str(e).find("SQLSyntaxErrorException") != -1 or
                                str(e).find("SQLException") != -1 or
                                str(e).find("SQLDataException") != -1
                        ):
                            # SQL 语法错误
                            if self.options["WHENEVER_SQLERROR"] == "EXIT":
                                raise SQLCliException(str(e))
                            else:
                                yield None, None, None, str(e)
                        else:
                            # 其他不明错误
                            m_SQL_Status = 1
                            m_SQL_ErrorMessage = str(e)
                            # 记录结束时间
                            end = time.time()

                            # 记录性能日志信息
                            self.Log_Perf(
                                {
                                    "StartedTime": start,
                                    "elapsed": end - start,
                                    "SQL": sql,
                                    "SQLStatus": m_SQL_Status,
                                    "ErrorMessage": m_SQL_ErrorMessage,
                                    "thread_name": self.m_Worker_Name
                                }
                            )
                            raise e
            except SQLCliException as e:
                m_SQL_Status = 1
                m_SQL_ErrorMessage = str(e.message)
                if self.options["WHENEVER_SQLERROR"] == "EXIT":
                    # 记录结束时间
                    end = time.time()

                    # 记录性能日志信息
                    self.Log_Perf(
                        {
                            "StartedTime": start,
                            "elapsed": end - start,
                            "SQL": sql,
                            "SQLStatus": m_SQL_Status,
                            "ErrorMessage": m_SQL_ErrorMessage,
                            "thread_name": self.m_Worker_Name
                        }
                    )
                    raise e
                else:
                    yield None, None, None, str(e.message)

            # 记录结束时间
            end = time.time()

            # 记录性能日志信息
            self.Log_Perf(
                {
                    "StartedTime": start,
                    "elapsed": end - start,
                    "SQL": sql,
                    "SQLStatus": m_SQL_Status,
                    "ErrorMessage": m_SQL_ErrorMessage,
                    "thread_name": self.m_Worker_Name
                }
            )
            self.m_Current_RunningSQL = None

            # 如果需要，打印语句执行时间
            if self.options['TIMING'] == 'ON':
                if sql.strip().upper() not in ('EXIT', 'QUIT'):
                    yield None, None, None, 'Running time elapsed: %9.2f Seconds' % (end - start)
            if self.options['TIME'] == 'ON':
                if sql.strip().upper() not in ('EXIT', 'QUIT'):
                    yield None, None, None, 'Current clock time  :' + strftime("%Y-%m-%d %H:%M:%S", localtime())

    def get_Current_RunningSQL(self):
        return self.m_Current_RunningSQL

    def get_result(self, cursor, rowcount):
        """Get the current result's data from the cursor."""
        title = headers = None
        m_FetchStatus = True

        # cursor.description is not None for queries that return result sets,
        # e.g. SELECT.
        result = []
        if cursor.description is not None:
            headers = [x[0] for x in cursor.description]
            status = "{0} row{1} selected."
            m_arraysize = int(self.options["ARRAYSIZE"])
            rowset = list(cursor.fetchmany(m_arraysize))
            if self.options['TERMOUT'] != 'OFF':
                for row in rowset:
                    m_row = []
                    for column in row:
                        if str(type(column)).find('JDBCClobClient') != -1:
                            m_row.append(column.getSubString(1, int(self.options["LONG"])))
                        else:
                            m_row.append(column)
                    m_row = tuple(m_row)
                    result.append(m_row)
            rowcount = rowcount + len(rowset)
            if len(rowset) < m_arraysize:
                # 已经没有什么可以取的了, 游标结束
                m_FetchStatus = False

        else:
            status = "{0} row{1} affected"
            rowcount = 0 if cursor.rowcount == -1 else cursor.rowcount
            result = None
            m_FetchStatus = False

        # 只要不是最后一次打印，不再返回status内容
        if m_FetchStatus:
            status = None

        if self.options['FEEDBACK'] == 'ON' and status is not None:
            status = status.format(rowcount, "" if rowcount == 1 else "s")
        else:
            status = None
        if self.options['TERMOUT'] == 'OFF':
            return title, [], headers, status, m_FetchStatus, rowcount
        else:
            return title, result, headers, status, m_FetchStatus, rowcount

    # 记录PERF信息，
    def Log_Perf(self, p_SQLResult):
        # 开始时间         StartedTime
        # 消耗时间         elapsed
        # SQL的前20个字母  SQLPrefix
        # 运行状态         SQLStatus
        # 错误日志         ErrorMessage
        # 线程名称         thread_name

        # 如果没有打开性能日志记录文件，直接跳过
        if self.SQLPerfFile is None:
            return

        # 多进程，多线程写入，考虑锁冲突
        self.m_PerfFileLocker.acquire()
        if self.SQLPerfFileHandle is None:
            if self.m_Worker_Name.upper() == "MAIN":
                self.SQLPerfFileHandle = open(self.SQLPerfFile, "w", encoding="utf-8")
                self.SQLPerfFileHandle.write("Started\telapsed\tSQLPrefix\tSQLStatus\tErrorMessage\tthread_name\n")
                self.SQLPerfFileHandle.close()
        # 打开Perf文件
        self.SQLPerfFileHandle = open(self.SQLPerfFile, "a", encoding="utf-8")

        # 写入内容信息
        self.SQLPerfFileHandle.write(
            "'" + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(p_SQLResult["StartedTime"])) + "'\t" +
            "%8.2f" % p_SQLResult["elapsed"] + "\t" +
            "'" + str(p_SQLResult["SQL"][0:40]).replace("\n", " ").replace("\t", "    ") + "'\t" +
            str(p_SQLResult["SQLStatus"]) + "\t" +
            "'" + str(p_SQLResult["ErrorMessage"]) + "'\t" +
            "'" + str(p_SQLResult["thread_name"] + "'" + "\n")
        )
        self.SQLPerfFileHandle.flush()
        self.SQLPerfFileHandle.close()
        self.m_PerfFileLocker.release()
