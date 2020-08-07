# -*- coding: utf-8 -*-
from .sqlparse import SQLAnalyze
from .sqlparse import SQLFormatWithPrefix
from .commandanalyze import execute
from .commandanalyze import CommandNotFound
from .sqlcliexception import SQLCliException
import click
import time
import os
import re
import copy
from time import strftime, localtime
from multiprocessing import Lock
import traceback


class SQLExecute(object):
    conn = None                         # 数据库连接
    logfile = None                      # 打印的日志文件
    sqlscript = None                    # 需要执行的SQL脚本
    SQLMappingHandler = None            # SQL重写处理
    SQLOptions = None                   # 程序处理参数
    Console = False                     # 屏幕输出Console
    logger = None                       # 日志输出

    m_PerfFileLocker = Lock()           # # 进程锁, 用来在输出perf文件的时候控制并发写文件

    m_Shutdown_Flag = False             # 在程序的每个语句，Sleep间歇中都会判断这个进程终止标志

    SQLPerfFile = None                  # SQLPerf文件
    SQLPerfFileHandle = None            # SQLPerf文件句柄

    def __init__(self):
        # 记录最后SQL返回的结果
        self.LastAffectedRows = 0
        self.LastSQLResult = None
        self.LastElapsedTime = 0

        # 当前Execute的WorkerName
        self.m_Worker_Name = None

        # 程序Spool输出句柄
        self.spoolfile = None

    def set_Worker_Name(self, p_szWorkerName):
        self.m_Worker_Name = p_szWorkerName

    def set_logfile(self, p_logfile):
        self.logfile = p_logfile

    def set_connection(self, p_conn):
        self.conn = p_conn

    def run(self, statement, p_sqlscript=None):
        """
        返回结果包含5个方面的内容
        (title, rows, headers, columntypes, status).
        """
        m_SQL_Status = 0             # SQL 运行结果， 0 成功， 1 失败
        m_SQL_ErrorMessage = ""      # 错误日志信息

        # Remove spaces and EOL
        statement = statement.strip()
        if not statement:  # Empty string
            yield None, None, None, None, None

        # 分析SQL语句
        (ret_bSQLCompleted, ret_SQLSplitResults,
         ret_SQLSplitResultsWithComments, ret_SQLFlags) = SQLAnalyze(statement)
        for m_nPos in range(0, len(ret_SQLSplitResults)):

            sql = ret_SQLSplitResults[m_nPos]
            m_CommentSQL = ret_SQLSplitResultsWithComments[m_nPos]
            m_SQLFlag = ret_SQLFlags[m_nPos]
            if "Feature" in m_SQLFlag.keys():
                m_SQLFeature = m_SQLFlag["Feature"]
            else:
                m_SQLFeature = ""
            # 如果打开了回显，并且指定了输出文件，则在输出文件里显示SQL语句
            if self.SQLOptions.get("ECHO").upper() == 'ON' \
                    and len(m_CommentSQL.strip()) != 0 and self.logfile is not None:
                click.echo(SQLFormatWithPrefix(m_CommentSQL), file=self.logfile)
            if self.SQLOptions.get("ECHO").upper() == 'ON' \
                    and len(m_CommentSQL.strip()) != 0 and self.spoolfile is not None:
                # 在spool文件中，不显示spool off的信息，以避免log比对中的不必要内容
                if not re.match(r'spool\s+.*', m_CommentSQL.strip(), re.IGNORECASE):
                    click.echo(SQLFormatWithPrefix(m_CommentSQL), file=self.spoolfile)
            # 在logger中显示执行的SQL
            if self.logger is not None:
                self.logger.info(SQLFormatWithPrefix(m_CommentSQL))
            # 如果运行在脚本模式下，需要在控制台额外回显SQL
            # 非脚本模式下，由于是用户自行输入，所以不需要回显输入的SQL
            if p_sqlscript is not None:
                click.echo(SQLFormatWithPrefix(m_CommentSQL), file=self.Console)

            # 如果打开了回显，并且指定了输出文件，且SQL被改写过，输出改写后的SQL
            if self.SQLOptions.get("SQLREWRITE").upper() == 'ON':
                old_sql = sql
                sql = self.SQLMappingHandler.RewriteSQL(p_sqlscript, old_sql)
                if old_sql != sql:    # SQL已经发生了改变
                    if self.SQLOptions.get("ECHO").upper() == 'ON' and self.logfile is not None:
                        click.echo(SQLFormatWithPrefix(
                            "Your SQL has been changed to:\n" + sql, 'REWROTED '), file=self.logfile)
                    if self.SQLOptions.get("ECHO").upper() == 'ON' and self.spoolfile is not None:
                        click.echo(SQLFormatWithPrefix("Your SQL has been changed to:\n" + sql, 'REWROTED '),
                                   file=self.spoolfile)
                    if self.logger is not None:
                        self.logger.info(SQLFormatWithPrefix("Your SQL has been changed to:\n" + sql, 'REWROTED '))
                    click.echo(SQLFormatWithPrefix(
                        "Your SQL has been changed to:\n" + sql, 'REWROTED '), file=self.Console)

            # 如果是空语句，不在执行
            if len(sql.strip()) == 0:
                continue

            # 打开游标
            cur = self.conn.cursor() if self.conn else None

            # 记录命令开始时间
            start = time.time()

            # 检查SQL中是否包含特殊内容
            # 特殊内容都有：
            # 1. %lastsqlresult.LastAffectedRows%
            #    上一个SQL影响的行数
            # 2. %lastsqlresult.LastSQLResult[X][Y]%
            #    上一个SQL返回的记录集
            # 3. ${var}
            #    用户定义的变量

            # lastsqlresult.LastAffectedRows
            if sql.find("%lastsqlresult.LastAffectedRows%") != -1:
                sql = sql.replace("%lastsqlresult.LastAffectedRows%", str(self.LastAffectedRows))
                if self.SQLOptions.get("ECHO").upper() == 'ON' and self.logfile is not None:
                    click.echo(SQLFormatWithPrefix(
                        "Your SQL has been changed to:\n" + sql, 'REWROTED '), file=self.logfile)
                if self.SQLOptions.get("ECHO").upper() == 'ON' and self.spoolfile is not None:
                    click.echo(SQLFormatWithPrefix("Your SQL has been changed to:\n" + sql, 'REWROTED '),
                               file=self.spoolfile)
                click.echo(SQLFormatWithPrefix(
                    "Your SQL has been changed to:\n" + sql, 'REWROTED '), file=self.Console)
                if self.logger is not None:
                    self.logger.info(SQLFormatWithPrefix("Your SQL has been changed to:\n" + sql, 'REWROTED '))

            # lastsqlresult.LastSQLResult[X][Y]
            matchObj = re.search(r"%lastsqlresult.LastSQLResult\[(\d+)\]\[(\d+)\]%",
                                 sql, re.IGNORECASE | re.DOTALL)
            if matchObj:
                m_Searched = matchObj.group(0)
                m_nRow = int(matchObj.group(1))
                m_nColumn = int(matchObj.group(2))
                if m_nRow >= len(self.LastSQLResult):
                    m_Result = ""
                elif m_nColumn >= len(self.LastSQLResult[m_nRow]):
                    m_Result = ""
                else:
                    m_Result = str(self.LastSQLResult[m_nRow][m_nColumn])
                sql = sql.replace(m_Searched, m_Result)
                if self.SQLOptions.get("ECHO").upper() == 'ON' and self.logfile is not None:
                    click.echo(SQLFormatWithPrefix(
                        "Your SQL has been changed to:\n" + sql, 'REWROTED '), file=self.logfile)
                if self.SQLOptions.get("ECHO").upper() == 'ON' and self.spoolfile is not None:
                    click.echo(SQLFormatWithPrefix("Your SQL has been changed to:\n" + sql, 'REWROTED '),
                               file=self.spoolfile)
                if self.logger is not None:
                    self.logger.info(SQLFormatWithPrefix("Your SQL has been changed to:\n" + sql, 'REWROTED '))
                click.echo(SQLFormatWithPrefix(
                    "Your SQL has been changed to:\n" + sql, 'REWROTED '), file=self.Console)

            # ${var}
            bMatched = False
            while True:
                matchObj = re.search(r"(.*)\${([0-9A-Za-z_\-]+)\}(.*)?",
                                     sql, re.IGNORECASE | re.DOTALL)
                if matchObj:
                    bMatched = True
                    m_VarName = matchObj.group(2)
                    if not m_VarName.startswith('@'):
                        m_VarName = '@' + m_VarName
                    m_VarValue = self.SQLOptions.get(m_VarName)
                    if m_VarValue is not None:
                        sql = matchObj.group(1) + m_VarValue + matchObj.group(3)
                else:
                    break
            if bMatched:
                if self.SQLOptions.get("ECHO").upper() == 'ON' and self.logfile is not None:
                    # SQL已经发生了改变, 会将改变后的SQL信息在屏幕上单独显示出来
                    click.echo(SQLFormatWithPrefix(
                        "Your SQL has been changed to:\n" + sql, 'REWROTED '), file=self.logfile)
                if self.SQLOptions.get("ECHO").upper() == 'ON' and self.spoolfile is not None:
                    click.echo(SQLFormatWithPrefix("Your SQL has been changed to:\n" + sql, 'REWROTED '),
                               file=self.spoolfile)
                if self.logger is not None:
                    self.logger.info(SQLFormatWithPrefix("Your SQL has been changed to:\n" + sql, 'REWROTED '))
                click.echo(SQLFormatWithPrefix(
                    "Your SQL has been changed to:\n" + sql, 'REWROTED '), file=self.Console)

            # 执行SQL
            try:
                # 首先假设这是一个特殊命令
                for result in execute(cur, sql):
                    yield result
            except CommandNotFound:
                if cur is None:
                    # 进入到SQL执行阶段，但是没有conn，也不是特殊命令
                    if self.SQLOptions.get("WHENEVER_SQLERROR") == "EXIT":
                        raise SQLCliException("Not Connected. ")
                    else:
                        self.LastAffectedRows = 0
                        self.LastSQLResult = None
                        yield None, None, None, None, "Not connected. "

                # 执行正常的SQL语句
                if cur is not None:
                    try:
                        if "SQLCLI_DEBUG" in os.environ:
                            click.secho("DEBUG-SQL=[" + str(sql) + "]", file=self.logfile)
                        # 执行SQL脚本
                        cur.execute(sql)
                        rowcount = 0
                        while True:
                            (title, result, headers, columntypes, status, m_FetchStatus, m_FetchedRows) = \
                                self.get_result(cur, rowcount)
                            rowcount = m_FetchedRows

                            self.LastAffectedRows = rowcount
                            self.LastSQLResult = copy.copy(result)

                            # 检查SQLFlags
                            # 如果有order字样，对结果进行排序后再输出
                            if "Order" in m_SQLFlag.keys():
                                for i in range(1, len(result)):
                                    for j in range(0, len(result) - i):
                                        if str(result[j]) > str(result[j + 1]):
                                            result[j], result[j + 1] = result[j + 1], result[j]
                            yield title, result, headers, columntypes, status
                            if not m_FetchStatus:
                                break

                        # 记录结束时间
                        end = time.time()

                        # 记录SQL日志信息
                        self.Log_Perf(
                            {
                                "StartedTime": start,
                                "elapsed": end - start,
                                "SQL": sql,
                                "SQLStatus": m_SQL_Status,
                                "Feature": m_SQLFeature,
                                "ErrorMessage": m_SQL_ErrorMessage,
                                "thread_name": self.m_Worker_Name
                            }
                        )

                    except Exception as e:
                        end = time.time()  # 记录结束时间
                        m_SQL_Status = 1
                        m_SQL_ErrorMessage = str(e)
                        self.Log_Perf(
                            {
                                "StartedTime": start,
                                "elapsed": end - start,
                                "SQL": sql,
                                "SQLStatus": m_SQL_Status,
                                "Feature": m_SQLFeature,
                                "ErrorMessage": m_SQL_ErrorMessage,
                                "thread_name": self.m_Worker_Name
                            }
                        )

                        if (
                                str(e).find("SQLSyntaxErrorException") != -1 or
                                str(e).find("SQLException") != -1 or
                                str(e).find("SQLDataException") != -1 or
                                str(e).find("SQLTransactionRollbackException") != -1 or
                                str(e).find("SQLTransientConnectionException") != -1 or
                                str(e).find('time data') != -1
                        ):
                            # 发生了SQL语法错误
                            if self.SQLOptions.get("WHENEVER_SQLERROR") == "EXIT":
                                raise SQLCliException(str(e))
                            else:
                                self.LastAffectedRows = 0
                                self.LastSQLResult = None
                                yield None, None, None, None, str(e)
                        else:
                            # 发生了其他不明错误
                            raise e
            except SQLCliException as e:
                m_SQL_Status = 1
                m_SQL_ErrorMessage = str(e.message)
                # 如果要求出错退出，就立刻退出，否则打印日志信息
                if self.SQLOptions.get("WHENEVER_SQLERROR") == "EXIT":
                    raise e
                else:
                    if "SQLCLI_DEBUG" in os.environ:
                        print('traceback.print_exc():\n%s' % traceback.print_exc())
                        print('traceback.format_exc():\n%s' % traceback.format_exc())
                    self.LastAffectedRows = 0
                    self.LastSQLResult = None
                    yield None, None, None, None, str(e.message)

            # 如果需要，打印语句执行时间
            end = time.time()
            self.LastElapsedTime = end - start

            if "TimeLimit" in m_SQLFlag.keys():
                m_TimeLimit = m_SQLFlag["TimeLimit"]
                m_TimeCost = '%9.0f' % ((end - start) * 1000)
                if m_TimeLimit < (end - start) * 1000:
                    raise SQLCliException('LinkoopSQL-Err:: ' 
                                          'SQL elapsed time [' + m_TimeCost + '] over limit [' + str(m_TimeLimit) + ']')
                    # yield None, None, None, None, 'SQL elapsed time over limit [' + str(m_TimeLimit) + ']'

            if self.SQLOptions.get('TIMING').upper() == 'ON':
                if sql.strip().upper() not in ('EXIT', 'QUIT'):
                    yield None, None, None, None, 'Running time elapsed: %9.2f Seconds' % (end - start)
            if self.SQLOptions.get('TIME').upper() == 'ON':
                if sql.strip().upper() not in ('EXIT', 'QUIT'):
                    yield None, None, None, None, 'Current clock time  :' + strftime("%Y-%m-%d %H:%M:%S", localtime())

    def get_result(self, cursor, rowcount):
        """Get the current result's data from the cursor."""
        title = headers = None
        m_FetchStatus = True

        # cursor.description is not None for queries that return result sets,
        # e.g. SELECT.
        result = []
        columntypes = []
        if cursor.description is not None:
            headers = [x[0] for x in cursor.description]
            status = "{0} row{1} selected."
            m_arraysize = int(self.SQLOptions.get("ARRAYSIZE"))
            rowset = list(cursor.fetchmany(m_arraysize))
            if self.SQLOptions.get('TERMOUT').upper() != 'OFF':
                for row in rowset:
                    m_row = []
                    # 记录字段类型
                    if len(columntypes) == 0:
                        for column in row:
                            columntypes.append(type(column))
                    for column in row:
                        if str(type(column)).upper().find('OBJECT[]') != -1:
                            m_ColumnValue = "STRUCTURE("
                            for m_nPos in range(0, len(column)):
                                m_ColumnType = str(type(column[m_nPos]))
                                if m_nPos == 0:
                                    if m_ColumnType.upper().find('STR') != -1:
                                        m_ColumnValue = m_ColumnValue + "'" + str(column[m_nPos]) + "'"
                                    elif m_ColumnType.upper().find('SQLDATE') != -1:
                                        m_ColumnValue = m_ColumnValue + "DATE'" + str(column[m_nPos]) + "'"
                                    elif str(type(column)).upper().find("FLOAT") != -1:
                                        m_ColumnValue = m_ColumnValue + \
                                                        self.SQLOptions.get("FLOAT_FORMAT") % column[m_nPos]
                                    elif str(type(column)).upper().find("DOUBLE") != -1:
                                        m_ColumnValue = m_ColumnValue + \
                                                        self.SQLOptions.get("DOUBLE_FORMAT") % column[m_nPos]
                                    else:
                                        m_ColumnValue = m_ColumnValue + str(column[m_nPos])
                                else:
                                    if m_ColumnType.upper().find('STR') != -1:
                                        m_ColumnValue = m_ColumnValue + ",'" + str(column[m_nPos]) + "'"
                                    elif m_ColumnType.upper().find('SQLDATE') != -1:
                                        m_ColumnValue = m_ColumnValue + ",DATE'" + str(column[m_nPos]) + "'"
                                    elif str(type(column)).upper().find("FLOAT") != -1:
                                        m_ColumnValue = m_ColumnValue + "," + \
                                                        self.SQLOptions.get("FLOAT_FORMAT") % column[m_nPos]
                                    elif str(type(column)).upper().find("DOUBLE") != -1:
                                        m_ColumnValue = m_ColumnValue + "," + \
                                                        self.SQLOptions.get("DOUBLE_FORMAT") % column[m_nPos]
                                    else:
                                        m_ColumnValue = m_ColumnValue + "," + str(column[m_nPos])
                            m_ColumnValue = m_ColumnValue + ")"
                            m_row.append(m_ColumnValue)
                        elif str(type(column)).upper().find('JDBCCLOBCLIENT') != -1:
                            m_Length = column.length()
                            m_ColumnValue = column.getSubString(1, int(self.SQLOptions.get("LOB_LENGTH")))
                            if m_Length > int(self.SQLOptions.get("LOB_LENGTH")):
                                m_ColumnValue = m_ColumnValue + "..."
                            m_row.append(m_ColumnValue)
                        elif str(type(column)).upper().find('JDBCBLOBCLIENT') != -1:
                            # 对于二进制数据，用16进制数来显示
                            # 2: 意思是去掉Hex前面的0x字样
                            m_Length = column.length()
                            m_ColumnValue = "".join([hex(int(i))[2:]
                                                     for i in column.getBytes(1,
                                                                              int(self.SQLOptions.get("LOB_LENGTH")))])
                            if m_Length > int(self.SQLOptions.get("LOB_LENGTH")):
                                m_ColumnValue = m_ColumnValue + "..."
                            m_row.append(m_ColumnValue)
                        elif str(type(column)).upper().find("FLOAT") != -1:
                            m_row.append(self.SQLOptions.get("FLOAT_FORMAT") % column)
                        elif str(type(column)).upper().find("DOUBLE") != -1:
                            m_row.append(self.SQLOptions.get("DOUBLE_FORMAT") % column)
                        elif str(type(column)).upper().find('STR') != -1:
                            # 对于二进制数据，其末尾用0x00表示，这里进行截断
                            m_0x00Start = column.find(chr(0))
                            if m_0x00Start != -1:
                                column = column[0:m_0x00Start]
                            m_row.append(column)
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

        if self.SQLOptions.get('FEEDBACK').upper() == 'ON' and status is not None:
            status = status.format(rowcount, "" if rowcount == 1 else "s")
        else:
            status = None
        if self.SQLOptions.get('TERMOUT').upper() == 'OFF':
            return title, [], headers, columntypes, status, m_FetchStatus, rowcount
        else:
            return title, result, headers, columntypes, status, m_FetchStatus, rowcount

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
        try:
            self.m_PerfFileLocker.acquire()
            if not os.path.exists(self.SQLPerfFile):
                # 如果文件不存在，创建文件，并写入文件头信息
                self.SQLPerfFileHandle = open(self.SQLPerfFile, "a", encoding="utf-8")
                self.SQLPerfFileHandle.write("Script\tStarted\telapsed\tSQLPrefix\t"
                                             "SQLStatus\tErrorMessage\tthread_name\t"
                                             "Copies\tFinished\tFeature\n")
                self.SQLPerfFileHandle.close()

            if len(str(p_SQLResult["thread_name"]).split('-')) == 3:
                # 对于多线程运行，这里的thread_name通常为JOB_NAME, 副本数，循环次数
                m_ThreadName = str(p_SQLResult["thread_name"]).split('-')[0]
                m_Copies = str(p_SQLResult["thread_name"]).split('-')[1]
                m_CurrentLoop = int(str(p_SQLResult["thread_name"]).split('-')[2]) + 1
            else:
                # 对于单线程运行，这里的thread_name通常为JOB_NAME, 1， 1
                m_ThreadName = str(p_SQLResult["thread_name"])
                m_Copies = 1
                m_CurrentLoop = 1

            # 打开Perf文件
            self.SQLPerfFileHandle = open(self.SQLPerfFile, "a", encoding="utf-8")
            # 写入内容信息
            self.SQLPerfFileHandle.write(
                "'" + str(os.path.basename(self.sqlscript)) + "'\t" +
                "'" + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(p_SQLResult["StartedTime"])) + "'\t" +
                "%8.2f" % p_SQLResult["elapsed"] + "\t" +
                "'" + str(p_SQLResult["SQL"]).replace("\n", " ").replace("\t", "    ") + "'\t" +
                str(p_SQLResult["SQLStatus"]) + "\t" +
                "'" + str(p_SQLResult["ErrorMessage"]).replace("\n", " ").replace("\t", "    ") + "'\t" +
                "'" + str(m_ThreadName) + "'\t" +
                str(m_Copies) + "'\t" + str(m_CurrentLoop) + "\t" +
                "'" + str(p_SQLResult["Feature"]) + "'" +
                "\n"
            )
            self.SQLPerfFileHandle.flush()
            self.SQLPerfFileHandle.close()
        except Exception as ex:
            print("Internal error:: perf file write not complete. " + repr(ex))
        finally:
            self.m_PerfFileLocker.release()
