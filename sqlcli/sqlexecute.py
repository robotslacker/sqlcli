# -*- coding: utf-8 -*-
import decimal

import click
import time
import os
import re
import copy
from time import strftime, localtime
from multiprocessing import Lock
import traceback

from .sqlparse import SQLAnalyze
from .sqlparse import SQLFormatWithPrefix
from .commandanalyze import execute
from .commandanalyze import CommandNotFound
from .sqlcliexception import SQLCliException
from SQLCliODBC import SQLCliODBCException


class SQLExecute(object):
    conn = None                         # 数据库连接
    logfile = None                      # 打印的日志文件
    sqlscript = None                    # 需要执行的SQL脚本
    SQLMappingHandler = None            # SQL重写处理
    SQLOptions = None                   # 程序处理参数
    Console = False                     # 屏幕输出Console
    logger = None                       # 日志输出

    m_PerfFileLocker = Lock()           # 进程锁, 用来在输出perf文件的时候控制并发写文件

    SQLPerfFile = None                  # SQLPerf文件
    SQLPerfFileHandle = None            # SQLPerf文件句柄

    def __init__(self):
        # 记录最后SQL返回的结果
        self.LastAffectedRows = 0
        self.LastSQLResult = None
        self.LastElapsedTime = 0

        # 当前Executeor的WorkerName
        self.WorkerName = None

        # 程序Spool输出句柄
        self.spoolfile = None

        # 程序Echo输出句柄
        self.echofile = None

        # Scenario信息，如果当前SQL指定，则重复上一个的Scenario信息
        self.SQLScenario = ''

        # Transaction信息
        self.SQLTransaction = ''

    def run(self, statement, p_sqlscript=None):
        """
        返回结果包含5个方面的内容
        (title, rows, headers, columntypes, status).

        支持的SQLHint包括:
        # [Hint]  order           -- SQLCli将会把随后的SQL语句进行排序输出，原程序的输出顺序被忽略
        # [Hint]  scenario:XXXX   -- 相关SQL的场景ID，仅仅作为日志信息供查看
        """
        m_SQL_Status = 0             # SQL 运行结果， 0 成功， 1 失败
        m_SQL_ErrorMessage = ""      # 错误日志信息

        # Remove spaces and EOL
        statement = statement.strip()
        if not statement:  # Empty string
            yield None, None, None, None, None

        # 分析SQL语句
        (ret_bSQLCompleted, ret_SQLSplitResults,
         ret_SQLSplitResultsWithComments, ret_SQLHints) = SQLAnalyze(statement)
        for m_nPos in range(0, len(ret_SQLSplitResults)):
            m_raw_sql = ret_SQLSplitResults[m_nPos]                  # 记录原始SQL

            # 如果当前是在回显一个文件，并且不是echo off，则不再做任何处理
            if self.echofile is not None and \
                    not re.match(r'echo\s+off', m_raw_sql, re.IGNORECASE):
                if self.SQLOptions.get("ECHO").upper() == 'ON' and self.logfile is not None:
                    click.echo(m_raw_sql, file=self.logfile)
                if self.SQLOptions.get("ECHO").upper() == 'ON' and self.spoolfile is not None:
                    click.echo(m_raw_sql, file=self.spoolfile)
                if self.logger is not None:
                    if m_raw_sql is not None:
                        self.logger.info(m_raw_sql)
                click.echo(m_raw_sql, file=self.echofile)
                yield None, None, None, None, m_raw_sql
                continue

            sql = m_raw_sql                                          # 当前要被执行的SQL，这个SQL可能被随后的注释或者替换规则改写
            m_CommentSQL = ret_SQLSplitResultsWithComments[m_nPos]   # 记录带有注释信息的SQL

            # 分析SQLHint信息
            m_SQLHint = ret_SQLHints[m_nPos]                         # SQL提示信息，其中Scenario用作日志处理
            if "SCENARIO" in m_SQLHint.keys():
                if m_SQLHint['SCENARIO'].strip().upper() == "END":
                    self.SQLScenario = ""
                else:
                    self.SQLScenario = m_SQLHint['SCENARIO']

            # 如果打开了回显，并且指定了输出文件，则在输出文件里显示SQL语句
            if self.SQLOptions.get("ECHO").upper() == 'ON' \
                    and len(m_CommentSQL.strip()) != 0 and self.logfile is not None:
                if self.SQLOptions.get("SILENT").upper() == 'OFF':
                    click.echo(SQLFormatWithPrefix(m_CommentSQL), file=self.logfile)
            if self.SQLOptions.get("ECHO").upper() == 'ON' \
                    and len(m_CommentSQL.strip()) != 0 and self.spoolfile is not None:
                # 在spool文件中，不显示spool off的信息，以避免log比对中的不必要内容
                if not re.match(r'spool\s+.*', m_CommentSQL.strip(), re.IGNORECASE):
                    if self.SQLOptions.get("SILENT").upper() == 'OFF':
                        click.echo(SQLFormatWithPrefix(m_CommentSQL), file=self.spoolfile)

            # 在logger中显示执行的SQL
            if self.logger is not None:
                m_LoggerMessage = SQLFormatWithPrefix(m_CommentSQL)
                if m_LoggerMessage is not None:
                    if self.SQLOptions.get("SILENT").upper() == 'OFF':
                        self.logger.info(m_LoggerMessage)

            # 如果运行在脚本模式下，需要在控制台额外回显SQL
            # 非脚本模式下，由于是用户自行输入，所以不需要回显输入的SQL
            if p_sqlscript is not None:
                if self.SQLOptions.get("SILENT").upper() == 'OFF':
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

            # 检查SQL中是否包含特殊内容，如果有，改写SQL
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
            matchObj = re.search(r"%lastsqlresult.LastSQLResult\[(\d+)]\[(\d+)]%",
                                 sql, re.IGNORECASE | re.DOTALL)
            if matchObj:
                m_Searched = matchObj.group(0)
                m_nRow = int(matchObj.group(1))
                m_nColumn = int(matchObj.group(2))
                if self.LastSQLResult is not None:
                    if m_nRow >= len(self.LastSQLResult):
                        m_Result = ""
                    elif m_nColumn >= len(self.LastSQLResult[m_nRow]):
                        m_Result = ""
                    else:
                        m_Result = str(self.LastSQLResult[m_nRow][m_nColumn])
                else:
                    m_Result = ""
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
                matchObj = re.search(r"\${(.*?)}",
                                     sql, re.IGNORECASE | re.DOTALL)
                if matchObj:
                    bMatched = True
                    m_Searched = matchObj.group(0)
                    m_VarName = str(matchObj.group(1)).strip()
                    # 首先判断是否为一个Env函数
                    m_VarValue = '#UNDEFINE_VAR#'
                    if m_VarName.upper().startswith("ENV(") and m_VarName.upper().endswith(")"):
                        m_EnvName = m_VarName[4:-1].strip()
                        if m_EnvName in os.environ:
                            m_VarValue = os.environ[m_EnvName]
                    else:
                        m_VarValue = self.SQLOptions.get(m_VarName)
                        if m_VarValue is None:
                            m_VarValue = self.SQLOptions.get('@' + m_VarName)
                            if m_VarValue is None:
                                m_VarValue = '#UNDEFINE_VAR#'
                    # 替换相应的变量信息
                    sql = sql.replace(m_Searched, m_VarValue)
                    continue
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
                # 首先尝试这是一个特殊命令，如果返回CommandNotFound，则认为其是一个标准SQL
                for (title, result, headers, columntypes, status) in execute(sql):
                    yield title, result, headers, columntypes, status
            except CommandNotFound:
                if cur is None:
                    # 进入到SQL执行阶段，不是特殊命令, 数据库连接也不存在
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

                            # 如果Hints中有order字样，对结果进行排序后再输出
                            if "Order" in m_SQLHint.keys() and result is not None:
                                for i in range(1, len(result)):
                                    for j in range(0, len(result) - i):
                                        if str(result[j]) > str(result[j + 1]):
                                            result[j], result[j + 1] = result[j + 1], result[j]

                            if "SQLCLI_DEBUG" in os.environ:
                                # DEBUG模式下不会过滤任何东西
                                pass
                            else:
                                # 如果Hint中存在LogFilter，则过滤指定的输出信息
                                if "LogFilter" in m_SQLHint.keys() and result is not None:
                                    for item in result[:]:
                                        if re.match(m_SQLHint["LogFilter"], ''.join(str(item)), re.IGNORECASE):
                                            result.remove(item)

                                # 如果Hint中存在LogMask,，则掩码指定的输出信息
                                if "LogMask" in m_SQLHint.keys() and result is not None:
                                    for i in range(0, len(result)):
                                        m_Output = None
                                        for j in range(0, len(result[i])):
                                            if m_Output is None:
                                                m_Output = str(result[i][j])
                                            else:
                                                m_Output = m_Output + "," + str(result[i][j])
                                        m_NewOutput = re.sub(m_SQLHint["LogMask"][0], m_SQLHint["LogMask"][1],
                                                             m_Output, re.IGNORECASE)
                                        if m_NewOutput != m_Output:
                                            result[i] = tuple(m_NewOutput.split(','))

                            # 返回SQL结果
                            if self.SQLOptions.get('TERMOUT').upper() != 'OFF':
                                yield title, result, headers, columntypes, status
                            else:
                                yield title, [], headers, columntypes, status
                            if not m_FetchStatus:
                                break
                    except SQLCliODBCException as oe:
                        m_SQL_Status = 1
                        m_SQL_ErrorMessage = str(oe).strip()
                        for m_ErrorPrefix in ('ERROR:',):
                            if m_SQL_ErrorMessage.startswith(m_ErrorPrefix):
                                m_SQL_ErrorMessage = m_SQL_ErrorMessage[len(m_ErrorPrefix):].strip()
                        # 发生了SQL语法错误
                        if self.SQLOptions.get("WHENEVER_SQLERROR") == "EXIT":
                            raise SQLCliException(m_SQL_ErrorMessage)
                        else:
                            self.LastAffectedRows = 0
                            self.LastSQLResult = None
                            yield None, None, None, None, m_SQL_ErrorMessage
                    except Exception as e:
                        m_SQL_Status = 1
                        m_SQL_ErrorMessage = str(e).strip()
                        for m_ErrorPrefix in ('java.sql.SQLSyntaxErrorException:',
                                              "java.sql.SQLException:",
                                              "java.sql.SQLInvalidAuthorizationSpecException:",
                                              "java.sql.SQLDataException:",
                                              "java.sql.SQLTransactionRollbackException:",
                                              "java.sql.SQLTransientConnectionException:",
                                              "java.sql.SQLFeatureNotSupportedException",
                                              "com.microsoft.sqlserver.jdbc."):
                            if m_SQL_ErrorMessage.startswith(m_ErrorPrefix):
                                m_SQL_ErrorMessage = m_SQL_ErrorMessage[len(m_ErrorPrefix):].strip()
                        # 发生了SQL语法错误
                        if self.SQLOptions.get("WHENEVER_SQLERROR") == "EXIT":
                            raise SQLCliException(m_SQL_ErrorMessage)
                        else:
                            self.LastAffectedRows = 0
                            self.LastSQLResult = None
                            yield None, None, None, None, m_SQL_ErrorMessage
            except (SQLCliException, SQLCliODBCException) as e:
                m_SQL_Status = 1
                m_SQL_ErrorMessage = str(e).strip()
                # 如果要求出错退出，就立刻退出，否则打印日志信息
                if self.SQLOptions.get("WHENEVER_SQLERROR") == "EXIT":
                    raise e
                else:
                    if "SQLCLI_DEBUG" in os.environ:
                        print('traceback.print_exc():\n%s' % traceback.print_exc())
                        print('traceback.format_exc():\n%s' % traceback.format_exc())
                    self.LastAffectedRows = 0
                    self.LastSQLResult = None
                    yield None, None, None, None, m_SQL_ErrorMessage

            # 如果需要，打印语句执行时间
            end = time.time()
            self.LastElapsedTime = end - start

            # 记录SQL日志信息
            if self.SQLOptions.get("SILENT").upper() == 'OFF':
                self.Log_Perf(
                    {
                        "StartedTime": start,
                        "elapsed": self.LastElapsedTime,
                        "RAWSQL": m_raw_sql,
                        "SQL": sql,
                        "SQLStatus": m_SQL_Status,
                        "ErrorMessage": m_SQL_ErrorMessage,
                        "thread_name": self.WorkerName,
                        "Scenario": self.SQLScenario,
                        "Transaction": self.SQLTransaction
                    }
                )

            if self.SQLOptions.get('TIMING').upper() == 'ON':
                if sql.strip().upper() not in ('EXIT', 'QUIT'):
                    yield None, None, None, None, 'Running time elapsed: %9.2f Seconds' % self.LastElapsedTime
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
            rowset = cursor.fetchmany(m_arraysize)
            for row in rowset:
                m_row = []
                # 记录字段类型
                if len(columntypes) == 0:
                    for column in row:
                        if "SQLCLI_DEBUG" in os.environ:
                            print("COLUMN TYPE : [" + str(type(column)) + "]")
                        columntypes.append(type(column))
                for column in row:
                    if str(type(column)).upper().find('JDBCBLOBCLIENT') != -1:
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
                    elif type(column) == decimal.Decimal:
                        if self.SQLOptions.get("DECIMAL_FORMAT") != "":
                            m_row.append(self.SQLOptions.get("DECIMAL_FORMAT") % column)
                        else:
                            m_row.append(column)
                    elif type(column) == bytes:
                        # 对于二进制数据，其末尾用0x00表示，这里进行截断
                        column = column.decode()
                        m_TrimPos = 0
                        for m_nPos in range(len(column), 0, -2):
                            if column[m_nPos - 2] != '0' or column[m_nPos - 1] != '0':
                                m_TrimPos = m_nPos
                                break
                        if m_TrimPos != -1:
                            column = column[0:m_TrimPos]
                        if len(column) != 0:
                            m_row.append("0x" + column)
                        else:
                            m_row.append("")
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
            self.m_PerfFileLocker.release()
