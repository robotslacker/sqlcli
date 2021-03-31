# -*- coding: utf-8 -*-
import decimal

import click
import time
import os
import re
import traceback
import json

from .sqlparse import SQLAnalyze
from .sqlparse import SQLFormatWithPrefix
from .commandanalyze import execute
from .commandanalyze import CommandNotFound
from .sqlcliexception import SQLCliException
from SQLCliODBC import SQLCliODBCException
from .sqlclijdbcapi import SQLCliJDBCException


class SQLExecute(object):
    def __init__(self):
        self.conn = None                    # 数据库连接
        self.sqlscript = None               # 需要执行的SQL脚本
        self.SQLMappingHandler = None       # SQL重写处理
        self.SQLOptions = None              # 程序处理参数
        self.SQLCliHandler = None

        # 记录最后SQL返回的结果
        self.LastJsonSQLResult = None
        self.LastElapsedTime = 0

        # 当前Executeor的WorkerName
        self.WorkerName = None

        # 程序Echo输出句柄
        self.echofile = None

        # Scenario信息，如果当前SQL指定，则重复上一个的Scenario信息
        self.SQLScenario = ''

        # Transaction信息
        self.SQLTransaction = ''

        # 当前SQL游标
        self.cur = None

    def jqparse(self, obj, path='.'):
        class DecimalEncoder(json.JSONEncoder):
            def default(self, o):
                if isinstance(o, decimal.Decimal):
                    return float(o)
                super(DecimalEncoder, self).default(o)

        if self is None:
            pass
        if obj is None:
            if "SQLCLI_DEBUG" in os.environ:
                click.secho("JQ Parse Error: obj is None")
            return "****"
        try:
            obj = json.loads(obj) if isinstance(obj, str) else obj
            find_str, find_map = '', ['["%s"]', '[%s]', '%s', '.%s']
            for im in path.split('.'):
                if not im:
                    continue
                if obj is None:
                    if "SQLCLI_DEBUG" in os.environ:
                        click.secho("JQ Parse Error: obj is none")
                    return "****"
                if isinstance(obj, (list, tuple, str)):
                    if im.startswith('[') and im.endswith(']'):
                        im = im[1:-1]
                    if ':' in im:
                        slice_default = [0, len(obj), 1]
                        obj, quota = obj[slice(
                            *[int(sli) if sli else slice_default[i] for i, sli in
                              enumerate(im.split(':'))])], 1
                    else:
                        obj, quota = obj[int(im)], 1
                else:
                    if im in obj:
                        obj, quota = obj.get(im), 0
                    elif im.endswith('()'):
                        obj, quota = list(getattr(obj, im[:-2])()), 3
                    else:
                        if im.isdigit():
                            obj, quota = obj[int(im)], 1
                        else:
                            raise KeyError(im)
                find_str += find_map[quota] % im
            return obj if isinstance(obj, str) else json.dumps(obj,
                                                               sort_keys=True,
                                                               ensure_ascii=False,
                                                               cls=DecimalEncoder)
        except (IndexError, KeyError, ValueError) as je:
            if "SQLCLI_DEBUG" in os.environ:
                click.secho("JQ Parse Error: " + repr(je))
            return "****"

    def run(self, statement, p_sqlscript=None):
        """
        返回的结果可能是多种类型，处理的时候需要根据type的结果来判断具体的数据格式：
        SQL解析结果：
        {
            "type": "parse",
            "rawsql": "原始SQL语句",
            "formattedsql": "被排版后的SQL语句（包含注释信息）",
            "rewrotedsql": "被改写后的SQL语句（已经被排版过），如果不存在改写，则不存在",
            "script"："SQL当前执行的脚本名称"
        }
        SQL执行结果：
        {
            "type": "result",
            "title": "表头信息",
            "rows": "结果数据集",
            "headers": "结果集的header定义，列名信息",
            "columntypes": "列类型，字符串格式",
            "status": "返回结果汇总消息"
        }
        SQL错误信息：
        {
            "type": "error",
            "message": "错误描述"
        }
        SQL回显信息：
        {
            "type": "error",
            "message": "错误描述",
            "script"："SQL当前执行的脚本名称"
        }

        支持的SQLHint包括:
        -- [Hint]  order           -- SQLCli将会把随后的SQL语句进行排序输出，原程序的输出顺序被忽略
        -- [Hint]  scenario:XXXX   -- 相关SQL的场景ID，仅仅作为日志信息供查看
        -- .....
        """
        m_SQL_Status = 0             # SQL 运行结果， 0 成功， 1 失败
        m_SQL_ErrorMessage = ""      # 错误日志信息

        # Remove spaces and EOL
        statement = statement.strip()
        if not statement:  # Empty string
            return

        # 分析SQL语句
        (ret_bSQLCompleted, ret_SQLSplitResults,
         ret_SQLSplitResultsWithComments, ret_SQLHints) = SQLAnalyze(statement)
        for m_nPos in range(0, len(ret_SQLSplitResults)):
            m_raw_sql = ret_SQLSplitResults[m_nPos]                  # 记录原始SQL

            # 如果当前是在回显一个文件，则不再做任何处理，直接返回
            if self.echofile is not None and \
                    not re.match(r'echo\s+off', m_raw_sql, re.IGNORECASE):
                yield {
                    "type": "echo",
                    "message": m_raw_sql,
                    "script": p_sqlscript
                }
                continue

            sql = m_raw_sql                                          # 当前要被执行的SQL，这个SQL可能被随后的注释或者替换规则改写
            m_CommentSQL = ret_SQLSplitResultsWithComments[m_nPos]   # 记录带有注释信息的SQL
            m_FormattedSQL = None                                    # 已经被格式化了的SQL语句
            m_RewrotedSQL = []                                       # SQL可能会被多次改写

            # 分析SQLHint信息
            m_SQLHint = ret_SQLHints[m_nPos]                         # SQL提示信息，其中Scenario用作日志处理
            if "SCENARIO" in m_SQLHint.keys():
                if m_SQLHint['SCENARIO'].strip().upper() == "END":
                    self.SQLScenario = ""
                else:
                    self.SQLScenario = m_SQLHint['SCENARIO']

            # 记录包含有注释信息的SQL
            m_FormattedSQL = SQLFormatWithPrefix(m_CommentSQL)

            # 如果打开了回显，并且指定了输出文件，且SQL被改写过，输出改写后的SQL
            if self.SQLOptions.get("SQLREWRITE").upper() == 'ON':
                old_sql = sql
                sql = self.SQLMappingHandler.RewriteSQL(p_sqlscript, old_sql)
                if old_sql != sql:    # SQL已经发生了改变
                    # 记录被SQLMapping改写的SQL
                    m_RewrotedSQL.append(SQLFormatWithPrefix("Your SQL has been changed to:\n" + sql, 'REWROTED '))

            # 如果是空语句，不需要执行，但可能是完全注释行
            if len(sql.strip()) == 0:
                # 返回SQL的解析信息
                yield {
                    "type": "parse",
                    "rawsql": m_raw_sql,
                    "formattedsql": m_FormattedSQL,
                    "rewrotedsql": m_RewrotedSQL,
                    "script": p_sqlscript
                }
                continue

            matchObj = re.match(r"^show\s+table\s+storage\s+(.*)$", sql, re.IGNORECASE)
            if matchObj:
                m_TableName = matchObj.group(1)
                m_Location = ""
                for m_Result in self.run("show table properties " + m_TableName):
                    if m_Result["type"] == "result":
                        if len(m_Result["rows"]) != 0:
                            m_Location = m_Result["rows"][len(m_Result["rows"]) - 1][1]
                            break
                if m_Location != "":
                    m_HostCommand = "hdfs fsck " + m_Location + " -files -blocks -locations"
                    m_Result = []
                    m_Status = ""
                    for m_OSResult in self.run("host " + m_HostCommand):
                        m_HostBlockInfo = {}
                        if m_OSResult["type"] == "result":
                            m_StoargeInfo = m_OSResult["status"].splitlines()
                            m_BlockCount = 0
                            for m_line in m_StoargeInfo:
                                matchObj = re.search(r"bytes, (\d+) block\(s\):\s+OK", m_line)
                                if matchObj:
                                    m_BlockCount = int(matchObj.group(1))
                                matchObj = re.search(r"DatanodeInfoWithStorage\[(.*?):", m_line)
                                if matchObj:
                                    m_Host = str(matchObj.group(1)).strip()
                                    if m_Host in m_HostBlockInfo.keys():
                                        m_HostBlockInfo[m_Host] = m_HostBlockInfo[m_Host] + m_BlockCount
                                    else:
                                        m_HostBlockInfo[m_Host] = m_BlockCount
                                matchObj = re.search(r"Status:", m_line)
                                if matchObj:
                                    m_Status = m_Status + m_line + "\n"
                                matchObj = re.search(r"Total size:", m_line)
                                if matchObj:
                                    m_Status = m_Status + m_line + "\n"
                                matchObj = re.search(r"Total dirs:", m_line)
                                if matchObj:
                                    m_Status = m_Status + m_line + "\n"
                                matchObj = re.search(r"Total files:", m_line)
                                if matchObj:
                                    m_Status = m_Status + m_line + "\n"
                        for HostIP, HostBlockCount in m_HostBlockInfo.items():
                            m_Result.append((HostIP, HostBlockCount))
                    yield {
                        "type": "parse",
                        "rawsql": m_raw_sql,
                        "formattedsql": m_FormattedSQL,
                        "rewrotedsql": m_RewrotedSQL,
                        "script": p_sqlscript
                    }
                    yield {
                        "type": "result",
                        "title": "HDFS Storage Information for " + m_TableName,
                        "rows": m_Result,
                        "headers": ["Node", "Block(s)"],
                        "columntypes": ["str", "int"],
                        "status": m_Status
                    }
                return

            # 检查SQL中是否包含特殊内容，如果有，改写SQL
            # 特殊内容都有：
            # 1. ${LastSQLResult(.*)}       # .* JQ Parse Pattern
            # 2. ${var}
            #    用户定义的变量

            matchObj = re.search(r"\${LastSQLResult\((.*)\)}",
                                 sql, re.IGNORECASE | re.DOTALL)
            if matchObj:
                m_Searched = matchObj.group(0)
                m_JQPattern = matchObj.group(1)
                sql = sql.replace(m_Searched, self.jqparse(obj=self.LastJsonSQLResult, path=m_JQPattern))
                if self.SQLOptions.get("SILENT").upper() == 'ON':
                    # SILENT模式下不打印任何日志
                    pass
                else:
                    # 记录被JQ表达式改写的SQL
                    m_RewrotedSQL.append(SQLFormatWithPrefix("Your SQL has been changed to:\n" + sql, 'REWROTED '))

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
                # 记录被变量信息改写的SQL
                m_RewrotedSQL.append(SQLFormatWithPrefix("Your SQL has been changed to:\n" + sql, 'REWROTED '))

            # 返回SQL的解析信息
            yield {
                "type": "parse",
                "rawsql": m_raw_sql,
                "formattedsql": m_FormattedSQL,
                "rewrotedsql": m_RewrotedSQL,
                "script": p_sqlscript
            }

            # 记录命令开始时间
            start = time.time()

            # 执行SQL
            try:
                # 首先尝试这是一个特殊命令，如果返回CommandNotFound，则认为其是一个标准SQL
                for m_Result in execute(self.SQLCliHandler,  sql):
                    if "type" not in m_Result.keys():
                        m_Result.update({"type": "result"})
                    yield m_Result
            except CommandNotFound:
                # 进入到SQL执行阶段, 开始执行SQL语句
                if self.conn:
                    # 打开游标
                    self.cur = self.conn.cursor()
                else:
                    # 进入到SQL执行阶段，不是特殊命令, 数据库连接也不存在
                    if self.SQLOptions.get("WHENEVER_SQLERROR") == "EXIT":
                        raise SQLCliException("Not Connected. ")
                    else:
                        self.LastJsonSQLResult = None
                        yield {
                            "type": "error",
                            "message": "Not connected. "
                        }
                        return

                # 执行正常的SQL语句
                if self.cur is not None:
                    try:
                        if "SQLCLI_DEBUG" in os.environ:
                            print("DEBUG-SQL=[" + str(sql) + "]")
                        # 执行SQL脚本
                        if "SQL_DIRECT" in m_SQLHint.keys():
                            self.cur.execute_direct(sql)
                        elif "SQL_PREPARE" in m_SQLHint.keys():
                            self.cur.execute(sql)
                        else:
                            if self.SQLOptions.get("SQL_EXECUTE") == "DIRECT":
                                self.cur.execute_direct(sql)
                            else:
                                self.cur.execute(sql)
                        rowcount = 0
                        while True:
                            (title, result, headers, columntypes, status, m_FetchStatus, m_FetchedRows) = \
                                self.get_result(self.cur, rowcount)
                            rowcount = m_FetchedRows
                            if "SQLCLI_DEBUG" in os.environ:
                                if result is not None:
                                    for m_RowPos in range(0, len(result)):
                                        for m_CellPos in range(0, len(result[m_RowPos])):
                                            print("Cell[" + str(m_RowPos) + ":" +
                                                  str(m_CellPos) + "]=[" + str(result[m_RowPos][m_CellPos]) + "]")
                            self.LastJsonSQLResult = {"desc": headers,
                                                      "rows": rowcount,
                                                      "elapsed": time.time() - start,
                                                      "result": result}

                            # 如果Hints中有order字样，对结果进行排序后再输出
                            if "Order" in m_SQLHint.keys() and result is not None:
                                for i in range(1, len(result)):
                                    for j in range(0, len(result) - i):
                                        if str(result[j]) > str(result[j + 1]):
                                            result[j], result[j + 1] = result[j + 1], result[j]

                            # 如果Hint中存在LogFilter，则结果集中过滤指定的输出信息
                            if "LogFilter" in m_SQLHint.keys() and result is not None:
                                for m_SQLFilter in m_SQLHint["LogFilter"]:
                                    for item in result[:]:
                                        if "SQLCLI_DEBUG" in os.environ:
                                            print("Apply Filter: " + str(''.join(str(item))) + " with " + m_SQLFilter)
                                        if re.match(m_SQLFilter, ''.join(str(item)), re.IGNORECASE):
                                            result.remove(item)
                                            continue

                            # 如果Hint中存在LogMask,则掩码指定的输出信息
                            if "LogMask" in m_SQLHint.keys() and result is not None:
                                for i in range(0, len(result)):
                                    m_Output = None
                                    for j in range(0, len(result[i])):
                                        if m_Output is None:
                                            m_Output = str(result[i][j])
                                        else:
                                            m_Output = m_Output + "|||" + str(result[i][j])
                                    for m_SQLMaskString in m_SQLHint["LogMask"]:
                                        m_SQLMask = m_SQLMaskString.split("=>")
                                        if len(m_SQLMask) == 2:
                                            m_SQLMaskPattern = m_SQLMask[0]
                                            m_SQLMaskTarget = m_SQLMask[1]
                                            if "SQLCLI_DEBUG" in os.environ:
                                                print("Apply Mask: " + m_Output +
                                                      " with " + m_SQLMaskPattern + "=>" + m_SQLMaskTarget)
                                            m_NewOutput = re.sub(m_SQLMaskPattern, m_SQLMaskTarget, m_Output,
                                                                 re.IGNORECASE)
                                            if m_NewOutput != m_Output:
                                                result[i] = tuple(m_NewOutput.split('|||'))
                                        else:
                                            if "SQLCLI_DEBUG" in os.environ:
                                                print("LogMask Hint Error: " + m_SQLHint["LogMask"])

                            # 如果存在SQL_LOOP信息，则需要反复执行上一个SQL
                            if "SQL_LOOP" in m_SQLHint.keys():
                                if "SQLCLI_DEBUG" in os.environ:
                                    print("SQL_LOOP=" + str(m_SQLHint["SQL_LOOP"]))
                                # 循环执行SQL列表，构造参数列表
                                m_LoopTimes = int(m_SQLHint["SQL_LOOP"]["LoopTimes"])
                                m_LoopInterval = int(m_SQLHint["SQL_LOOP"]["LoopInterval"])
                                m_LoopUntil = m_SQLHint["SQL_LOOP"]["LoopUntil"]
                                if m_LoopInterval <= 0:
                                    m_LoopTimes = 0
                                    if "SQLCLI_DEBUG" in os.environ:
                                        raise SQLCliException(
                                            "SQLLoop Hint Error, Unexpected LoopTime: " + str(m_LoopInterval))
                                if m_LoopInterval <= 0:
                                    m_LoopTimes = 0
                                    if "SQLCLI_DEBUG" in os.environ:
                                        raise SQLCliException(
                                            "SQLLoop Hint Error, Unexpected LoopInterval: " + str(m_LoopInterval))

                                # 保存Silent设置
                                m_OldSilentMode = self.SQLOptions.get("SILENT")
                                m_OldTimingMode = self.SQLOptions.get("TIMING")
                                m_OldTimeMode = self.SQLOptions.get("TIME")
                                self.SQLOptions.set("SILENT", "ON")
                                self.SQLOptions.set("TIMING", "OFF")
                                self.SQLOptions.set("TIME", "OFF")
                                m_LoopFinished = False
                                for m_nLoopPos in range(0, m_LoopTimes):
                                    # 检查Until条件，如果达到Until条件，退出
                                    for _, _, _, _, assert_status in \
                                            self.run("__internal__ test assert " + m_LoopUntil):
                                        if assert_status.startswith("Assert Successful"):
                                            m_LoopFinished = True
                                            break
                                        else:
                                            # 测试失败, 等待一段时间后，开始下一次检查
                                            time.sleep(m_LoopInterval)
                                            for title, result, headers, columntypes, status in self.run(sql):
                                                # 最后一次执行的结果将被传递到外层，作为SQL返回结果
                                                pass
                                    if m_LoopFinished:
                                        break
                                self.SQLOptions.set("TIME", m_OldTimeMode)
                                self.SQLOptions.set("TIMING", m_OldTimingMode)
                                self.SQLOptions.set("SILENT", m_OldSilentMode)

                            # 返回SQL结果
                            if self.SQLOptions.get('TERMOUT').upper() != 'OFF':
                                yield {
                                    "type": "result",
                                    "title": title,
                                    "rows": result,
                                    "headers": headers,
                                    "columntypes": columntypes,
                                    "status": status
                                }
                            else:
                                yield {
                                    "type": "result",
                                    "title": title,
                                    "rows": [],
                                    "headers": headers,
                                    "columntypes": columntypes,
                                    "status": status
                                }
                            if not m_FetchStatus:
                                break
                    except SQLCliODBCException as oe:
                        m_SQL_Status = 1
                        m_SQL_ErrorMessage = str(oe).strip()
                        for m_ErrorPrefix in ('ERROR:',):
                            if m_SQL_ErrorMessage.startswith(m_ErrorPrefix):
                                m_SQL_ErrorMessage = m_SQL_ErrorMessage[len(m_ErrorPrefix):].strip()
                        # 发生了ODBC的SQL语法错误
                        if self.SQLOptions.get("WHENEVER_SQLERROR") == "EXIT":
                            raise SQLCliException(m_SQL_ErrorMessage)
                        else:
                            yield {"type": "error", "message": m_SQL_ErrorMessage}
                    except SQLCliJDBCException as je:
                        m_SQL_Status = 1
                        m_SQL_ErrorMessage = str(je).strip()
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

                        # 如果Hint中存在LogFilter，则输出的消息中过滤指定的输出信息
                        if "LogFilter" in m_SQLHint.keys():
                            m_SQL_MultiLineErrorMessage = m_SQL_ErrorMessage.split('\n')
                            m_ErrorMessageHasChanged = False
                            for m_SQLFilter in m_SQLHint["LogFilter"]:
                                for item in m_SQL_MultiLineErrorMessage[:]:
                                    if re.match(m_SQLFilter, ''.join(str(item)), re.IGNORECASE):
                                        m_SQL_MultiLineErrorMessage.remove(item)
                                        m_ErrorMessageHasChanged = True
                                        continue
                            if m_ErrorMessageHasChanged:
                                m_SQL_ErrorMessage = "\n".join(m_SQL_MultiLineErrorMessage)

                        # 如果Hint中存在LogMask,则掩码指定的输出信息
                        if "LogMask" in m_SQLHint.keys():
                            m_SQL_MultiLineErrorMessage = m_SQL_ErrorMessage.split('\n')
                            m_ErrorMessageHasChanged = False
                            for m_SQLMaskString in m_SQLHint["LogMask"]:
                                m_SQLMask = m_SQLMaskString.split("=>")
                                if len(m_SQLMask) == 2:
                                    m_SQLMaskPattern = m_SQLMask[0]
                                    m_SQLMaskTarget = m_SQLMask[1]
                                    for m_nPos2 in range(0, len(m_SQL_MultiLineErrorMessage)):
                                        m_NewOutput = re.sub(m_SQLMaskPattern, m_SQLMaskTarget,
                                                             m_SQL_MultiLineErrorMessage[m_nPos2],
                                                             re.IGNORECASE)
                                        if m_NewOutput != m_SQL_MultiLineErrorMessage[m_nPos2]:
                                            m_ErrorMessageHasChanged = True
                                            m_SQL_MultiLineErrorMessage[m_nPos2] = m_NewOutput
                                else:
                                    if "SQLCLI_DEBUG" in os.environ:
                                        raise SQLCliException("LogMask Hint Error: " + m_SQLHint["LogMask"])
                            if m_ErrorMessageHasChanged:
                                m_SQL_ErrorMessage = "\n".join(m_SQL_MultiLineErrorMessage)

                        # 发生了JDBC的SQL语法错误
                        if self.SQLOptions.get("WHENEVER_SQLERROR") == "EXIT":
                            raise SQLCliException(m_SQL_ErrorMessage)
                        else:
                            yield {"type": "error", "message": m_SQL_ErrorMessage}
                    except Exception as e:
                        m_SQL_Status = 1
                        m_SQL_ErrorMessage = str(e).strip()

                        # 发生了其他SQL语法错误
                        if self.SQLOptions.get("WHENEVER_SQLERROR") == "EXIT":
                            raise SQLCliException(m_SQL_ErrorMessage)
                        else:
                            yield {"type": "error", "message": m_SQL_ErrorMessage}
            except EOFError:
                # EOFError是程序退出的标志，退出应用程序
                raise EOFError
            except Exception as e:
                # 只有在WHENEVER_SQLERROR==EXIT的时候，才会由前面的错误代码抛出错误到这里
                m_SQL_Status = 1
                m_SQL_ErrorMessage = str(e).strip()
                # 如果要求出错退出，就立刻退出，否则打印日志信息
                if self.SQLOptions.get("WHENEVER_SQLERROR") == "EXIT":
                    raise e
                else:
                    if "SQLCLI_DEBUG" in os.environ:
                        print('traceback.print_exc():\n%s' % traceback.print_exc())
                        print('traceback.format_exc():\n%s' % traceback.format_exc())
                    self.LastJsonSQLResult = None
                    yield {"type": "error", "message": m_SQL_ErrorMessage}

            # 如果需要，打印语句执行时间
            end = time.time()
            self.LastElapsedTime = end - start

            # 记录SQL日志信息
            if self.SQLOptions.get("SILENT").upper() == 'OFF':
                yield {
                    "type": "statistics",
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
                            print("DEBUG: COLUMN TYPE TYPE: [" + str(type(column)) + "]")
                        m_ColumnType = ""
                        if type(column) == int:
                            m_ColumnType = "int"
                        elif type(column) == str:
                            m_ColumnType = "str"
                        elif type(column) == bool:
                            m_ColumnType = "bool"
                        elif type(column) == float:
                            m_ColumnType = "float"
                        else:
                            m_ColumnType = str(type(column))
                        columntypes.append(m_ColumnType)
                        if "SQLCLI_DEBUG" in os.environ:
                            print("DEBUG: COLUMN TYPE STR : [" + m_ColumnType + "]")
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
