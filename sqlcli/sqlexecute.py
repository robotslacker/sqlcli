# -*- coding: utf-8 -*-
import datetime
import decimal

import click
import time
import os
import re
import platform
import random
import traceback
import json
import binascii

from .sqlparse import SQLAnalyze
from .sqlparse import SQLFormatWithPrefix
from .commandanalyze import execute
from .commandanalyze import CommandNotFound
from .sqlcliexception import SQLCliException
from .sqlclijdbcapi import SQLCliJDBCException
from .sqlclijdbcapi import SQLCliJDBCTimeOutException
from .sqlclijdbcapi import SQLCliJDBCLargeObject
from .sqlcliodbc import SQLCliODBCException


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
        self.SQLTransactionName = ""

        # 当前SQL游标
        self.cur = None

        # 脚本启动的时间
        self.StartTime = time.time()

    def setStartTime(self, p_StartTime):
        self.StartTime = p_StartTime

    def getStartTime(self):
        return self.StartTime

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

            # ${random(1,100)}
            # 处理脚本中的随机数问题
            matchObj = re.search(r"\${random_int\((\s+)?(\d+)(\s+)?,(\s+)?(\d+)(\s+)?\)}",
                                 sql, re.IGNORECASE | re.DOTALL)
            if matchObj:
                m_Searched = matchObj.group(0)
                m_random_start = int(matchObj.group(2))
                m_random_end = int(matchObj.group(5))
                sql = sql.replace(m_Searched, str(random.randint(m_random_start, m_random_end)))

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
            try:
                m_SQLTimeOut = int(self.SQLOptions.get("SQL_TIMEOUT"))
            except ValueError:
                m_SQLTimeOut = -1
                if "SQLCLI_DEBUG" in os.environ:
                    yield {"type": "error", "message": "SQLCLI-0000: Not a valid SQLTimeOut Setting, Ignore this."}
            try:
                m_ScriptTimeOut = int(self.SQLOptions.get("SCRIPT_TIMEOUT"))
            except ValueError:
                m_ScriptTimeOut = -1
                if "SQLCLI_DEBUG" in os.environ:
                    yield {"type": "error", "message": "SQLCLI-0000: Not a valid ScriptTimeOut Setting, Ignore this."}

            # 执行SQL
            try:
                # 处理超时时间问题
                if m_ScriptTimeOut > 0:
                    if m_ScriptTimeOut <= time.time() - self.getStartTime():
                        if sql.upper() not in ["EXIT", "QUIT"]:
                            m_SQL_ErrorMessage = "SQLCLI-0000: Script Timeout " \
                                                 "(" + str(round(time.time() - self.getStartTime(), 2)) + \
                                                 ") expired. Abort this Script."
                            yield {"type": "error", "message": m_SQL_ErrorMessage}
                        raise EOFError
                    else:
                        if m_SQLTimeOut <= 0 or (m_ScriptTimeOut - (time.time() - self.getStartTime()) < m_SQLTimeOut):
                            # 脚本时间已经不够，剩下的SQL时间变少
                            m_SQLTimeOut = m_ScriptTimeOut - (time.time() - self.getStartTime())
                # 首先尝试这是一个特殊命令，如果返回CommandNotFound，则认为其是一个标准SQL
                for m_Result in execute(self.SQLCliHandler,  sql, p_nTimeout=m_SQLTimeOut):
                    # 保存之前的运行结果
                    if "type" not in m_Result.keys():
                        m_Result.update({"type": "result"})
                    if m_Result["type"] == "result" and sql.startswith("__internal__"):
                        result = m_Result["rows"]
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
                                        try:
                                            m_NewOutput = re.sub(m_SQLMaskPattern, m_SQLMaskTarget, m_Output,
                                                                 re.IGNORECASE)
                                            if m_NewOutput != m_Output:
                                                result[i] = tuple(m_NewOutput.split('|||'))
                                                m_Output = m_NewOutput
                                        except re.error:
                                            if "SQLCLI_DEBUG" in os.environ:
                                                print('traceback.print_exc():\n%s' % traceback.print_exc())
                                                print('traceback.format_exc():\n%s' % traceback.format_exc())
                                    else:
                                        if "SQLCLI_DEBUG" in os.environ:
                                            print("LogMask Hint Error: " + m_SQLHint["LogMask"])
                        m_Result["rows"] = result
                        if m_Result["rows"] is None:
                            m_Rows = 0
                        else:
                            m_Rows = len(m_Result["rows"])
                        self.LastJsonSQLResult = {"desc": m_Result["headers"],
                                                  "rows": m_Rows,
                                                  "elapsed": time.time() - start,
                                                  "result": m_Result["rows"],
                                                  "status": 0,
                                                  "warnings": ""}
                        # 如果TERMOUT为关闭，不在返回结果信息
                        if self.SQLOptions.get('TERMOUT').upper() == 'OFF':
                            m_Result["rows"] = []
                            m_Result["title"] = ""
                    yield m_Result
            except SQLCliJDBCTimeOutException:
                # 处理超时时间问题
                if m_ScriptTimeOut > 0:
                    if m_ScriptTimeOut <= time.time() - self.getStartTime():
                        if sql.upper() not in ["EXIT", "QUIT"]:
                            m_SQL_ErrorMessage = "SQLCLI-0000: Script Timeout " \
                                                 "(" + str(round(time.time() - self.getStartTime(), 2)) + \
                                                 ") expired. Abort this Script."
                            yield {"type": "error", "message": m_SQL_ErrorMessage}
                        raise EOFError
                m_SQL_ErrorMessage = "SQLCLI-0000: SQL Timeout (" + str(round(time.time() - start, 2)) + \
                                     ") expired. Abort this command."
                yield {"type": "error", "message": m_SQL_ErrorMessage}
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

                # 执行正常的SQL语句
                if self.cur is not None:
                    try:
                        if "SQLCLI_DEBUG" in os.environ:
                            print("DEBUG-SQL=[" + str(sql) + "]")
                        # 处理超时时间问题
                        if m_ScriptTimeOut > 0:
                            if m_ScriptTimeOut <= time.time() - self.getStartTime():
                                if sql.upper() not in ["EXIT", "QUIT"]:
                                    m_SQL_ErrorMessage = "SQLCLI-0000: Script Timeout (" + \
                                                         str(round(time.time() - self.getStartTime(), 2)) + \
                                                         ") expired. Abort this Script."
                                    yield {"type": "error", "message": m_SQL_ErrorMessage}
                                raise EOFError
                            else:
                                if m_SQLTimeOut <= 0 or \
                                        (m_ScriptTimeOut - (time.time() - self.getStartTime()) < m_SQLTimeOut):
                                    # 脚本时间已经不够，剩下的SQL时间变少
                                    m_SQLTimeOut = m_ScriptTimeOut - (time.time() - self.getStartTime())

                        try:
                            # 执行数据库的SQL语句
                            if "SQL_DIRECT" in m_SQLHint.keys():
                                self.cur.execute_direct(sql, TimeOutLimit=m_SQLTimeOut)
                            elif "SQL_PREPARE" in m_SQLHint.keys():
                                self.cur.execute(sql, TimeOutLimit=m_SQLTimeOut)
                            else:
                                if self.SQLOptions.get("SQL_EXECUTE").upper() == "DIRECT":
                                    self.cur.execute_direct(sql, TimeOutLimit=m_SQLTimeOut)
                                else:
                                    self.cur.execute(sql, TimeOutLimit=m_SQLTimeOut)
                        except Exception as e:
                            if "SQL_LOOP" in m_SQLHint.keys():
                                # 如果在循环中, 错误不会处理， 一直到循环结束
                                pass
                            else:
                                raise e

                        rowcount = 0
                        m_SQL_Status = 0
                        while True:
                            (title, result, headers, columntypes, status,
                             m_FetchStatus, m_FetchedRows, m_SQLWarnings) = \
                                self.get_result(self.cur, rowcount)
                            rowcount = m_FetchedRows
                            if "SQLCLI_DEBUG" in os.environ:
                                print("!!DEBUG!!  headers=" + str(headers))
                                if result is not None:
                                    for m_RowPos in range(0, len(result)):
                                        for m_CellPos in range(0, len(result[m_RowPos])):
                                            print("Cell[" + str(m_RowPos) + ":" +
                                                  str(m_CellPos) + "]=[" + str(result[m_RowPos][m_CellPos]) + "]")

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
                                            try:
                                                m_NewOutput = re.sub(m_SQLMaskPattern, m_SQLMaskTarget, m_Output,
                                                                     re.IGNORECASE)
                                                if m_NewOutput != m_Output:
                                                    result[i] = tuple(m_NewOutput.split('|||'))
                                                    m_Output = m_NewOutput
                                            except re.error:
                                                if "SQLCLI_DEBUG" in os.environ:
                                                    print('traceback.print_exc():\n%s' % traceback.print_exc())
                                                    print('traceback.format_exc():\n%s' % traceback.format_exc())
                                        else:
                                            if "SQLCLI_DEBUG" in os.environ:
                                                print("LogMask Hint Error: " + m_SQLHint["LogMask"])

                            # 保存之前的运行结果
                            if result is None:
                                m_Rows = 0
                            else:
                                m_Rows = len(result)
                            self.LastJsonSQLResult = {"desc": headers,
                                                      "rows": m_Rows,
                                                      "elapsed": time.time() - start,
                                                      "result": result,
                                                      "status": 0,
                                                      "warnings": m_SQLWarnings}

                            # 如果存在SQL_LOOP信息，则需要反复执行上一个SQL
                            if "SQL_LOOP" in m_SQLHint.keys():
                                if "SQLCLI_DEBUG" in os.environ:
                                    print("SQL_LOOP=" + str(m_SQLHint["SQL_LOOP"]))
                                # 循环执行SQL列表，构造参数列表
                                m_LoopTimes = int(m_SQLHint["SQL_LOOP"]["LoopTimes"])
                                m_LoopInterval = int(m_SQLHint["SQL_LOOP"]["LoopInterval"])
                                m_LoopUntil = m_SQLHint["SQL_LOOP"]["LoopUntil"]
                                if m_LoopInterval < 0:
                                    m_LoopTimes = 0
                                    if "SQLCLI_DEBUG" in os.environ:
                                        raise SQLCliException(
                                            "SQLLoop Hint Error, Unexpected LoopInterval: " + str(m_LoopInterval))
                                if m_LoopTimes < 0:
                                    if "SQLCLI_DEBUG" in os.environ:
                                        raise SQLCliException(
                                            "SQLLoop Hint Error, Unexpected LoopTime: " + str(m_LoopTimes))

                                # 保存Silent设置
                                m_OldSilentMode = self.SQLOptions.get("SILENT")
                                m_OldTimingMode = self.SQLOptions.get("TIMING")
                                m_OldTimeMode = self.SQLOptions.get("TIME")
                                self.SQLOptions.set("SILENT", "ON")
                                self.SQLOptions.set("TIMING", "OFF")
                                self.SQLOptions.set("TIME", "OFF")
                                for m_nLoopPos in range(1, m_LoopTimes):
                                    # 检查Until条件，如果达到Until条件，退出
                                    m_AssertSuccessful = False
                                    for m_Result in \
                                            self.run("__internal__ test assert " + m_LoopUntil):
                                        if m_Result["type"] == "result":
                                            if m_Result["status"].startswith("Assert Successful"):
                                                m_AssertSuccessful = True
                                            break
                                    if m_AssertSuccessful:
                                        break
                                    else:
                                        # 测试失败, 等待一段时间后，开始下一次检查
                                        time.sleep(m_LoopInterval)
                                        for m_Result in self.run(sql):
                                            # 最后一次执行的结果将被传递到外层，作为SQL返回结果
                                            if m_Result["type"] == "result":
                                                m_SQL_Status = 0
                                                title = m_Result["title"]
                                                result = m_Result["rows"]
                                                headers = m_Result["headers"]
                                                columntypes = m_Result["columntypes"]
                                                status = m_Result["status"]
                                            if m_Result["type"] == "error":
                                                m_SQL_Status = 1
                                                m_SQL_ErrorMessage = m_Result["message"]
                                self.SQLOptions.set("TIME", m_OldTimeMode)
                                self.SQLOptions.set("TIMING", m_OldTimingMode)
                                self.SQLOptions.set("SILENT", m_OldSilentMode)

                            # 返回SQL结果
                            if m_SQL_Status == 1:
                                yield {"type": "error", "message": m_SQL_ErrorMessage}
                            else:
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
                    except SQLCliJDBCTimeOutException:
                        # 处理超时时间问题
                        if m_ScriptTimeOut > 0:
                            if m_ScriptTimeOut <= time.time() - self.getStartTime():
                                if sql.upper() not in ["EXIT", "QUIT"]:
                                    m_SQL_ErrorMessage = "SQLCLI-0000: Script Timeout (" + \
                                                         str(round(time.time() - self.getStartTime(), 2)) + \
                                                         ") expired. Abort this script."
                                    yield {"type": "error", "message": m_SQL_ErrorMessage}
                                raise EOFError
                        m_SQL_ErrorMessage = "SQLCLI-0000: SQL Timeout (" + \
                                             str(round(time.time() - start, 2)) + \
                                             ") expired. Abort this command."
                        yield {"type": "error", "message": m_SQL_ErrorMessage}
                    except SQLCliODBCException as oe:
                        m_SQL_Status = 1
                        m_SQL_ErrorMessage = str(oe).strip()
                        for m_ErrorPrefix in ('ERROR:',):
                            if m_SQL_ErrorMessage.startswith(m_ErrorPrefix):
                                m_SQL_ErrorMessage = m_SQL_ErrorMessage[len(m_ErrorPrefix):].strip()

                        self.LastJsonSQLResult = {"elapsed": time.time() - start,
                                                  "message": m_SQL_ErrorMessage,
                                                  "status": -1,
                                                  "rows": 0}

                        # 发生了ODBC的SQL语法错误
                        if self.SQLOptions.get("WHENEVER_SQLERROR") == "EXIT":
                            raise SQLCliException(m_SQL_ErrorMessage)
                        else:
                            yield {"type": "error", "message": m_SQL_ErrorMessage}
                    except (SQLCliJDBCException, Exception) as je:
                        m_SQL_Status = 1
                        m_SQL_ErrorMessage = str(je).strip()
                        for m_ErrorPrefix in ["java.util.concurrent.ExecutionException:", ]:
                            if m_SQL_ErrorMessage.startswith(m_ErrorPrefix):
                                m_SQL_ErrorMessage = m_SQL_ErrorMessage[len(m_ErrorPrefix):].strip()
                        for m_ErrorPrefix in ['java.sql.SQLSyntaxErrorException:',
                                              "java.sql.SQLException:",
                                              "java.sql.SQLInvalidAuthorizationSpecException:",
                                              "java.sql.SQLDataException:",
                                              "java.sql.SQLTransactionRollbackException:",
                                              "java.sql.SQLTransientConnectionException:",
                                              "java.sql.SQLFeatureNotSupportedException",
                                              "com.microsoft.sqlserver.jdbc.", ]:
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

                        self.LastJsonSQLResult = {"elapsed": time.time() - start,
                                                  "message": m_SQL_ErrorMessage,
                                                  "status": -1,
                                                  "rows": 0}

                        # 发生了JDBC的SQL语法错误
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
                        print("traceback.pid():" + str(os.getpid()))
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
                    "Transaction": self.SQLTransactionName
                }

    def get_result(self, cursor, rowcount):
        """
            返回的内容：
                title           输出的前提示信息
                result          结果数据集
                headers         表头信息
                columntypes     结果字段类型
                status          输出的后提示信息
                FetchStatus     是否输出完成
                rowcount        共返回记录行数
                Warning         警告信息
        """
        title = headers = None
        m_FetchStatus = True

        def format_column(p_column, p_columntype):
            if type(p_column) == float:
                return self.SQLOptions.get("FLOAT_FORMAT") % p_column
            elif type(p_column) in (bool, str, int):
                return p_column
            elif type(p_column) == datetime.date:
                m_ColumnFormat = self.SQLOptions.get("DATE_FORMAT")
                if platform.system().lower() == 'windows':
                    m_ColumnFormat = m_ColumnFormat.replace("%04Y", "%Y")
                else:
                    m_ColumnFormat = m_ColumnFormat.replace("%Y", "%04Y")
                return p_column.strftime(m_ColumnFormat)
            elif type(p_column) == datetime.datetime:
                if p_columntype in ["TIMESTAMP WITH TIME ZONE",
                                    "TIMESTAMP WITH LOCAL TIME ZONE"]:
                    m_ColumnFormat = self.SQLOptions.get("DATETIME-TZ_FORMAT")
                else:
                    m_ColumnFormat = self.SQLOptions.get("DATETIME_FORMAT")
                if platform.system().lower() == 'windows':
                    m_ColumnFormat = m_ColumnFormat.replace("%04Y", "%Y")
                else:
                    m_ColumnFormat = m_ColumnFormat.replace("%Y", "%04Y")
                p_column.strftime(m_ColumnFormat)
                return p_column.strftime(m_ColumnFormat)
            elif type(p_column) == datetime.time:
                return p_column.strftime(self.SQLOptions.get("TIME_FORMAT"))
            elif type(p_column) == bytearray:
                if p_columntype == "BLOB":
                    m_ColumnTrimLength = int(self.SQLOptions.get("LOB_LENGTH"))
                    m_ColumnisCompleteOutput = True
                    if len(p_column) > m_ColumnTrimLength:
                        m_ColumnisCompleteOutput = False
                        p_column = p_column[:m_ColumnTrimLength]
                    # 转换为16进制，并反算成ASCII
                    p_column = binascii.b2a_hex(p_column)
                    p_column = p_column.decode()
                    if not m_ColumnisCompleteOutput:
                        # 用...的方式提醒输出没有结束，只是由于格式控制导致不显示
                        return "0x" + p_column + "..."
                    else:
                        return "0x" + p_column
                else:
                    # 转换为16进制，并反算成ASCII
                    p_column = binascii.b2a_hex(p_column)
                    p_column = p_column.decode()
                    return "0x" + p_column
            elif type(p_column) == decimal.Decimal:
                if self.SQLOptions.get("DECIMAL_FORMAT") != "":
                    return self.SQLOptions.get("DECIMAL_FORMAT") % p_column
                else:
                    return p_column
            elif type(p_column) == SQLCliJDBCLargeObject:
                m_TrimLength = int(self.SQLOptions.get("LOB_LENGTH"))
                if m_TrimLength < 4:
                    m_TrimLength = 4
                if m_TrimLength > p_column.getObjectLength():
                    if p_column.getColumnTypeName().upper().find("CLOB") != -1:
                        m_DataValue = p_column.getData(1, p_column.getObjectLength())
                        return m_DataValue
                    elif p_column.getColumnTypeName().upper().find("BLOB") != -1:
                        m_DataValue = p_column.getData(1, p_column.getObjectLength())
                        m_DataValue = binascii.b2a_hex(m_DataValue)
                        m_DataValue = m_DataValue.decode()
                        return "0x" + m_DataValue
                else:
                    if p_column.getColumnTypeName().upper().find("CLOB") != -1:
                        m_DataValue = "Len:" + str(p_column.getObjectLength()) + ";" + \
                                      "Content:[" + \
                                      p_column.getData(1, m_TrimLength - 3) + "..." + \
                                      p_column.getData(p_column.getObjectLength() - 2, 3) + \
                                      "]"
                        return m_DataValue
                    elif p_column.getColumnTypeName().upper().find("BLOB") != -1:
                        m_DataValue = "Len:" + str(p_column.getObjectLength()) + ";" + \
                                      "Content:0x[" + \
                                      binascii.b2a_hex(p_column.getData(1, m_TrimLength - 3)).decode() + "..." + \
                                      binascii.b2a_hex(p_column.getData(p_column.getObjectLength() - 2, 3)).decode() + \
                                      "]"
                        return m_DataValue
            elif isinstance(p_column, type(None)):
                return p_column
            else:
                # 其他类型直接返回
                raise SQLCliJDBCException("SQLCLI-00000: Unknown column type [" +
                                          str(p_columntype) + ":" + str(type(p_column)) +
                                          "] in format_column")

        # cursor.description is not None for queries that return result sets,
        # e.g. SELECT.
        result = []
        columntypes = []
        if cursor.description is not None:
            headers = [x[0] for x in cursor.description]
            columntypes = [x[1] for x in cursor.description]
            if cursor.warnings is not None:
                status = "{0} row{1} selected with warnings."
            else:
                status = "{0} row{1} selected."
            m_arraysize = int(self.SQLOptions.get("ARRAYSIZE"))
            rowset = cursor.fetchmany(m_arraysize)
            for row in rowset:
                m_row = []
                for m_nColumnPos in range(0, len(row)):
                    column = row[m_nColumnPos]
                    columntype = columntypes[m_nColumnPos]
                    # 对于空值直接返回
                    if column is None:
                        m_row.append(None)
                        continue

                    # 处理各种数据类型
                    if columntypes[m_nColumnPos] == "STRUCT":
                        m_ColumnValue = "STRUCTURE("
                        for m_nPos in range(0, len(column)):
                            m_ColumnType = str(type(column[m_nPos]))
                            if m_nPos == 0:
                                if type(column[m_nPos]) == str:
                                    m_ColumnValue = m_ColumnValue + "'" + str(column[m_nPos]) + "'"
                                elif type(column[m_nPos]) == datetime.date:
                                    m_ColumnValue = m_ColumnValue + "DATE '" + \
                                                    format_column(column[m_nPos], m_ColumnType) + "'"
                                elif type(column[m_nPos]) == datetime.datetime:
                                    m_ColumnValue = m_ColumnValue + "TIMESTAMP '" + \
                                                    format_column(column[m_nPos], m_ColumnType) + "'"
                                else:
                                    m_ColumnValue = m_ColumnValue + \
                                                    str(format_column(column[m_nPos], m_ColumnType))
                            else:
                                if type(column[m_nPos]) == str:
                                    m_ColumnValue = m_ColumnValue + ",'" + str(column[m_nPos]) + "'"
                                elif type(column[m_nPos]) == datetime.date:
                                    m_ColumnValue = m_ColumnValue + ",DATE '" + \
                                                    format_column(column[m_nPos], m_ColumnType) + "'"
                                elif type(column[m_nPos]) == datetime.datetime:
                                    m_ColumnValue = m_ColumnValue + ",TIMESTAMP '" + \
                                                    format_column(column[m_nPos], m_ColumnType) + "'"
                                else:
                                    m_ColumnValue = m_ColumnValue + "," + \
                                                    str(format_column(column[m_nPos], m_ColumnType))
                        m_ColumnValue = m_ColumnValue + ")"
                        m_row.append(m_ColumnValue)
                    elif columntypes[m_nColumnPos] == "ARRAY":
                        m_ColumnValue = "ARRAY["
                        for m_nPos in range(0, len(column)):
                            m_ColumnType = str(type(column[m_nPos]))
                            if m_nPos == 0:
                                if type(column[m_nPos]) == str:
                                    m_ColumnValue = m_ColumnValue + "'" + str(column[m_nPos]) + "'"
                                elif type(column[m_nPos]) == datetime.date:
                                    m_ColumnValue = m_ColumnValue + "DATE '" + \
                                                    format_column(column[m_nPos], m_ColumnType) + "'"
                                elif type(column[m_nPos]) == datetime.datetime:
                                    m_ColumnValue = m_ColumnValue + "TIMESTAMP '" + \
                                                    format_column(column[m_nPos], m_ColumnType) + "'"
                                else:
                                    m_ColumnValue = m_ColumnValue + \
                                                    str(format_column(column[m_nPos], m_ColumnType))
                            else:
                                if type(column[m_nPos]) == str:
                                    m_ColumnValue = m_ColumnValue + ",'" + str(column[m_nPos]) + "'"
                                elif type(column[m_nPos]) == datetime.date:
                                    m_ColumnValue = m_ColumnValue + ",DATE '" + \
                                                    format_column(column[m_nPos], m_ColumnType) + "'"
                                elif type(column[m_nPos]) == datetime.datetime:
                                    m_ColumnValue = m_ColumnValue + ",TIMESTAMP '" + \
                                                    format_column(column[m_nPos], m_ColumnType) + "'"
                                else:
                                    m_ColumnValue = m_ColumnValue + "," + \
                                                    str(format_column(column[m_nPos], m_ColumnType))
                        m_ColumnValue = m_ColumnValue + "]"
                        m_row.append(m_ColumnValue)
                    else:
                        m_row.append(format_column(column, columntype))
                m_row = tuple(m_row)
                result.append(m_row)
            rowcount = rowcount + len(rowset)
            if len(rowset) < m_arraysize:
                # 已经没有什么可以取的了, 游标结束
                m_FetchStatus = False
        else:
            if cursor.warnings is not None:
                status = "{0} row{1} affected with warnings."
            else:
                status = "{0} row{1} affected."
            rowcount = 0 if cursor.rowcount == -1 else cursor.rowcount
            result = None
            m_FetchStatus = False

        # 只要不是最后一次打印，不再返回status内容
        if m_FetchStatus:
            status = None

        if self.SQLOptions.get('FEEDBACK').upper() == 'ON' and status is not None:
            status = status.format(rowcount, "" if rowcount in [0, 1] else "s")
        else:
            status = None
        return title, result, headers, columntypes, status, m_FetchStatus, rowcount, cursor.warnings
