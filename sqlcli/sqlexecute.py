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

        # Scenario名称，如果当前SQL未指定，则重复上一个SQL的Scenario信息
        self.SQLScenario = ''

        # Scenario的优先级，如果当前SQL未指定，则重复上一个SQL的Scenario信息
        self.SQLPriority = ''

        # Transaction信息
        self.SQLTransactionName = ""

        # 当前SQL游标
        self.cur = None

        # 脚本启动的时间
        self.StartTime = time.time()

        # 当前执行的SQL脚本
        self.SQLScript = None

        # 当前脚本的TimeOut设置
        self.sqlTimeOut = -1          # SQL执行的超时时间设置
        self.scriptTimeOut = -1       # 脚本执行的超时时间设置
        self.timeout = -1             # 当前SQL的超时时间设置
        self.timeOutMode = None       # SQL|SCRIPT|NONE

    def setStartTime(self, p_StartTime):
        self.StartTime = p_StartTime

    def getStartTime(self):
        return self.StartTime

    def jqparse(self, obj, path='.'):
        class DecimalEncoder(json.JSONEncoder):
            def default(self, o):
                if isinstance(o, decimal.Decimal):
                    intPart = o.quantize(decimal.Decimal(1), decimal.ROUND_HALF_UP)
                    if o - intPart == 0:
                        return int(intPart)
                    else:
                        return float(o)
                super(DecimalEncoder, self).default(o)

        if self is None:
            pass
        if obj is None:
            if "SQLCLI_DEBUG" in os.environ:
                click.secho("[DEBUG] JQ Parse Error: obj is None")
            return "****"
        try:
            obj = json.loads(obj) if isinstance(obj, str) else obj
            find_str, find_map = '', ['["%s"]', '[%s]', '%s', '.%s']
            for im in path.split('.'):
                if not im:
                    continue
                if obj is None:
                    if "SQLCLI_DEBUG" in os.environ:
                        click.secho("[DEBUG] JQ Parse Error: obj is none")
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
                click.secho("[DEBUG] JQ Parse Error: " + repr(je))
            return "****"

    @staticmethod
    def sortresult(result):
        # 无法使用系统默认的排序函数，这里空值总是置于最后
        for i in range(len(result) - 1, 0, -1):
            for j in range(i - 1, -1, -1):
                bNeedExchange = False
                for k in range(0, len(result[i])):
                    if len(result[i]) != len(result[j]):
                        return
                    if result[i][k] is None and result[j][k] is None:
                        # 两边都是空值
                        continue
                    if result[i][k] is None and result[j][k] is not None:
                        # 左边空值， 右侧不空， 按照左侧大值来考虑
                        break
                    if result[j][k] is None and result[i][k] is not None:
                        # 右侧空值， 左边不空， 按照右侧大值来考虑
                        bNeedExchange = True
                        break
                    if not isinstance(result[i][k], type(result[j][k])):
                        if str(result[i][k]) < str(result[j][k]):
                            bNeedExchange = True
                            break
                        if str(result[i][k]) > str(result[j][k]):
                            break
                    else:
                        if result[i][k] < result[j][k]:
                            bNeedExchange = True
                            break
                        if result[i][k] > result[j][k]:
                            break
                if bNeedExchange:
                    result[j], result[i] = result[i], result[j]

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
            "columnTypes": "列类型，字符串格式",
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
        -- [Hint]  order              -- SQLCli将会把随后的SQL语句进行排序输出，原程序的输出顺序被忽略
        -- [Hint]  scenario:XXXX      -- 相关SQL的场景名称
        -- [Hint]  scenario:XXXX:P1   -- 相关SQL的场景名称以及SQL优先级，如果应用程序指定了限制的优先级，则只运行指定的优先级脚本
        -- .....
        """
        # Remove spaces and EOL
        statement = statement.strip()
        if not statement:  # Empty string
            return

        # 优先级
        m_Priorites = []
        if self.SQLOptions.get("PRIORITY") != "":
            for m_Priority in self.SQLOptions.get("PRIORITY").split(','):
                m_Priorites.append(m_Priority.strip().upper())

        # 记录SQL的文件名
        self.SQLScript = p_sqlscript

        # 分析SQL语句
        m_HasOutputScenarioSkipInfo = False
        (ret_bSQLCompleted, ret_SQLSplitResults,
         ret_SQLSplitResultsWithComments, ret_SQLHints) = SQLAnalyze(statement)

        for pos in range(0, len(ret_SQLSplitResults)):
            m_raw_sql = ret_SQLSplitResults[pos]                 # 记录原始SQL
            m_SQL_ErrorMessage = ""                                 # 错误日志信息
            m_SQL_Status = 0                                        # SQL 运行结果， 0 成功， 1 失败
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
            m_CommentSQL = ret_SQLSplitResultsWithComments[pos]   # 记录带有注释信息的SQL
            m_RewrotedSQL = []                                       # SQL可能会被多次改写

            # 分析SQLHint信息
            m_SQLHint = ret_SQLHints[pos]                         # SQL提示信息，其中Scenario用作日志处理
            if "SCENARIO" in m_SQLHint.keys():
                if m_SQLHint['SCENARIO'].strip().upper() == "END":
                    m_HasOutputScenarioSkipInfo = False
                    self.SQLScenario = ""
                    self.SQLPriority = ""
                else:
                    self.SQLScenario = m_SQLHint['SCENARIO']
                    m_HasOutputScenarioSkipInfo = False
                    if "PRIORITY" in m_SQLHint.keys():
                        self.SQLPriority = m_SQLHint['PRIORITY']

            # 检查优先级, 如果定义了优先级，但不符合定义要求，则直接跳过这个语句
            if self.SQLPriority != "" and len(m_Priorites) != 0:
                if self.SQLPriority not in m_Priorites:
                    # 由于优先级原因，这个SQL不会被执行，直接返回
                    # Scenario的第一个语句打印Skip信息
                    if not m_HasOutputScenarioSkipInfo:
                        yield {
                            "type": "result",
                            "title": None,
                            "rows": None,
                            "headers": None,
                            "columnTypes": None,
                            "status": "Skip scenario [" + self.SQLScenario + "] due to priority set."
                        }
                        m_HasOutputScenarioSkipInfo = True
                    continue

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
            match_obj = re.search(r"\${LastSQLResult\((.*?)\)}", sql, re.IGNORECASE | re.DOTALL)
            if match_obj:
                while True:
                    m_Searched = match_obj.group(0)
                    m_JQPattern = match_obj.group(1)
                    new_sql = sql.replace(m_Searched, self.jqparse(obj=self.LastJsonSQLResult, path=m_JQPattern))
                    if new_sql == sql:
                        # 没有新的变化，没有必要继续循环下去
                        break
                    sql = new_sql
                    match_obj = re.search(r"\${LastSQLResult\((.*?)\)}", sql, re.IGNORECASE | re.DOTALL)
                    if not match_obj:
                        break
                # 记录被JQ表达式改写的SQL
                if self.SQLOptions.get("SILENT").upper() != 'ON':
                    m_RewrotedSQL.append(SQLFormatWithPrefix("Your SQL has been changed to:\n" + sql, 'REWROTED '))

            # ${random(1,100)}
            # 处理脚本中的随机数问题
            match_obj = re.search(
                r"\${random_int\((\s+)?(\d+)(\s+)?,(\s+)?(\d+)(\s+)?\)}",
                sql,
                re.IGNORECASE | re.DOTALL)
            if match_obj:
                m_Searched = match_obj.group(0)
                m_random_start = int(match_obj.group(2))
                m_random_end = int(match_obj.group(5))
                sql = sql.replace(m_Searched, str(random.randint(m_random_start, m_random_end)))

            # ${var}
            bMatched = False
            while True:
                match_obj = re.search(r"\${(.*?)}", sql, re.IGNORECASE | re.DOTALL)
                if match_obj:
                    bMatched = True
                    m_Searched = match_obj.group(0)
                    m_VarName = str(match_obj.group(1)).strip()
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
                self.sqlTimeOut = int(self.SQLOptions.get("SQL_TIMEOUT"))
            except ValueError:
                self.sqlTimeOut = -1
            try:
                self.scriptTimeOut = int(self.SQLOptions.get("SCRIPT_TIMEOUT"))
            except ValueError:
                self.scriptTimeOut = -1

            # 执行SQL
            try:
                if "SQLCLI_DEBUG" in os.environ:
                    print("[DEBUG] SQL=[" + str(sql) + "]")

                # 处理超时时间问题
                if self.scriptTimeOut > 0:
                    if self.scriptTimeOut <= time.time() - self.getStartTime():
                        # 脚本还没有执行，就发现已经超时
                        if sql.upper() not in ["EXIT", "QUIT"]:
                            m_SQL_ErrorMessage = "SQLCLI-0000: Script Timeout " \
                                                 "(" + str(round(self.scriptTimeOut, 2)) + \
                                                 ") expired. Abort this Script."
                            yield {"type": "error", "message": m_SQL_ErrorMessage}
                        raise EOFError
                    else:
                        if self.sqlTimeOut > 0:
                            if self.scriptTimeOut - (time.time() - self.getStartTime()) < self.sqlTimeOut:
                                # 脚本超时剩余时间已经比SQL超时要多
                                self.timeOutMode = "SCRIPT"
                                self.timeout = self.scriptTimeOut - (time.time() - self.getStartTime())
                            else:
                                self.timeOutMode = "SQL"
                                self.timeout = self.sqlTimeOut
                        else:
                            self.timeOutMode = "SCRIPT"
                            self.timeout = self.scriptTimeOut - (time.time() - self.getStartTime())
                elif self.sqlTimeOut > 0:
                    # 没有设置SCRIPT的超时时间，只设置了SQL的超时时间
                    self.timeOutMode = "SQL"
                    self.timeout = self.sqlTimeOut
                else:
                    # 什么超时时间都没有设置
                    self.timeOutMode = None
                    self.timeout = -1

                # 首先尝试这是一个特殊命令，如果返回CommandNotFound，则认为其是一个标准SQL
                for m_Result in execute(self.SQLCliHandler, sql,  timeout=self.timeout):
                    # 保存之前的运行结果
                    if "type" not in m_Result.keys():
                        m_Result.update({"type": "result"})
                    if m_Result["type"] == "result" and \
                            ((sql.lower().startswith("__internal__") or
                              sql.lower().startswith("loaddriver")) or
                             sql.lower().startswith("host")):
                        # 如果存在SQL_LOOP信息，则需要反复执行上一个SQL
                        if "SQL_LOOP" in m_SQLHint.keys():
                            if "SQLCLI_DEBUG" in os.environ:
                                print("[DEBUG] LOOP=" + str(m_SQLHint["SQL_LOOP"]))
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
                                for m_SQLResult in \
                                        self.run("__internal__ test assert " + m_LoopUntil):
                                    if m_SQLResult["type"] == "result":
                                        if m_SQLResult["status"].startswith("Assert Successful"):
                                            m_AssertSuccessful = True
                                        break
                                if m_AssertSuccessful:
                                    break
                                else:
                                    # 测试失败, 等待一段时间后，开始下一次检查
                                    time.sleep(m_LoopInterval)
                                    if "SQLCLI_DEBUG" in os.environ:
                                        print("[DEBUG] SQL(LOOP " + str(m_nLoopPos) + ")=[" + str(sql) + "]")
                                    for m_SQLResult in self.run(sql):
                                        # 最后一次执行的结果将被传递到外层，作为SQL返回结果
                                        if m_SQLResult["type"] == "result":
                                            m_SQL_Status = 0
                                            m_Result["title"] = m_SQLResult["title"]
                                            m_Result["rows"] = m_SQLResult["rows"]
                                            m_Result["headers"] = m_SQLResult["headers"]
                                            m_Result["columnTypes"] = m_SQLResult["columnTypes"]
                                            m_Result["status"] = m_SQLResult["status"]
                                        if m_SQLResult["type"] == "error":
                                            m_SQL_Status = 1
                                            m_SQL_ErrorMessage = m_SQLResult["message"]
                            self.SQLOptions.set("TIME", m_OldTimeMode)
                            self.SQLOptions.set("TIMING", m_OldTimingMode)
                            self.SQLOptions.set("SILENT", m_OldSilentMode)

                        # 保存之前的运行结果
                        result = m_Result["rows"]

                        # 如果Hints中有order字样，对结果进行排序后再输出
                        if "Order" in m_SQLHint.keys() and result is not None:
                            if "SQLCLI_DEBUG" in os.environ:
                                print("[DEBUG] Apply Sort for this result 1.")
                            self.sortresult(result)

                        # 如果Hint中存在LogFilter，则结果集中过滤指定的输出信息
                        if "LogFilter" in m_SQLHint.keys() and result is not None:
                            for m_SQLFilter in m_SQLHint["LogFilter"]:
                                for item in result[:]:
                                    if "SQLCLI_DEBUG" in os.environ:
                                        print("[DEBUG] Apply Filter: " + str(''.join(str(item))) +
                                              " with " + m_SQLFilter)
                                    if re.match(m_SQLFilter, ''.join(str(item)), re.IGNORECASE):
                                        result.remove(item)
                                        continue

                        # 如果Hint中存在LogMask,则掩码指定的输出信息
                        if "LogMask" in m_SQLHint.keys() and result is not None:
                            for i in range(0, len(result)):
                                m_RowResult = list(result[i])
                                m_DataChanged = False
                                for j in range(0, len(m_RowResult)):
                                    if m_RowResult[j] is None:
                                        continue
                                    m_Output = str(m_RowResult[j])
                                    for m_SQLMaskString in m_SQLHint["LogMask"]:
                                        m_SQLMask = m_SQLMaskString.split("=>")
                                        if len(m_SQLMask) == 2:
                                            m_SQLMaskPattern = m_SQLMask[0]
                                            m_SQLMaskTarget = m_SQLMask[1]
                                            if "SQLCLI_DEBUG" in os.environ:
                                                print("[DEBUG] Apply Mask: " + m_Output +
                                                      " with " + m_SQLMaskPattern + "=>" + m_SQLMaskTarget)
                                            try:
                                                m_BeforeSub = m_Output
                                                nIterCount = 0
                                                while True:
                                                    # 循环多次替代，一直到没有可替代为止
                                                    m_AfterSub = re.sub(m_SQLMaskPattern, m_SQLMaskTarget,
                                                                        m_BeforeSub, re.IGNORECASE)
                                                    if m_AfterSub == m_BeforeSub or nIterCount > 99:
                                                        m_NewOutput = m_AfterSub
                                                        break
                                                    m_BeforeSub = m_AfterSub
                                                    nIterCount = nIterCount + 1
                                                if m_NewOutput != m_Output:
                                                    m_DataChanged = True
                                                    m_RowResult[j] = m_NewOutput
                                            except re.error:
                                                if "SQLCLI_DEBUG" in os.environ:
                                                    print('[DEBUG] traceback.print_exc():\n%s'
                                                          % traceback.print_exc())
                                                    print('[DEBUG] traceback.format_exc():\n%s'
                                                          % traceback.format_exc())
                                        else:
                                            if "SQLCLI_DEBUG" in os.environ:
                                                print("[DEBUG] LogMask Hint Error: " + str(m_SQLHint["LogMask"]))
                                if m_DataChanged:
                                    result[i] = tuple(m_RowResult)

                        # 处理Status
                        status = m_Result["status"]
                        if "LogMask" in m_SQLHint.keys() and status is not None:
                            for m_SQLMaskString in m_SQLHint["LogMask"]:
                                m_SQLMask = m_SQLMaskString.split("=>")
                                if len(m_SQLMask) == 2:
                                    m_SQLMaskPattern = m_SQLMask[0]
                                    m_SQLMaskTarget = m_SQLMask[1]
                                    if "SQLCLI_DEBUG" in os.environ:
                                        print("[DEBUG] Apply Mask: " + status +
                                              " with " + m_SQLMaskPattern + "=>" + m_SQLMaskTarget)
                                    try:
                                        m_BeforeSub = status
                                        nIterCount = 0
                                        while True:
                                            # 循环多次替代，最多99次，一直到没有可替代为止
                                            m_AfterSub = re.sub(m_SQLMaskPattern, m_SQLMaskTarget,
                                                                m_BeforeSub, re.IGNORECASE)
                                            if m_AfterSub == m_BeforeSub or nIterCount > 99:
                                                status = m_AfterSub
                                                break
                                            m_BeforeSub = m_AfterSub
                                            nIterCount = nIterCount + 1
                                    except re.error:
                                        if "SQLCLI_DEBUG" in os.environ:
                                            print('[DEBUG] traceback.print_exc():\n%s'
                                                  % traceback.print_exc())
                                            print('[DEBUG] traceback.format_exc():\n%s'
                                                  % traceback.format_exc())
                                    else:
                                        if "SQLCLI_DEBUG" in os.environ:
                                            print("[DEBUG] LogMask Hint Error: " + str(m_SQLHint["LogMask"]))

                        if "LogFilter" in m_SQLHint.keys() and status is not None:
                            for m_SQLFilter in m_SQLHint["LogFilter"]:
                                if "SQLCLI_DEBUG" in os.environ:
                                    print("[DEBUG] Apply Filter: " + str(''.join(str(status))) +
                                          " with " + m_SQLFilter)
                                if re.match(m_SQLFilter, status, re.IGNORECASE):
                                    status = None
                                    continue

                        # 返回运行结果
                        m_Result["rows"] = result
                        m_Result["status"] = status
                        if m_Result["rows"] is None:
                            m_Rows = 0
                        else:
                            m_Rows = len(m_Result["rows"])
                        self.LastJsonSQLResult = {"desc": m_Result["headers"],
                                                  "type": m_Result["type"],
                                                  "rows": m_Rows,
                                                  "elapsed": time.time() - start,
                                                  "result": m_Result["rows"],
                                                  "status": status,
                                                  "warnings": ""}
                        # 如果TERMOUT为关闭，不在返回结果信息
                        if self.SQLOptions.get('TERMOUT').upper() == 'OFF':
                            m_Result["rows"] = []
                            m_Result["title"] = ""
                    yield m_Result
            except SQLCliJDBCTimeOutException:
                # 处理超时时间问题
                if sql.upper() not in ["EXIT", "QUIT"]:
                    if self.timeOutMode == "SCRIPT":
                        m_SQL_ErrorMessage = "SQLCLI-0000: Script Timeout " \
                                             "(" + str(self.scriptTimeOut) + \
                                             ") expired. Abort this command."
                    else:
                        m_SQL_ErrorMessage = "SQLCLI-0000: SQL Timeout " \
                                             "(" + str(self.sqlTimeOut) + \
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
                        try:
                            # 执行数据库的SQL语句
                            if "SQL_DIRECT" in m_SQLHint.keys():
                                self.cur.execute_direct(sql, TimeOutLimit=self.timeout)
                            elif "SQL_PREPARE" in m_SQLHint.keys():
                                self.cur.execute(sql, TimeOutLimit=self.timeout)
                            else:
                                if self.SQLOptions.get("SQL_EXECUTE").upper() == "DIRECT":
                                    self.cur.execute_direct(sql, TimeOutLimit=self.timeout)
                                else:
                                    self.cur.execute(sql, TimeOutLimit=self.timeout)
                        except Exception as e:
                            if "SQL_LOOP" in m_SQLHint.keys():
                                # 如果在循环中, 错误不会处理， 一直到循环结束
                                pass
                            else:
                                raise e

                        rowcount = 0
                        m_SQL_Status = 0
                        while True:
                            (title, result, headers, columnTypes, status,
                             m_FetchStatus, m_FetchedRows, m_SQLWarnings) = \
                                self.get_result(self.cur, rowcount)
                            rowcount = m_FetchedRows
                            if "SQLCLI_DEBUG" in os.environ:
                                print("[DEBUG] headers=" + str(headers))
                                if result is not None:
                                    for m_RowPos in range(0, len(result)):
                                        for m_CellPos in range(0, len(result[m_RowPos])):
                                            print("[DEBUG] Cell[" + str(m_RowPos) + ":" +
                                                  str(m_CellPos) + "]=[" + str(result[m_RowPos][m_CellPos]) + "]")

                            # 如果Hints中有order字样，对结果进行排序后再输出
                            if "Order" in m_SQLHint.keys() and result is not None:
                                if "SQLCLI_DEBUG" in os.environ:
                                    print("[DEBUG] Apply Sort for this result 2.")
                                # 不能用sorted函数，需要考虑None出现在列表中特定元素的问题
                                # l =  [(-32767,), (32767,), (None,), (0,)]
                                # l = sorted(l, key=lambda x: (x is None, x))
                                # '<' not supported between instances of 'NoneType' and 'int'
                                self.sortresult(result)

                            # 如果Hint中存在LogFilter，则结果集中过滤指定的输出信息
                            if "LogFilter" in m_SQLHint.keys() and result is not None:
                                for m_SQLFilter in m_SQLHint["LogFilter"]:
                                    for item in result[:]:
                                        if "SQLCLI_DEBUG" in os.environ:
                                            print("[DEBUG] Apply Filter: " + str(''.join(str(item))) +
                                                  " with " + m_SQLFilter)
                                        if re.match(m_SQLFilter, ''.join(str(item)), re.IGNORECASE):
                                            result.remove(item)
                                            continue

                            # 如果Hint中存在LogMask,则掩码指定的输出信息
                            if "LogMask" in m_SQLHint.keys() and result is not None:
                                for i in range(0, len(result)):
                                    m_RowResult = list(result[i])
                                    m_DataChanged = False
                                    for j in range(0, len(m_RowResult)):
                                        if m_RowResult[j] is None:
                                            continue
                                        m_Output = str(m_RowResult[j])
                                        for m_SQLMaskString in m_SQLHint["LogMask"]:
                                            m_SQLMask = m_SQLMaskString.split("=>")
                                            if len(m_SQLMask) == 2:
                                                m_SQLMaskPattern = m_SQLMask[0]
                                                m_SQLMaskTarget = m_SQLMask[1]
                                                if "SQLCLI_DEBUG" in os.environ:
                                                    print("[DEBUG] Apply Mask: " + m_Output +
                                                          " with " + m_SQLMaskPattern + "=>" + m_SQLMaskTarget)
                                                try:
                                                    m_BeforeSub = m_Output
                                                    nIterCount = 0
                                                    while True:
                                                        # 循环多次替代，一直到没有可替代为止
                                                        m_AfterSub = re.sub(m_SQLMaskPattern, m_SQLMaskTarget,
                                                                            m_BeforeSub, re.IGNORECASE)
                                                        if m_AfterSub == m_BeforeSub or nIterCount > 99:
                                                            m_NewOutput = m_AfterSub
                                                            break
                                                        m_BeforeSub = m_AfterSub
                                                        nIterCount = nIterCount + 1
                                                    if m_NewOutput != m_Output:
                                                        m_DataChanged = True
                                                        m_RowResult[j] = m_NewOutput
                                                except re.error:
                                                    if "SQLCLI_DEBUG" in os.environ:
                                                        print('[DEBUG] traceback.print_exc():\n%s'
                                                              % traceback.print_exc())
                                                        print('[DEBUG] traceback.format_exc():\n%s'
                                                              % traceback.format_exc())
                                            else:
                                                if "SQLCLI_DEBUG" in os.environ:
                                                    print("[DEBUG] LogMask Hint Error: " + m_SQLHint["LogMask"])
                                    if m_DataChanged:
                                        result[i] = tuple(m_RowResult)
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
                                    print("[DEBUG] LOOP=" + str(m_SQLHint["SQL_LOOP"]))
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
                                        if "SQLCLI_DEBUG" in os.environ:
                                            print("[DEBUG] SQL(LOOP " + str(m_nLoopPos) + ")=[" + str(sql) + "]")
                                        for m_Result in self.run(sql):
                                            # 最后一次执行的结果将被传递到外层，作为SQL返回结果
                                            if m_Result["type"] == "result":
                                                m_SQL_Status = 0
                                                title = m_Result["title"]
                                                result = m_Result["rows"]
                                                if "Order" in m_SQLHint.keys() and result is not None:
                                                    if "SQLCLI_DEBUG" in os.environ:
                                                        print("[DEBUG] Apply Sort for this result 3.")
                                                    self.sortresult(result)
                                                headers = m_Result["headers"]
                                                columnTypes = m_Result["columnTypes"]
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
                                        "columnTypes": columnTypes,
                                        "status": status
                                    }
                                else:
                                    yield {
                                        "type": "result",
                                        "title": title,
                                        "rows": [],
                                        "headers": headers,
                                        "columnTypes": columnTypes,
                                        "status": status
                                    }
                            if not m_FetchStatus:
                                break
                    except SQLCliJDBCTimeOutException:
                        # 处理超时时间问题
                        if sql.upper() not in ["EXIT", "QUIT"]:
                            if self.timeOutMode == "SCRIPT":
                                m_SQL_ErrorMessage = "SQLCLI-0000: Script Timeout " \
                                                     "(" + str(self.scriptTimeOut) + \
                                                     ") expired. Abort this command."
                            else:
                                m_SQL_ErrorMessage = "SQLCLI-0000: SQL Timeout " \
                                                     "(" + str(self.sqlTimeOut) + \
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
                                    for pos2 in range(0, len(m_SQL_MultiLineErrorMessage)):
                                        m_NewOutput = re.sub(m_SQLMaskPattern, m_SQLMaskTarget,
                                                             m_SQL_MultiLineErrorMessage[pos2],
                                                             re.IGNORECASE)
                                        if m_NewOutput != m_SQL_MultiLineErrorMessage[pos2]:
                                            m_ErrorMessageHasChanged = True
                                            m_SQL_MultiLineErrorMessage[pos2] = m_NewOutput
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
                    "Priority": self.SQLPriority,
                    "Transaction": self.SQLTransactionName
                }
        # 当前SQL文件已经执行完毕，清空Scenario，以及Priority
        if p_sqlscript is not None:
            self.SQLScenario = ''
            self.SQLPriority = ''

    def get_result(self, cursor, rowcount):
        """
            返回的内容：
                title           输出的前提示信息
                result          结果数据集
                headers         表头信息
                columnTypes     结果字段类型
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
            elif type(p_column) == list:
                return p_column
            elif type(p_column) == datetime.date:
                m_ColumnFormat = self.SQLOptions.get("DATE_FORMAT")
                if platform.system().lower() in ['windows', 'darwin']:
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
                if platform.system().lower() in ['windows', 'darwin']:
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
        columnTypes = []
        if cursor.description is not None:
            headers = [x[0] for x in cursor.description]
            columnTypes = [x[1] for x in cursor.description]
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
                    columntype = columnTypes[m_nColumnPos]
                    # 对于空值直接返回
                    if column is None:
                        m_row.append(None)
                        continue

                    # 处理各种数据类型
                    if columnTypes[m_nColumnPos] == "STRUCT":
                        m_ColumnValue = "STRUCTURE("
                        for pos in range(0, len(column)):
                            m_ColumnType = str(type(column[pos]))
                            if pos == 0:
                                if type(column[pos]) == str:
                                    m_ColumnValue = m_ColumnValue + "'" + str(column[pos]) + "'"
                                elif type(column[pos]) == datetime.date:
                                    m_ColumnValue = m_ColumnValue + "DATE '" + \
                                                    format_column(column[pos], m_ColumnType) + "'"
                                elif type(column[pos]) == datetime.datetime:
                                    m_ColumnValue = m_ColumnValue + "TIMESTAMP '" + \
                                                    format_column(column[pos], m_ColumnType) + "'"
                                elif isinstance(column[pos], type(None)):
                                    m_ColumnValue = m_ColumnValue + "<null>"
                                else:
                                    m_ColumnValue = m_ColumnValue + \
                                                    str(format_column(column[pos], m_ColumnType))
                            else:
                                if type(column[pos]) == str:
                                    m_ColumnValue = m_ColumnValue + ",'" + str(column[pos]) + "'"
                                elif type(column[pos]) == datetime.date:
                                    m_ColumnValue = m_ColumnValue + ",DATE '" + \
                                                    format_column(column[pos], m_ColumnType) + "'"
                                elif type(column[pos]) == datetime.datetime:
                                    m_ColumnValue = m_ColumnValue + ",TIMESTAMP '" + \
                                                    format_column(column[pos], m_ColumnType) + "'"
                                elif isinstance(column[pos], type(None)):
                                    m_ColumnValue = m_ColumnValue + ",<null>"
                                else:
                                    m_ColumnValue = m_ColumnValue + "," + \
                                                    str(format_column(column[pos], m_ColumnType))
                        m_ColumnValue = m_ColumnValue + ")"
                        m_row.append(m_ColumnValue)
                    elif columnTypes[m_nColumnPos] == "ARRAY":
                        m_ColumnValue = "ARRAY["
                        if self.SQLOptions.get('OUTPUT_SORT_ARRAY') == "ON":
                            # 保证Array的输出每次都一样顺序
                            # 需要注意可能有NULL值导致字符数组无法排序的情况, column是一个一维数组
                            column.sort(key=lambda x: (x is None, x))
                        for pos in range(0, len(column)):
                            m_ColumnType = str(type(column[pos]))
                            if pos == 0:
                                if type(column[pos]) == str:
                                    m_ColumnValue = m_ColumnValue + "'" + str(column[pos]) + "'"
                                elif type(column[pos]) == datetime.date:
                                    m_ColumnValue = m_ColumnValue + "DATE '" + \
                                                    format_column(column[pos], m_ColumnType) + "'"
                                elif type(column[pos]) == datetime.datetime:
                                    m_ColumnValue = m_ColumnValue + "TIMESTAMP '" + \
                                                    format_column(column[pos], m_ColumnType) + "'"
                                elif isinstance(column[pos], type(None)):
                                    m_ColumnValue = m_ColumnValue + "<null>"
                                else:
                                    m_ColumnValue = m_ColumnValue + \
                                                    str(format_column(column[pos], m_ColumnType))
                            else:
                                if type(column[pos]) == str:
                                    m_ColumnValue = m_ColumnValue + ",'" + str(column[pos]) + "'"
                                elif type(column[pos]) == datetime.date:
                                    m_ColumnValue = m_ColumnValue + ",DATE '" + \
                                                    format_column(column[pos], m_ColumnType) + "'"
                                elif type(column[pos]) == datetime.datetime:
                                    m_ColumnValue = m_ColumnValue + ",TIMESTAMP '" + \
                                                    format_column(column[pos], m_ColumnType) + "'"
                                elif isinstance(column[pos], type(None)):
                                    m_ColumnValue = m_ColumnValue + ",<null>"
                                else:
                                    m_ColumnValue = m_ColumnValue + "," + \
                                                    str(format_column(column[pos], m_ColumnType))
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
        return title, result, headers, columnTypes, status, m_FetchStatus, rowcount, cursor.warnings
