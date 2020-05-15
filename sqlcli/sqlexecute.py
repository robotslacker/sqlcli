# -*- coding: utf-8 -*-
import logging
from .sqlparse import SQLAnalyze
from .sqlparse import SQLFormatWithPrefix
from .commandanalyze import execute
from .commandanalyze import CommandNotFound
from .sqlcliexception import SQLCliException
import click
import time

_logger = logging.getLogger(__name__)


class SQLExecute(object):
    conn = None                         # 数据库连接
    options = None                      # 用户设置的各种选项
    logfile = None                      # 打印的日志文件
    sqlscript = None                    # 需要执行的SQL脚本
    SQLMappingHandler = None            # SQL重写处理

    def __init__(self):
        # 设置一些默认的参数
        self.options = {"WHENEVER_SQLERROR": "CONTINUE", "PAGE": "OFF", "OUTPUT_FORMAT": "ASCII", "ECHO": "ON",
                        "LONG": 20, 'KAFKA_SERVERS': None, 'TIMING': 'OFF', 'TERMOUT': 'ON', 'FEEDBACK': 'ON',
                        "ARRAYSIZE": 10000, 'SQLREWRITE': 'ON', "DEBUG": 'OFF'}

    def set_connection(self, p_conn):
        self.conn = p_conn

    def run(self, statement):
        """Execute the sql in the database and return the results. The results
        are a list of tuples. Each tuple has 4 values
        (title, rows, headers, status).
        """

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
            if self.sqlscript is not None:
                click.secho(SQLFormatWithPrefix(m_CommentSQL))

            # 如果打开了回显，并且指定了输出文件，且SQL被改写过，输出改写后的SQL
            if self.options["SQLREWRITE"] == 'ON':
                old_sql = sql
                sql = self.SQLMappingHandler.RewriteSQL(self.sqlscript, old_sql)
                if old_sql != sql:
                    if self.options["ECHO"] == 'ON' and self.logfile is not None:
                        # SQL已经发生了改变
                        click.echo(SQLFormatWithPrefix(sql, '!!!'), file=self.logfile)
                    if self.sqlscript is not None:
                        click.secho(SQLFormatWithPrefix(sql, '!!!'))

            # 如果是空语句，不在执行
            if len(sql.strip()) == 0:
                continue

            # 在没有数据库连接的时候，必须先加载驱动程序，连接数据库
            if not self.conn and not (
                sql.startswith("loaddriver")
                or sql.startswith("loadsqlmap")
                or sql.startswith("connect")
                or sql.startswith("start")
                or sql.startswith("__internal__")
                or sql.startswith("help")
                or sql.startswith("exit")
                or sql.startswith("quit")
                or sql.startswith("set")
            ):
                if self.options["WHENEVER_SQLERROR"] == "EXIT":
                    raise SQLCliException("Not Connected. ")
                else:
                    yield None, None, None, "Not connected. "

            # 打开游标
            cur = self.conn.cursor() if self.conn else None

            # 记录命令开始时间
            start = time.time()

            # 执行SQL
            try:
                # 首先假设这是一个特殊命令
                for result in execute(cur, sql):
                    yield result
            except CommandNotFound:
                # 执行正常的SQL语句
                if cur is not None:
                    try:
                        cur.execute(sql)
                        yield self.get_result(cur)
                    except Exception as e:
                        if (
                                str(e).find("SQLSyntaxErrorException") != -1 or
                                str(e).find("SQLException") != -1
                        ):
                            # SQL 语法错误
                            if self.options["WHENEVER_SQLERROR"] == "EXIT":
                                raise SQLCliException(str(e))
                            else:
                                yield None, None, None, str(e)
                        else:
                            # 其他不明错误
                            raise e
            except SQLCliException as e:
                if self.options["WHENEVER_SQLERROR"] == "EXIT":
                    raise e
                else:
                    yield None, None, None, str(e.message)

            # 记录结束时间
            end = time.time()
            # 如果需要，打印语句执行时间
            if self.options['TIMING'] == 'ON':
                if sql.strip().upper() not in ('EXIT', 'QUIT'):
                    yield None, None, None, 'Running time elapsed: %8.2f Seconds' % (end - start)

    def get_result(self, cursor):
        """Get the current result's data from the cursor."""
        title = headers = None

        # cursor.description is not None for queries that return result sets,
        # e.g. SELECT.
        result = []
        if cursor.description is not None:
            headers = [x[0] for x in cursor.description]
            status = "{0} row{1} selected."
            rowcount = 0
            while True:
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
                    break
        else:
            _logger.debug("No rows in result.")
            status = "{0} row{1} affected"
            rowcount = 0 if cursor.rowcount == -1 else cursor.rowcount
            result = None

        if self.options['FEEDBACK'] == 'ON':
            status = status.format(rowcount, "" if rowcount == 1 else "s")
        else:
            status = None
        if self.options['TERMOUT'] == 'OFF':
            return title, [], headers, status
        else:
            return title, result, headers, status
