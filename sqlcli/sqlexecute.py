# -*- coding: utf-8 -*-
import logging
from .sqlparse import SQLAnalyze
from .commandanalyze import execute
from .commandanalyze import CommandNotFound
from .sqlcliexception import SQLCliException

_logger = logging.getLogger(__name__)


class SQLExecute(object):
    conn = None            # 数据库连接
    options = None         # 用户设置的各种选项

    def __init__(self):
        # 设置一些默认的参数
        self.options = {}
        self.options["WHENEVER_SQLERROR"] = "CONTINUE"
        self.options["PAGE"] = "OFF"
        self.options["OUTPUT_FORMAT"] = "ASCII"
        self.options["ECHO"] = "OFF"
        self.options["LONG"] = 20

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

            # SQL的回显
            if self.options["ECHO"] == 'ON':
                if len(m_CommentSQL.strip()) != 0:
                    yield 'SQL> ' + m_CommentSQL, None, None, None

            # 如果是空语句，不在执行
            if len(sql.strip()) == 0:
                continue

            # 在没有数据库连接的时候，必须先加载驱动程序，连接数据库
            if not self.conn and not (
                sql.startswith("load")
                or sql.startswith(".load")
                or sql.startswith("connect")
                or sql.startswith(".connect")
                or sql.startswith("start")
                or sql.startswith(".start")
                or sql.startswith("__internal__")
                or sql.startswith(".internal")
                or sql.startswith("\\?")
                or sql.startswith("\\q")
                or sql.startswith("help")
                or sql.startswith("exit")
                or sql.startswith("quit")
                or sql.startswith("set")
            ):
                if self.options["WHENEVER_SQLERROR"] == "CONTINUE":
                    yield None,None,None, "Not connected. "
                else:
                    raise SQLCliException("Not Connected. ")
            cur = self.conn.cursor() if self.conn else None

            try:
                # 首先假设这是一个特殊命令
                for result in execute(cur, sql):
                    yield result
            except CommandNotFound:
                # 执行正常的SQL语句
                if cur is not None:
                    cur.execute(sql)
                    yield self.get_result(cur)
            except SQLCliException as e:
                if self.options["WHENEVER_SQLERROR"] == "CONTINUE":
                    yield None,None,None, str(e.message)
                else:
                    raise Exception(e.message)

    def get_result(self, cursor):
        """Get the current result's data from the cursor."""
        title = headers = None

        # cursor.description is not None for queries that return result sets,
        # e.g. SELECT.
        if cursor.description is not None:
            headers = [x[0] for x in cursor.description]
            status = "{0} row{1} selected."
            cursor = list(cursor.fetchall())
            result = []
            for row in cursor:
                m_row = []
                for column in row:
                    if str(type(column)).find('JDBCClobClient') != -1:
                        m_row.append(column.getSubString(1, int(self.options["LONG"])))
                    else:
                        m_row.append(column)
                m_row = tuple(m_row)
                result.append(m_row)
            cursor = result
            rowcount = len(cursor)
        else:
            _logger.debug("No rows in result.")
            status = "{0} row{1} affected"
            rowcount = 0 if cursor.rowcount == -1 else cursor.rowcount
            cursor = None

        status = status.format(rowcount, "" if rowcount == 1 else "s")
        return title, cursor, headers, status
