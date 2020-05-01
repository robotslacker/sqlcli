# -*- coding: utf-8 -*-
import logging
from .sqlparse import SQLAnalyze
from .packages import special

_logger = logging.getLogger(__name__)


class SQLExecute(object):
    conn = None            # 数据库连接
    options = None         # 用户设置的各种选项

    def __init__(self):
        self.options = {"ECHO":"OFF", "LONG": 20}
        pass

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
        for sql in ret_SQLSplitResults:
            # \G is treated specially since we have to set the expanded output.
            if sql.endswith("\\G"):
                special.iocommands.set_expanded_output(True)
                sql = sql[:-2].strip()

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
                _logger.debug(
                    "Not connected to database. Will not run statement: %s.", sql
                )
                raise Exception("Please connect database first. ")
            cur = self.conn.cursor() if self.conn else None
            if self.options["ECHO"] == 'ON':
                yield 'SQL> ' + sql, None, None, None
            try:
                # 首先假设这是一个特殊命令
                for result in special.execute(cur, sql):
                    yield result
            except special.CommandNotFound:
                # 执行正常的SQL语句
                cur.execute(sql)
                yield self.get_result(cur)

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
