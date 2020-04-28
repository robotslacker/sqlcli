import logging
import sqlite3
import uuid
from contextlib import closing
from .sqlparse import SQLAnalyze
from .packages import special

_logger = logging.getLogger(__name__)


class SQLExecute(object):
    conn = None            # 数据库连接
    options_echo = 'OFF'   # 是否回显执行的SQL
    options_long = 20      # 设置CLOB的默认输出长度

    databases_query = """
        PRAGMA database_list
    """

    tables_query = """
        SELECT name
        FROM sqlite_master
        WHERE type IN ('table','view') AND name NOT LIKE 'sqlite_%'
        ORDER BY 1
    """

    table_columns_query = """
        SELECT m.name as tableName, p.name as columnName
        FROM sqlite_master m
        LEFT OUTER JOIN pragma_table_info((m.name)) p ON m.name <> p.name
        WHERE m.type IN ('table','view') AND m.name NOT LIKE 'sqlite_%'
        ORDER BY tableName, columnName
    """

    functions_query = '''SELECT ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES
    WHERE ROUTINE_TYPE="FUNCTION" AND ROUTINE_SCHEMA = "%s"'''

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
                or sql.lower().startswith("use")
                or sql.startswith("\\u")
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
            if self.options_echo == 'ON':
                yield 'SQL> ' + sql, None, None, None
            try:  # Special command
                _logger.debug("Trying a dbspecial command. sql: %r", sql)
                for result in special.execute(cur, sql):
                    yield result
            except special.CommandNotFound:  # Regular SQL
                _logger.debug("Regular sql statement. sql: %r", sql)
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
                        m_row.append(column.getSubString(1, self.options_long))
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

    def tables(self):
        """Yields table names"""

        with closing(self.conn.cursor()) as cur:
            _logger.debug("Tables Query. sql: %r", self.tables_query)
            cur.execute(self.tables_query)
            for row in cur:
                yield row

    def table_columns(self):
        """Yields column names"""
        with closing(self.conn.cursor()) as cur:
            _logger.debug("Columns Query. sql: %r", self.table_columns_query)
            cur.execute(self.table_columns_query)
            for row in cur:
                yield row

    def databases(self):
        if not self.conn:
            return

        with closing(self.conn.cursor()) as cur:
            _logger.debug("Databases Query. sql: %r", self.databases_query)
            for row in cur.execute(self.databases_query):
                yield row[1]

    def functions(self):
        """Yields tuples of (schema_name, function_name)"""

        with closing(self.conn.cursor()) as cur:
            _logger.debug("Functions Query. sql: %r", self.functions_query)
            cur.execute(self.functions_query % self.dbname)
            for row in cur:
                yield row

    def show_candidates(self):
        with closing(self.conn.cursor()) as cur:
            _logger.debug("Show Query. sql: %r", self.show_candidates_query)
            try:
                cur.execute(self.show_candidates_query)
            except sqlite3.DatabaseError as e:
                _logger.error("No show completions due to %r", e)
                yield ""
            else:
                for row in cur:
                    yield (row[0].split(None, 1)[-1],)

    def server_type(self):
        self._server_type = ("sqlite3", "3")
        return self._server_type

    def get_connection_id(self):
        if not self.connection_id:
            self.reset_connection_id()
        return self.connection_id

    def reset_connection_id(self):
        # Remember current connection id
        _logger.debug("Get current connection id")
        # res = self.run('select connection_id()')
        self.connection_id = uuid.uuid4()
        # for title, cur, headers, status in res:
        #     self.connection_id = cur.fetchone()[0]
        _logger.debug("Current connection id: %s", self.connection_id)
