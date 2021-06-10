# -*- coding: utf-8 -*-

"""
http://www.python.org/topics/database/DatabaseAPI-2.0.html
"""

import datetime
import glob
import os
import sys
import time
import warnings
import decimal
import jpype
from .sqloption import SQLOptions


class SQLCliJDBCException(Exception):
    def __init__(self, message):
        Exception.__init__(self)
        self.message = message

    def __str__(self):
        return self.message


def reraise(tp, value, tb=None):
    if value is None:
        value = tp()
    else:
        value = tp(value)
    if tb:
        raise value.with_traceback(tb)
    raise value


string_type = str


# Mapping from java.sql.Types attribute name to attribute value
_jdbc_name_to_const = None


# Mapping from java.sql.Types attribute constant value to it's attribute name
_jdbc_const_to_name = []

_sqloptions = SQLOptions()

# 默认情况下blob字段内容输出的最大长度
_blobdefaultfetchsize = 20
# 默认情况下clob字段内容输出的最大长度
_clobdefaultfetchsize = 20


def _handle_sql_exception_jpype():
    import jpype
    SQLException = jpype.java.sql.SQLException
    exc_info = sys.exc_info()
    db_err = isinstance(exc_info[1], SQLException)

    if db_err:
        exc_type = DatabaseError
    else:
        exc_type = InterfaceError

    reraise(exc_type, exc_info[1], exc_info[2])


def _jdbc_connect_jpype(jclassname, url, driver_args, jars, libs):
    import jpype
    if not getattr(jpype, "isJVMStarted")():
        args = []
        class_path = []
        if jars:
            class_path.extend(jars)
        class_path.extend(_get_classpath())
        if class_path:
            args.append('-Djava.class.path=%s' %
                        os.path.pathsep.join(class_path))
        if libs:
            # path to shared libraries
            libs_path = os.path.pathsep.join(libs)
            args.append('-Djava.library.path=%s' % libs_path)
        jvm_path = getattr(jpype, "getDefaultJVMPath")()
        getattr(jpype, "startJVM")(jvm_path, *args, ignoreUnrecognized=True, convertStrings=True)
    if not getattr(jpype, "isThreadAttachedToJVM")():
        getattr(jpype, "attachThreadToJVM")()
        jpype.java.lang.Thread.currentThread().setContextClassLoader(jpype.java.lang.ClassLoader.getSystemClassLoader())
    if _jdbc_name_to_const is None:
        types = jpype.java.sql.Types
        types_map = {}
        for i in types.class_.getFields():
            if jpype.java.lang.reflect.Modifier.isStatic(i.getModifiers()):
                const = i.get(None)
                types_map[i.getName()] = const
        _init_types(types_map)

    # register driver for DriverManager
    try:
        jpype.JClass(jclassname)
    except TypeError:
        raise SQLCliJDBCException("SQLCLI-00000: Load java class failed. [" + str(jclassname) + "]")
    if isinstance(driver_args, dict):
        Properties = jpype.java.util.Properties
        info = Properties()
        for k, v in driver_args.items():
            info.setProperty(k, v)
        dargs = [info]
    else:
        dargs = driver_args

    try:
        return jpype.java.sql.DriverManager.getConnection(url, *dargs)
    except jpype.java.sql.SQLException as je:
        raise SQLCliJDBCException(je.toString().replace("java.sql.SQLException: ", "").
                                  replace("java.sql.SQLTransientConnectionException: ", "").
                                  replace("java.sql.SQLInvalidAuthorizationSpecException: ", ""))


def _get_classpath():
    """Extract CLASSPATH from system environment as JPype doesn't seem
    to respect that variable.
    """
    try:
        orig_cp = os.environ['CLASSPATH']
    except KeyError:
        return []
    expanded_cp = []
    for i in orig_cp.split(os.path.pathsep):
        expanded_cp.extend(_jar_glob(i))
    return expanded_cp


def _jar_glob(item):
    if item.endswith('*'):
        return glob.glob('%s.[jJ][aA][rR]' % item)
    else:
        return [item]


#  DB-API 模块所兼容的 DB-API 最高版本号
apilevel = '2.0'

'''
0:不支持线程安全, 多个线程不能共享此模块
1:初级线程安全支持: 线程可以共享模块, 但不能共享连接
2:中级线程安全支持线程可以共享模块和连接, 但不能共享游标
3:完全线程安全支持 线程可以共享模块, 连接及游标
'''
threadsafety = 1

# 该模块支持的 SQL 语句参数风格
paramstyle = 'qmark'


class DBAPITypeObject(object):
    _mappings = {}

    def __init__(self, *values):
        """Construct new DB-API 2.0 type object.
        values: Attribute names of java.sql.Types constants"""
        self.values = values
        for type_name in values:
            if type_name in DBAPITypeObject._mappings:
                raise ValueError("Non unique mapping for type '%s'" % type_name)
            DBAPITypeObject._mappings[type_name] = self

    def __cmp__(self, other):
        if other in self.values:
            return 0
        if other < self.values:
            return 1
        else:
            return -1

    def __repr__(self):
        return 'DBAPITypeObject(%s)' % ", ".join([repr(i) for i in self.values])

    @classmethod
    def _map_jdbc_type_to_dbapi(cls, jdbc_type_const):
        try:
            type_name = _jdbc_const_to_name[jdbc_type_const]
        except KeyError:
            if "SQLCLI_DEBUG" in os.environ:
                warnings.warn("Unknown JDBC type with constant value %d. "
                              "Using None as a default type_code." % jdbc_type_const)
            return None
        try:
            return cls._mappings[type_name]
        except KeyError:
            if "SQLCLI_DEBUG" in os.environ:
                warnings.warn("No type mapping for JDBC type '%s' (constant value %d). "
                              "Using None as a default type_code." % (type_name, jdbc_type_const))
            return None


STRING = DBAPITypeObject('CHAR', 'NCHAR', 'NVARCHAR', 'VARCHAR', 'OTHER')
TEXT = DBAPITypeObject('CLOB', 'LONGVARCHAR', 'LONGNVARCHAR', 'NCLOB', 'SQLXML', 'STRUCT', 'ARRAY')
BINARY = DBAPITypeObject('BINARY', 'BLOB', 'LONGVARBINARY', 'VARBINARY')
NUMBER = DBAPITypeObject('BOOLEAN', 'BIGINT', 'BIT', 'INTEGER', 'SMALLINT', 'TINYINT')
FLOAT = DBAPITypeObject('FLOAT', 'REAL', 'DOUBLE')
DECIMAL = DBAPITypeObject('DECIMAL', 'NUMERIC')
DATE = DBAPITypeObject('DATE')
TIME = DBAPITypeObject('TIME')
DATETIME = DBAPITypeObject('TIMESTAMP', 'TIMESTAMP_WITH_TIMEZONE')
ROWID = DBAPITypeObject('ROWID')


# DB-API 2.0 Module Interface Exceptions
class Error(Exception):
    pass


class InterfaceError(Error):
    pass


class DatabaseError(Error):
    pass


class InternalError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class DataError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass


# DB-API 2.0 Type Objects and Constructors

def _java_sql_blob(data):
    return jpype.JArray(jpype.JByte, 1)(data)


def _str_func(func):
    def to_str(*parms):
        return str(func(*parms))

    return to_str


Binary = _java_sql_blob
Date = _str_func(datetime.date)
Time = _str_func(datetime.time)
Timestamp = _str_func(datetime.datetime)


def setBlobDefaultFetchSize(p_nBlobDefaultFetchSize):
    # 程序用到的各种会话控制参数
    global _blobdefaultfetchsize
    _blobdefaultfetchsize = p_nBlobDefaultFetchSize


def setClobDefaultFetchSize(p_nClobDefaultFetchSize):
    # 程序用到的各种会话控制参数
    global _clobdefaultfetchsize
    _clobdefaultfetchsize = p_nClobDefaultFetchSize


# DB-API 2.0 Module Interface connect constructor
def connect(jclassname, url, driver_args=None, jars=None, libs=None):
    """Open a connection to a database using a JDBC driver and return
    a Connection instance.

    jclassname: Full qualified Java class name of the JDBC driver.
    url: Database url as required by the JDBC driver.
    driver_args: Dictionary or sequence of arguments to be passed to
           the Java DriverManager.getConnection method. Usually
           sequence of username and password for the db. Alternatively
           a dictionary of connection arguments (where `user` and
           `password` would probably be included). See
           http://docs.oracle.com/javase/7/docs/api/java/sql/DriverManager.html
           for more details
    jars: Jar filename or sequence of filenames for the JDBC driver
    libs: Dll/so filenames or sequence of dlls/sos used as shared
          library by the JDBC driver
    """
    if isinstance(driver_args, string_type):
        driver_args = [driver_args]
    if not driver_args:
        driver_args = []
    if jars:
        if isinstance(jars, string_type):
            jars = [jars]
    else:
        jars = []
    if libs:
        if isinstance(libs, string_type):
            libs = [libs]
    else:
        libs = []

    # 如果无法连接，则尝试3次，间隔等待2秒
    jconn = _jdbc_connect_jpype(jclassname, url, driver_args, jars, libs)
    return Connection(jconn, _converters)


def DateFromTicks(ticks):
    return Date(time.localtime(ticks)[:3])


def TimeFromTicks(ticks):
    return Time(time.localtime(ticks)[3:6])


def TimestampFromTicks(ticks):
    return Timestamp(time.localtime(ticks)[:6])


# DB-API 2.0 Connection Object
class Connection(object):
    Error = Error
    Warning = Warning
    InterfaceError = InterfaceError
    DatabaseError = DatabaseError
    InternalError = InternalError
    OperationalError = OperationalError
    ProgrammingError = ProgrammingError
    IntegrityError = IntegrityError
    DataError = DataError
    NotSupportedError = NotSupportedError

    def __init__(self, jconn, converters):
        self.jconn = jconn
        self._closed = False
        self._converters = converters

    def isClosed(self):
        return self._closed

    def close(self):
        if self.jconn is not None:
            self.jconn.close()
        self._closed = True

    def commit(self):
        try:
            self.jconn.commit()
        except Exception:
            _handle_sql_exception_jpype()

    def rollback(self):
        try:
            self.jconn.rollback()
        except Exception:
            _handle_sql_exception_jpype()

    def cursor(self):
        return Cursor(self, self._converters)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def setAutoCommit(self, p_bAutoCommit):
        self.jconn.setAutoCommit(p_bAutoCommit)


# DB-API 2.0 Cursor Object
class Cursor(object):
    rowcount = -1
    _meta = None
    _prep = None
    _stmt = None
    _rs = None
    _description = None
    warnings = None

    def __init__(self, connection, converters):
        self._connection = connection
        self._converters = converters

    # 取消当前正在执行的SQL语句
    def cancel(self):
        try:
            if self._prep is not None:
                self._prep.cancel()
            if self._stmt is not None:
                self._stmt.cancel()
        except Exception:
            # 并不处理任何cancel时候发生的错误
            pass

    @property
    def description(self):
        if self._description:
            return self._description
        m = self._meta
        if m:
            count = m.getColumnCount()
            self._description = []
            for col in range(1, count + 1):
                size = m.getColumnDisplaySize(col)
                jdbc_type = m.getColumnType(col)
                if jdbc_type == 0:
                    # PEP-0249: SQL NULL values are represented by the
                    # Python None singleton
                    column_type = None
                else:
                    if jdbc_type not in _jdbc_const_to_name:
                        column_type = "UNKNOWN"
                    else:
                        column_type = str(_jdbc_const_to_name[jdbc_type])
                col_desc = (m.getColumnLabel(col),
                            column_type,
                            size,
                            size,
                            m.getPrecision(col),
                            m.getScale(col),
                            m.isNullable(col),
                            )
                self._description.append(col_desc)
            return self._description

    #   optional callproc(self, procname, *parameters) unsupported
    def close(self):
        self._close_last()
        self._connection = None

    def _close_last(self):
        """Close the resultset and reset collected meta data.
        """
        if self._rs:
            self._rs.close()
        self._rs = None
        if self._prep:
            self._prep.close()
        self._prep = None
        self._meta = None
        self._description = None

    def _set_stmt_parms(self, prep_stmt, parameters):
        if self:
            pass
        for i in range(len(parameters)):
            if type(parameters[i]) == list:
                m_JavaObject = None
                if len(parameters[i]) != 0:
                    m_JavaArray = jpype.JArray(jpype.JObject)
                    m_JavaObject = m_JavaArray(parameters[i])
                prep_stmt.setObject(i + 1, m_JavaObject)
            else:
                prep_stmt.setObject(i + 1, parameters[i])

    def execute_direct(self, operation):
        if self._connection.isClosed():
            raise Error()
        self._close_last()
        is_rs = False
        self._stmt = self._connection.jconn.createStatement()
        try:
            is_rs = self._stmt.execute(operation)
            m_SQLWarnMessage = None
            m_SQLWarnings = self._stmt.getWarnings()
            while m_SQLWarnings is not None:
                if m_SQLWarnMessage is None:
                    m_SQLWarnMessage = m_SQLWarnings.getMessage()
                else:
                    m_SQLWarnMessage = m_SQLWarnMessage + "\n" + m_SQLWarnings.getMessage()
                m_SQLWarnings = m_SQLWarnings.getNextWarning()
            self.warnings = m_SQLWarnMessage
        except Exception:
            _handle_sql_exception_jpype()
        self._rs = self._stmt.getResultSet()
        if self._rs is not None:
            self._meta = self._rs.getMetaData()
            self.rowcount = self._stmt.getUpdateCount()
        else:
            self._meta = None
            self.rowcount = -1
        if is_rs:
            self.rowcount = -1
        else:
            self.rowcount = self._stmt.getUpdateCount()

    def execute(self, operation, parameters=None):
        if self._connection.isClosed():
            raise Error()
        if not parameters:
            parameters = ()
        self._close_last()
        self._prep = self._connection.jconn.prepareStatement(operation)
        self._set_stmt_parms(self._prep, parameters)
        is_rs = False
        try:
            is_rs = self._prep.execute()
            m_SQLWarnMessage = None
            m_SQLWarnings = self._prep.getWarnings()
            while m_SQLWarnings is not None:
                if m_SQLWarnMessage is None:
                    m_SQLWarnMessage = m_SQLWarnings.getMessage()
                else:
                    m_SQLWarnMessage = m_SQLWarnMessage + "\n" + m_SQLWarnings.getMessage()
                m_SQLWarnings = m_SQLWarnings.getNextWarning()
            self.warnings = m_SQLWarnMessage
        except Exception:
            _handle_sql_exception_jpype()
        # 忽略对execute的返回判断，总是恒定的去调用getResultSet
        self._rs = self._prep.getResultSet()
        if self._rs is not None:
            self._meta = self._rs.getMetaData()
        else:
            self._meta = None
        if is_rs:
            self.rowcount = -1
        else:
            self.rowcount = self._prep.getUpdateCount()

    def executemany(self, operation, seq_of_parameters):
        self._close_last()
        self._prep = self._connection.jconn.prepareStatement(operation)
        for parameters in seq_of_parameters:
            self._set_stmt_parms(self._prep, parameters)
            self._prep.addBatch()
        update_counts = self._prep.executeBatch()
        m_SQLWarnMessage = None
        m_SQLWarnings = self._prep.getWarnings()
        while m_SQLWarnings is not None:
            if m_SQLWarnMessage is None:
                m_SQLWarnMessage = m_SQLWarnings.getMessage()
            else:
                m_SQLWarnMessage = m_SQLWarnMessage + "\n" + m_SQLWarnings.getMessage()
            m_SQLWarnings = m_SQLWarnings.getNextWarning()
        self.warnings = m_SQLWarnMessage
        self.rowcount = sum(update_counts)
        self._close_last()

    def fetchone(self):
        if not self._rs:
            raise Error()
        if not self._rs.next():
            return None
        row = []
        for col in range(1, self._meta.getColumnCount() + 1):
            sqltype = self._meta.getColumnType(col)
            m_ColumnClassName = self._meta.getColumnClassName(col)
            m_ColumnTypeName = self._meta.getColumnTypeName(col)
            if m_ColumnClassName is None:
                # NULL值
                converter = _DEFAULT_CONVERTERS["VARCHAR"]
            elif m_ColumnClassName in 'org.postgresql.util.PGmoney':
                converter = _DEFAULT_CONVERTERS["VARCHAR"]
            elif m_ColumnClassName in ('oracle.sql.TIMESTAMPTZ', 'oracle.sql.TIMESTAMPLTZ'):
                converter = _DEFAULT_CONVERTERS["TIMESTAMP_WITH_TIMEZONE"]
            elif m_ColumnClassName.upper().find("BFILE") != -1:
                converter = _DEFAULT_CONVERTERS["BFILE"]
            elif m_ColumnTypeName in ['YEAR']:  # mysql数据类型
                converter = _DEFAULT_CONVERTERS["YEAR"]
            else:
                if sqltype in self._converters.keys():
                    converter = self._converters.get(sqltype)
                else:
                    converter = _unknownSqlTypeConverter
                    if "SQLCLI_DEBUG" in os.environ:
                        warnings.warn("Unknown JDBC convert with constant value " + str(sqltype) +
                                      ":" + self._meta.getColumnClassName(col))
            if "SQLCLI_DEBUG" in os.environ:
                print("JDBC SQLType=[" + str(converter.__name__) + "] for col [" + str(col) + "]. " +
                      "sqltype=[" + str(sqltype) + ":" + str(m_ColumnClassName) + ":" + m_ColumnTypeName + "]")
            v = converter(self._connection.jconn, self._rs, col)
            row.append(v)
        return tuple(row)

    def fetchmany(self, size=None):
        if not self._rs:
            raise Error()
        if size is None:
            size = self.arraysize
        self._rs.setFetchSize(size)
        rows = []
        row = None
        for i in range(size):
            row = self.fetchone()
            if row is None:
                break
            else:
                rows.append(row)
        # reset fetch size
        if row:
            self._rs.setFetchSize(0)
        return rows

    def fetchall(self):
        rows = []
        while True:
            row = self.fetchone()
            if row is None:
                break
            else:
                rows.append(row)
        return rows

    # optional nextset() unsupported

    arraysize = 1

    def setinputsizes(self, sizes):
        pass

    def setoutputsize(self, size, column=None):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def _unknownSqlTypeConverter(conn, rs, col):
    if conn:
        pass
    java_val = rs.getObject(col)
    if java_val is None:
        return
    return str(java_val)


def _to_datetime(conn, rs, col):
    if conn:
        pass
    java_val = rs.getTimestamp(col)
    if not java_val:
        return
    try:
        d = datetime.datetime.strptime(str(java_val)[:19], "%Y-%m-%d %H:%M:%S")
        d = d.replace(microsecond=int(str(java_val.getNanos())[:6]))
    except ValueError:
        # 处理一些超过Python数据类型限制的时间类型
        d = str(java_val)
    return str(d)


def _to_time(conn, rs, col):
    if conn:
        pass
    try:
        java_val = rs.getTime(col)
    except Exception:
        # 处理一些超过Python数据类型限制的时间类型
        return rs.getString(col)
    if not java_val:
        return
    return str(java_val)


def _to_year(conn, rs, col):
    if conn:
        pass
    try:
        java_val = rs.getDate(col)
    except Exception:
        # 处理一些超过Python数据类型限制的时间类型
        return rs.getString(col)
    if not java_val:
        return
    java_cal = jpype.JClass("java.util.Calendar").getInstance()
    java_cal.setTime(java_val)
    return java_cal.get(1)


def _to_date(conn, rs, col):
    if conn:
        pass
    try:
        java_val = rs.getDate(col)
    except Exception:
        # 处理一些超过Python数据类型限制的时间类型
        return rs.getString(col)
    if not java_val:
        return
    # The following code requires Python 3.3+ on dates before year 1900.
    # d = datetime.datetime.strptime(str(java_val)[:10], "%Y-%m-%d")
    # return d.strftime("%Y-%m-%d")
    # Workaround / simpler soltution (see
    # https://github.com/baztian/jaydebeapi/issues/18):
    return str(java_val)[:10]


def _to_bit(conn, rs, col):
    if conn:
        pass
    try:
        java_val = rs.getObject(col)
    except Exception:
        # 不支持getObject操作, 直接返回字符类型
        str_val = rs.getString(col)
        if str_val is None:
            return
        else:
            return "0b" + rs.getString(col)
    if java_val is None:
        return
    m_TypeName = str(java_val.getClass().getTypeName())
    if m_TypeName == "byte[]":
        m_BitAsciiStr = "0b" + str(bin(int.from_bytes(java_val, sys.byteorder))).lstrip("0b").zfill(len(java_val)*8)
        return m_BitAsciiStr
    elif m_TypeName.find("Boolean") != -1:
        str_val = rs.getString(col)
        if str_val is None:
            return
        else:
            if len(str_val) > 1:
                # 这不是一个标准的Boolean，按照字节内容返回
                return "0b" + str(str_val)
            else:
                if java_val == 0:
                    return "False"
                else:
                    return "True"
    elif m_TypeName.find("BinaryData") != -1:
        m_Byte = java_val.getBytes()
        return "0b" + str(bin(int.from_bytes(m_Byte, sys.byteorder))).lstrip("0b").zfill(len(m_Byte)*8)
    else:
        raise SQLCliJDBCException("SQLCLI-00000: Unknown java class type [" + m_TypeName + "] in _to_bit")


def _to_varbinary(conn, rs, col):
    if conn:
        pass
    java_val = rs.getObject(col)
    if java_val is None:
        return
    m_TypeName = str(java_val.getClass().getTypeName())
    if m_TypeName == "byte[]":
        # 截断所有后面为0的字节数组内容
        m_TrimPos = -1
        for m_nPos in range(len(java_val) - 1, 0, -1):
            if java_val[m_nPos] != 0:
                m_TrimPos = m_nPos
                break
        if m_TrimPos == -1:
            return java_val
        else:
            return java_val[:m_TrimPos + 1]
    elif m_TypeName.upper().find("BLOB") != -1:
        m_Length = java_val.length()
        # 总是返回比LOB_LENGTH大1的字节数，后续的显示中将根据LOB_LENGTH是否超长做出是否打印省略号的标志
        m_TrimLength = _blobdefaultfetchsize + 1
        if m_TrimLength > m_Length:
            m_TrimLength = m_Length
        m_Bytes = java_val.getBytes(1, int(m_TrimLength))
        return m_Bytes
    elif m_TypeName.find("BFILE") != -1:
        return "bfilename(" + java_val.getDirAlias() + ":" + java_val.getName() + ")"
    elif m_TypeName.find("Boolean") != -1:
        str_val = rs.getString(col)
        if str_val is None:
            return
        else:
            if java_val == 0:
                return "False"
            else:
                return "True"
    else:
        raise SQLCliJDBCException("SQLCLI-00000: Unknown java class type [" + m_TypeName + "] in _to_binary")


def _to_binary(conn, rs, col):
    if conn:
        pass
    java_val = rs.getObject(col)
    if java_val is None:
        return
    m_TypeName = str(java_val.getClass().getTypeName())
    if m_TypeName == "byte[]":
        return java_val
    elif m_TypeName.upper().find("BLOB") != -1:
        m_Length = java_val.length()
        # 总是返回比LOB_LENGTH大1的字节数，后续的显示中将根据LOB_LENGTH是否超长做出是否打印省略号的标志
        m_TrimLength = _blobdefaultfetchsize + 1
        if m_TrimLength > m_Length:
            m_TrimLength = m_Length
        m_Bytes = java_val.getBytes(1, int(m_TrimLength))
        return m_Bytes
    elif m_TypeName.find("BFILE") != -1:
        return "bfilename(" + java_val.getDirAlias() + ":" + java_val.getName() + ")"
    elif m_TypeName.find("Boolean") != -1:
        str_val = rs.getString(col)
        if str_val is None:
            return
        else:
            if java_val == 0:
                return "False"
            else:
                return "True"
    else:
        raise SQLCliJDBCException("SQLCLI-00000: Unknown java class type [" + m_TypeName + "] in _to_binary")


def _java_to_py(java_method):
    def to_py(conn, rs, col):
        if conn:
            pass
        java_val = rs.getObject(col)
        if java_val is None:
            return
        if isinstance(java_val, (string_type, int, float, bool)):
            return java_val
        return getattr(java_val, java_method)()
    return to_py


def _java_to_py_bigdecimal(conn, rs, col):
    if conn:
        pass
    java_val = rs.getObject(col)
    if java_val is None:
        return
    m_TypeName = str(java_val.getClass().getTypeName())
    if m_TypeName == "java.math.BigDecimal":
        return decimal.Decimal(java_val.toPlainString())
    elif m_TypeName == "java.lang.Long":
        return decimal.Decimal(java_val.toString())
    elif m_TypeName == "java.math.BigInteger":
        return decimal.Decimal(java_val.toString())
    elif m_TypeName == "java.lang.Double":
        return decimal.Decimal(java_val.toString())
    elif m_TypeName == "java.lang.Float":
        return decimal.Decimal(java_val.toString())
    else:
        raise SQLCliJDBCException(
            "SQLCLI-00000: Unknown java class type [" + m_TypeName + "] in _java_to_py_bigdecimal")


def _java_to_py_timestampwithtimezone(conn, rs, col):
    java_val = rs.getObject(col)
    if java_val is None:
        return
    m_TypeName = str(java_val.getClass().getTypeName())
    if m_TypeName == "java.time.OffsetDateTime":
        return java_val.format(
            jpype.java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS Z"))
    elif m_TypeName in ('oracle.sql.TIMESTAMPTZ', 'oracle.sql.TIMESTAMPLTZ'):
        return java_val.offsetDateTimeValue(conn).format(
            jpype.java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS Z"))
    elif m_TypeName in 'org.h2.api.TimestampWithTimeZone':
        return java_val.toString()
    else:
        raise SQLCliJDBCException(
            "SQLCLI-00000: Unknown java class type [" + m_TypeName +
            "] in _java_to_py_timestampwithtimezone")


def _java_to_py_clob(conn, rs, col):
    if conn:
        pass
    java_val = rs.getObject(col)
    if java_val is None:
        return
    m_TypeName = str(java_val.getClass().getTypeName())
    if m_TypeName.upper().find('CLOB') != -1:
        m_Length = java_val.length()
        m_TrimLength = _clobdefaultfetchsize
        if m_TrimLength > m_Length:
            m_TrimLength = m_Length
        m_ColumnValue = java_val.getSubString(1, int(m_TrimLength))
        if m_Length > int(m_TrimLength):
            m_ColumnValue = m_ColumnValue + "..."
        return m_ColumnValue
    else:
        raise SQLCliJDBCException(
            "SQLCLI-00000: Unknown java class type [" + m_TypeName +
            "] in _java_to_py_clob")


def _java_to_py_stru(conn, rs, col):
    if conn:
        pass
    java_val = rs.getObject(col)
    if java_val is None:
        return
    m_TypeName = str(java_val.getClass().getTypeName())
    if m_TypeName == "java.lang.Object[]":
        m_retVal = tuple(java_val)
        return m_retVal
    else:
        raise SQLCliJDBCException(
            "SQLCLI-00000: Unknown java class type [" + m_TypeName +
            "] in _java_to_py_stru")


def _java_to_py_array(conn, rs, col):
    if conn:
        pass
    java_val = rs.getObject(col)
    if java_val is None:
        return
    m_TypeName = str(java_val.getClass().getTypeName())
    if m_TypeName.upper().find('ARRAY') != -1:
        java_val = java_val.getArray()
        m_retVal = []
        m_retVal.extend(java_val)
        return m_retVal
    elif m_TypeName.find('Object[]') != -1:
        m_retVal = []
        m_retVal.extend(java_val)
        return m_retVal
    else:
        raise SQLCliJDBCException(
            "SQLCLI-00000: Unknown java class type [" + m_TypeName +
            "] in _java_to_py_array")


def _java_to_py_str(conn, rs, col):
    if conn:
        pass
    try:
        java_val = rs.getObject(col)
        if java_val is None:
            return
    except Exception:
        java_val = rs.getString(col)
        if java_val is None:
            return
    return str(java_val)


def _init_types(types_map):
    global _jdbc_name_to_const
    _jdbc_name_to_const = types_map
    global _jdbc_const_to_name
    _jdbc_const_to_name = dict((y, x) for x, y in types_map.items())
    _init_converters(types_map)


def _init_converters(types_map):
    # 存在不属于标准JDBC的convert情况，所以如果types_map找不到，这里不报错
    global _converters
    _converters = {}
    for i in _DEFAULT_CONVERTERS:
        if i in types_map:
            const_val = types_map[i]
            _converters[const_val] = _DEFAULT_CONVERTERS[i]


# Mapping from java.sql.Types field to converter method
_converters = None

_DEFAULT_CONVERTERS = {
    # see
    # http://download.oracle.com/javase/8/docs/api/java/sql/Types.html
    # for possible keys
    'CHAR':                         _java_to_py_str,
    'LONGVARCHAR':                  _java_to_py_str,
    'VARCHAR':                      _java_to_py_str,
    'NCLOB':                        _java_to_py_clob,
    'CLOB':                         _java_to_py_clob,
    'TIMESTAMP_WITH_TIMEZONE':      _java_to_py_timestampwithtimezone,
    'TIMESTAMP':                    _to_datetime,
    'TIME':                         _to_time,
    'DATE':                         _to_date,
    'YEAR':                         _to_year,
    'VARBINARY':                    _to_varbinary,
    'BINARY':                       _to_binary,
    'LONGVARBINARY':                _to_binary,
    'BLOB':                         _to_binary,
    'BFILE':                        _to_binary,
    'DECIMAL':                      _java_to_py_bigdecimal,
    'NUMERIC':                      _java_to_py_bigdecimal,
    'DOUBLE':                       _java_to_py_bigdecimal,
    'FLOAT':                        _java_to_py('doubleValue'),
    'REAL':                         _java_to_py_bigdecimal,
    'TINYINT':                      _java_to_py('intValue'),
    'INTEGER':                      _java_to_py('intValue'),
    'SMALLINT':                     _java_to_py('intValue'),
    'BIGINT':                       _java_to_py_bigdecimal,
    'BOOLEAN':                      _java_to_py('booleanValue'),
    'BIT':                          _to_bit,
    'STRUCT':                       _java_to_py_stru,
    'ARRAY':                        _java_to_py_array
}
