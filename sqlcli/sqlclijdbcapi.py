# -*- coding: utf-8 -*-

'''
http://www.python.org/topics/database/DatabaseAPI-2.0.html
'''

import datetime
import glob
import os
import sys
import time
import traceback
import warnings
import decimal
import binascii
import jpype
from .sqlcliexception import SQLCliException
from .sqloption import SQLOptions


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
    if not jpype.isJVMStarted():
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
        jvm_path = jpype.getDefaultJVMPath()
        jpype.startJVM(jvm_path, *args, ignoreUnrecognized=True, convertStrings=True)
    if not jpype.isThreadAttachedToJVM():
        jpype.attachThreadToJVM()
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
    except TypeError as te:
        raise SQLCliException("SQLCLI-00000: Load java class failed. [" + str(jclassname) + "]")
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
        raise SQLCliException(je.toString().
                              replace("java.sql.SQLException: ", "").
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


class Warning(Exception):
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


# DB-API 2.0 Module Interface connect constructor
def connect(jclassname, url, driver_args=None, jars=None, libs=None, sqloptions=None):
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

    # 程序用到的各种会话控制参数
    global _sqloptions
    _sqloptions = sqloptions

    # 如果无法连接，则尝试3次，间隔等待2秒
    jconn = None
    retryCount = 0
    while True:
        try:
            jconn = _jdbc_connect_jpype(jclassname, url, driver_args, jars, libs)
            break
        except SQLCliException as je:
            if jconn is None:
                # jconn 为空，可能是网络错误，这里重复尝试
                if "SQLCLI_DEBUG" in os.environ:
                    print('traceback.print_exc():\n%s' % traceback.print_exc())
                    print('traceback.format_exc():\n%s' % traceback.format_exc())
                retryCount = retryCount + 1
                if retryCount >= int(_sqloptions.get("CONN_RETRY_TIMES")):
                    raise je
                else:
                    time.sleep(2)
                    continue
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

    def close(self):
        if self.jconn is not None:
            self.jconn.close()
        self._closed = True

    def commit(self):
        try:
            self.jconn.commit()
        except:
            _handle_sql_exception_jpype()

    def rollback(self):
        try:
            self.jconn.rollback()
        except:
            _handle_sql_exception_jpype()

    def cursor(self):
        return Cursor(self, self._converters)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


# DB-API 2.0 Cursor Object
class Cursor(object):
    rowcount = -1
    _meta = None
    _prep = None
    _rs = None
    _description = None

    def __init__(self, connection, converters):
        self._connection = connection
        self._converters = converters

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
                    dbapi_type = None
                else:
                    dbapi_type = DBAPITypeObject._map_jdbc_type_to_dbapi(jdbc_type)
                col_desc = (m.getColumnName(col),
                            dbapi_type,
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
        for i in range(len(parameters)):
            prep_stmt.setObject(i + 1, parameters[i])

    def execute(self, operation, parameters=None):
        if self._connection._closed:
            raise Error()
        if not parameters:
            parameters = ()
        self._close_last()
        self._prep = self._connection.jconn.prepareStatement(operation)
        self._set_stmt_parms(self._prep, parameters)
        is_rs = False
        try:
            is_rs = self._prep.execute()
        except:
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
        # self._prep.getWarnings() ???

    def executemany(self, operation, seq_of_parameters):
        self._close_last()
        self._prep = self._connection.jconn.prepareStatement(operation)
        for parameters in seq_of_parameters:
            self._set_stmt_parms(self._prep, parameters)
            self._prep.addBatch()
        update_counts = self._prep.executeBatch()
        # self._prep.getWarnings() ???
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
            if sqltype in self._converters.keys():
                converter = self._converters.get(sqltype)
            else:
                if m_ColumnClassName.upper().find("BFILE") != -1:
                    converter = _DEFAULT_CONVERTERS["BFILE"]
                elif m_ColumnClassName in ('oracle.sql.TIMESTAMPTZ', 'oracle.sql.TIMESTAMPLTZ'):
                    converter = _DEFAULT_CONVERTERS["TIMESTAMP_WITH_TIMEZONE"]
                else:
                    converter = _unknownSqlTypeConverter
                    if "SQLCLI_DEBUG" in os.environ:
                        warnings.warn("Unknown JDBC convert with constant value " + str(sqltype) +
                                      ":" + self._meta.getColumnClassName(col))
            if "SQLCLI_DEBUG" in os.environ:
                print("JDBC SQLType=[" + str(converter.__name__) + "] for col [" + str(col) + "]. " +
                      "sqltype=[" + str(sqltype) + ":" + m_ColumnClassName + "]")
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
    java_val = rs.getObject(col)
    if java_val is None:
        return
    return str(java_val)


def _to_datetime(conn, rs, col):
    java_val = rs.getTimestamp(col)
    if not java_val:
        return
    d = datetime.datetime.strptime(str(java_val)[:19], "%Y-%m-%d %H:%M:%S")
    d = d.replace(microsecond=int(str(java_val.getNanos())[:6]))
    return str(d)


def _to_time(conn, rs, col):
    java_val = rs.getTime(col)
    if not java_val:
        return
    return str(java_val)


def _to_date(conn, rs, col):
    java_val = rs.getDate(col)
    if not java_val:
        return
    # The following code requires Python 3.3+ on dates before year 1900.
    # d = datetime.datetime.strptime(str(java_val)[:10], "%Y-%m-%d")
    # return d.strftime("%Y-%m-%d")
    # Workaround / simpler soltution (see
    # https://github.com/baztian/jaydebeapi/issues/18):
    return str(java_val)[:10]


def _to_binary(conn, rs, col):
    java_val = rs.getObject(col)
    if java_val is None:
        return
    m_TypeName = str(java_val.getClass().getTypeName())
    if m_TypeName == "byte[]":
        return binascii.b2a_hex(java_val)
    elif m_TypeName.upper().find("BLOB") != -1:
        m_Bytes = java_val.getBytes(1, 1024)
        if m_Bytes is not None:
            return binascii.b2a_hex(java_val.getBytes(1, 1024))
        else:
            return ""
    elif m_TypeName.find("BFILE") != -1:
        return "bfilename(" + java_val.getDirAlias() + ":" + java_val.getName() + ")"
    else:
        # return binascii.b2a_hex(java_val.getBytes(0, 1024))
        raise SQLCliException("SQLCLI-00000: Unknown java class type [" + m_TypeName + "] in _to_binary")


def _java_to_py(java_method):
    def to_py(conn, rs, col):
        java_val = rs.getObject(col)
        if java_val is None:
            return
        if isinstance(java_val, (string_type, int, float, bool)):
            return java_val
        return getattr(java_val, java_method)()
    return to_py


def _java_to_py_bigdecimal(conn, rs, col):
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
    else:
        raise SQLCliException(
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
    else:
        raise SQLCliException(
            "SQLCLI-00000: Unknown java class type [" + m_TypeName +
            "] in _java_to_py_timestampwithtimezone")


def _java_to_py_clob(conn, rs, col):
    java_val = rs.getObject(col)
    if java_val is None:
        return
    m_TypeName = str(java_val.getClass().getTypeName())
    if m_TypeName.upper().find('CLOB') != -1:
        m_Length = java_val.length()
        m_TrimLength = int(_sqloptions.get("LOB_LENGTH"))
        m_ColumnValue = java_val.getSubString(1, m_TrimLength)
        if m_Length > int(m_TrimLength):
            m_ColumnValue = m_ColumnValue + "..."
        return m_ColumnValue
    else:
        raise SQLCliException(
            "SQLCLI-00000: Unknown java class type [" + m_TypeName +
            "] in _java_to_py_clob")


def _java_to_py_stru(conn, rs, col):
    java_val = rs.getObject(col)
    if java_val is None:
        return
    m_TypeName = str(java_val.getClass().getTypeName())
    if (m_TypeName == "java.lang.Object[]"):
        m_ColumnValue = "STRUCTURE("
        for m_nPos in range(0, len(java_val)):
            m_ColumnType = str(type(java_val[m_nPos]))
            if m_nPos == 0:
                if m_ColumnType.upper().find('STR') != -1:
                    m_ColumnValue = m_ColumnValue + "'" + str(java_val[m_nPos]) + "'"
                elif m_ColumnType.upper().find('SQLDATE') != -1:
                    m_ColumnValue = m_ColumnValue + "DATE'" + str(java_val[m_nPos]) + "'"
                elif str(type(java_val)).upper().find("FLOAT") != -1:
                    m_ColumnValue = m_ColumnValue + \
                                    _sqloptions.get("FLOAT_FORMAT") % java_val[m_nPos]
                elif str(type(java_val)).upper().find("DOUBLE") != -1:
                    m_ColumnValue = m_ColumnValue + \
                                    _sqloptions.get("DOUBLE_FORMAT") % java_val[m_nPos]
                elif type(java_val) == decimal.Decimal:
                    if _sqloptions.get("DECIMAL_FORMAT") != "":
                        m_ColumnValue = m_ColumnValue + \
                                        _sqloptions.get("DECIMAL_FORMAT") % java_val[m_nPos]
                    else:
                        m_ColumnValue = m_ColumnValue + java_val[m_nPos]
                else:
                    m_ColumnValue = m_ColumnValue + str(java_val[m_nPos])
            else:
                if m_ColumnType.upper().find('STR') != -1:
                    m_ColumnValue = m_ColumnValue + ",'" + str(java_val[m_nPos]) + "'"
                elif m_ColumnType.upper().find('SQLDATE') != -1:
                    m_ColumnValue = m_ColumnValue + ",DATE'" + str(java_val[m_nPos]) + "'"
                elif str(type(java_val)).upper().find("FLOAT") != -1:
                    m_ColumnValue = m_ColumnValue + "," + \
                                    _sqloptions.get("FLOAT_FORMAT") % java_val[m_nPos]
                elif str(type(java_val)).upper().find("DOUBLE") != -1:
                    m_ColumnValue = m_ColumnValue + "," + \
                                    _sqloptions.get("DOUBLE_FORMAT") % java_val[m_nPos]
                elif type(java_val) == decimal.Decimal:
                    if _sqloptions.get("DECIMAL_FORMAT") != "":
                        m_ColumnValue = m_ColumnValue + "," + \
                                        _sqloptions.get("DECIMAL_FORMAT") % java_val[m_nPos]
                    else:
                        m_ColumnValue = m_ColumnValue + "," + java_val[m_nPos]
                else:
                    m_ColumnValue = m_ColumnValue + "," + str(java_val[m_nPos])
        m_ColumnValue = m_ColumnValue + ")"
        return m_ColumnValue
    else:
        raise SQLCliException(
            "SQLCLI-00000: Unknown java class type [" + m_TypeName +
            "] in _java_to_py_stru")


def _java_to_py_array(conn, rs, col):
    java_val = rs.getObject(col)
    if java_val is None:
        return
    m_TypeName = str(java_val.getClass().getTypeName())
    if m_TypeName.upper().find('ARRAY') != -1:
        m_ColumnValue = "ARRAY["
        java_val = java_val.getArray();
        for m_nPos in range(0, len(java_val)):
            m_ColumnType = str(type(java_val[m_nPos]))
            if m_nPos == 0:
                if m_ColumnType.upper().find('STR') != -1:
                    m_ColumnValue = m_ColumnValue + "'" + str(java_val[m_nPos]) + "'"
                elif m_ColumnType.upper().find('SQLDATE') != -1:
                    m_ColumnValue = m_ColumnValue + "DATE'" + str(java_val[m_nPos]) + "'"
                elif str(type(java_val)).upper().find("FLOAT") != -1:
                    m_ColumnValue = m_ColumnValue + \
                                    _sqloptions.get("FLOAT_FORMAT") % java_val[m_nPos]
                elif str(type(java_val)).upper().find("DOUBLE") != -1:
                    m_ColumnValue = m_ColumnValue + \
                                    _sqloptions.get("DOUBLE_FORMAT") % java_val[m_nPos]
                elif type(java_val) == decimal.Decimal:
                    if _sqloptions.get("DECIMAL_FORMAT") != "":
                        m_ColumnValue = m_ColumnValue + \
                                        _sqloptions.get("DECIMAL_FORMAT") % java_val[m_nPos]
                    else:
                        m_ColumnValue = m_ColumnValue + java_val[m_nPos]
                else:
                    m_ColumnValue = m_ColumnValue + str(java_val[m_nPos])
            else:
                if m_ColumnType.upper().find('STR') != -1:
                    m_ColumnValue = m_ColumnValue + ",'" + str(java_val[m_nPos]) + "'"
                elif m_ColumnType.upper().find('SQLDATE') != -1:
                    m_ColumnValue = m_ColumnValue + ",DATE'" + str(java_val[m_nPos]) + "'"
                elif str(type(java_val)).upper().find("FLOAT") != -1:
                    m_ColumnValue = m_ColumnValue + "," + \
                                    _sqloptions.get("FLOAT_FORMAT") % java_val[m_nPos]
                elif str(type(java_val)).upper().find("DOUBLE") != -1:
                    m_ColumnValue = m_ColumnValue + "," + \
                                    _sqloptions.get("DOUBLE_FORMAT") % java_val[m_nPos]
                elif type(java_val) == decimal.Decimal:
                    if _sqloptions.get("DECIMAL_FORMAT") != "":
                        m_ColumnValue = m_ColumnValue + "," + \
                                        _sqloptions.get("DECIMAL_FORMAT") % java_val[m_nPos]
                    else:
                        m_ColumnValue = m_ColumnValue + "," + java_val[m_nPos]
                else:
                    m_ColumnValue = m_ColumnValue + "," + str(java_val[m_nPos])
        m_ColumnValue = m_ColumnValue + "]"
        return m_ColumnValue
    else:
        raise SQLCliException(
            "SQLCLI-00000: Unknown java class type [" + m_TypeName +
            "] in _java_to_py_array")


def _java_to_py_str(conn, rs, col):
    java_val = rs.getObject(col)
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
    'VARBINARY':                    _to_binary,
    'BINARY':                       _to_binary,
    'LONGVARBINARY':                _to_binary,
    'BLOB':                         _to_binary,
    'BFILE':                        _to_binary,
    'DECIMAL':                      _java_to_py_bigdecimal,
    'NUMERIC':                      _java_to_py_bigdecimal,
    'DOUBLE':                       _java_to_py('doubleValue'),
    'FLOAT':                        _java_to_py('doubleValue'),
    'REAL':                         _java_to_py('doubleValue'),
    'TINYINT':                      _java_to_py('intValue'),
    'INTEGER':                      _java_to_py('intValue'),
    'SMALLINT':                     _java_to_py('intValue'),
    'BIGINT':                       _java_to_py_bigdecimal,
    'BOOLEAN':                      _java_to_py('booleanValue'),
    'BIT':                          _java_to_py('booleanValue'),
    'STRUCT':                       _java_to_py_stru,
    'ARRAY':                        _java_to_py_array
}
