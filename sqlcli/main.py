# -*- coding: utf-8 -*-
import os
import sys
import traceback
from io import open
import jaydebeapi
import re

from cli_helpers.tabular_output import TabularOutputFormatter
from cli_helpers.tabular_output import preprocessors
import click

from prompt_toolkit.shortcuts import PromptSession
from .sqlexecute import SQLExecute
from .sqlinternal import Create_file
from .sqlinternal import Create_SeedCacheFile
from .sqlcliexception import SQLCliException
from .commandanalyze import register_special_command
from .commandanalyze import CommandNotFound
from .sqlparse import SQLMapping

from .__init__ import __version__
from .sqlparse import SQLAnalyze

import itertools

click.disable_unicode_literals_warning = True

PACKAGE_ROOT = os.path.abspath(os.path.dirname(__file__))


class SQLCli(object):
    # 数据库连接的各种参数
    jar_file = None
    driver_class = None
    db_url = None
    db_username = None
    db_password = None
    db_type = None
    db_driver_type = None
    db_host = None
    db_port = None
    db_service_name = None
    db_conn = None

    # SQLCli的初始化参数
    logon = None
    logfilename = None
    sqlscript = None
    sqlmap = None
    nologo = None

    # 执行者
    SQLMappingHandler = None
    SQLExecuteHandler = None

    # 程序输出日志
    logfile = None

    # 屏幕控制程序
    prompt_app = None

    def __init__(
            self,
            logon=None,
            logfilename=None,
            sqlscript=None,
            sqlmap=None,
            nologo=None,
            breakwitherror=False
    ):
        # 初始化SQLExecute和SQLMap
        self.SQLExecuteHandler = SQLExecute()
        self.SQLMappingHandler = SQLMapping()

        # 传递各种参数
        self.sqlscript = sqlscript
        self.sqlmap = sqlmap
        self.nologo = nologo
        self.logon = logon
        self.logfilename = logfilename

        # 设置其他的变量
        self.SQLExecuteHandler.sqlscript = sqlscript
        self.SQLExecuteHandler.SQLMappingHandler = self.SQLMappingHandler
        self.SQLExecuteHandler.logfile = self.logfile

        # 默认的输出格式
        self.formatter = TabularOutputFormatter(format_name='ascii')
        self.formatter.sqlcli = self
        self.syntax_style = 'default'
        self.output_style = None

        # 加载一些特殊的命令
        self.register_special_commands()

        # 设置WHENEVER_SQLERROR
        if breakwitherror:
            self.SQLExecuteHandler.options["WHENEVER_SQLERROR"] = "EXIT"

    def register_special_commands(self):

        # 加载数据库驱动
        register_special_command(
            self.load_driver,
            command="loaddriver",
            description="load JDBC driver .",
            hidden=False
        )

        # 加载SQL映射文件
        register_special_command(
            self.load_sqlmap,
            command="loadsqlmap",
            description="load SQL Mapping file .",
            hidden=False
        )

        # 连接数据库
        register_special_command(
            handler=self.connect_db,
            command="connect",
            description="Connect to database .",
            hidden=False
        )

        # 从文件中执行脚本
        register_special_command(
            self.execute_from_file,
            command="start",
            description="Execute commands from file.",
            hidden=False
        )

        # 设置各种参数选项
        register_special_command(
            self.set_options,
            command="set",
            description="set options .",
            hidden=False
        )

        # 执行特殊的命令
        register_special_command(
            self.execute_internal_command,
            command="__internal__",
            description="execute internal command.",
            hidden=False
        )

    # 加载JDBC驱动文件
    def load_driver(self, arg, **_):
        if arg is None:
            raise SQLCliException("Missing required argument, load [driver file name] [driver class name].")
        elif arg == "":
            raise SQLCliException("Missing required argument, load [driver file name] [driver class name].")
        else:
            load_parameters = str(arg).split()
            if len(load_parameters) != 2:
                raise SQLCliException("Missing required argument, loaddriver [driver file name] [driver class name].")
            # 首先尝试，绝对路径查找这个文件
            # 如果没有找到，尝试从脚本所在的路径开始查找
            m_NewJarFile = None
            if not os.path.exists(str(load_parameters[0])):
                if self.sqlscript is not None:
                    if os.path.exists(os.path.join(os.path.dirname(self.sqlscript), str(load_parameters[0]))):
                        m_NewJarFile = os.path.join(os.path.dirname(self.sqlscript), str(load_parameters[0]))
                else:
                    # 用户在Console上输入，如果路径信息不全，则放弃
                    raise SQLCliException("driver file [" + str(arg) + "] does not exist.")
            else:
                m_NewJarFile = str(load_parameters[0])

            # 如果jar包或者驱动类发生了变化，则当前数据库连接自动失效
            if self.jar_file:
                if self.jar_file != m_NewJarFile:
                    self.db_conn = None
                    self.SQLExecuteHandler.set_connection(None)
            if self.driver_class:
                if self.driver_class != str(load_parameters[1]):
                    self.db_conn = None
                    self.SQLExecuteHandler.set_connection(None)
            self.jar_file = m_NewJarFile
            self.driver_class = str(load_parameters[1])

        yield (
            None,
            None,
            None,
            'Driver loaded.'
        )

    # 加载数据库SQL映射
    def load_sqlmap(self, arg, **_):
        self.SQLExecuteHandler.options["SQLREWRITE"] = "ON"
        self.SQLMappingHandler.Load_SQL_Mappings(self.sqlscript, arg)
        yield (
            None,
            None,
            None,
            'Mapping file loaded.'
        )

    # 连接数据库
    def connect_db(self, arg, **_):
        if arg is None:
            raise SQLCliException(
                "Missing required argument\n." + "connect [user name]/[password]@" +
                "jdbc:[db type]:[driver type]://[host]:[port]/[service name]")
        elif arg == "":
            raise SQLCliException(
                "Missing required argument\n." + "connect [user name]/[password]@" +
                "jdbc:[db type]:[driver type]://[host]:[port]/[service name]")
        elif self.jar_file is None:
            raise SQLCliException("Please load driver first.")

        # 去掉为空的元素
        connect_parameters = [var for var in re.split(r'//|:|@| |/', arg) if var]
        if len(connect_parameters) == 8:
            # 指定了所有的数据库连接参数
            self.db_username = connect_parameters[0]
            self.db_password = connect_parameters[1]
            self.db_type = connect_parameters[3]
            self.db_driver_type = connect_parameters[4]
            self.db_host = connect_parameters[5]
            self.db_port = connect_parameters[6]
            self.db_service_name = connect_parameters[7]
            self.db_url = \
                connect_parameters[2] + ':' + connect_parameters[3] + ':' + \
                connect_parameters[4] + '://' + connect_parameters[5] + ':' + \
                connect_parameters[6] + ':/' + connect_parameters[7]
        elif len(connect_parameters) == 2:
            # 用户只指定了用户名和口令， 认为用户和上次保留一直的连接字符串信息
            self.db_username = connect_parameters[0]
            self.db_password = connect_parameters[1]
            if not self.db_url:
                if "SQLCLI_CONNECTION_URL" in os.environ:
                    # 从环境变量里头拼的连接字符串
                    connect_parameters = [var for var in re.split(r'//|:|@|/', os.environ['SQLCLI_CONNECTION_URL']) if
                                          var]
                    if len(connect_parameters) == 6:
                        self.db_type = connect_parameters[1]
                        self.db_driver_type = connect_parameters[2]
                        self.db_host = connect_parameters[3]
                        self.db_port = connect_parameters[4]
                        self.db_service_name = connect_parameters[5]
                        self.db_url = \
                            connect_parameters[0] + ':' + connect_parameters[1] + ':' +\
                            connect_parameters[2] + '://' + connect_parameters[3] + ':' + \
                            connect_parameters[4] + ':/' + connect_parameters[5]
                    else:
                        print("db_type = [" + str(self.db_type) + "]")
                        print("db_host = [" + str(self.db_host) + "]")
                        print("db_port = [" + str(self.db_port) + "]")
                        print("db_service_name = [" + str(self.db_service_name) + "]")
                        print("db_url = [" + str(self.db_url) + "]")
                        raise SQLCliException("Unexpeced env SQLCLI_CONNECTION_URL\n." +
                                              "jdbc:[db type]:[driver type]://[host]:[port]/[service name]")
                else:
                    # 用户第一次连接，而且没有指定环境变量
                    raise SQLCliException("Missing required argument\n." + "connect [user name]/[password]@" +
                                          "jdbc:[db type]:[driver type]://[host]:[port]/[service name]")
        elif len(connect_parameters) == 4:
            # 用户写法是connect user xxx password xxxx; 密码可能包含引号
            if connect_parameters[0].upper() == "USER" and connect_parameters[2].upper() == "PASSWORD":
                self.db_username = connect_parameters[1]
                self.db_password = connect_parameters[3].replace("'", "").replace('"', "")
                if not self.db_url:
                    if "SQLCLI_CONNECTION_URL" in os.environ:
                        # 从环境变量里头拼的连接字符串
                        connect_parameters = [var for var in re.split(r'//|:|@|/',
                                                                      os.environ['SQLCLI_CONNECTION_URL']) if var]
                        if len(connect_parameters) == 6:
                            self.db_type = connect_parameters[1]
                            self.db_driver_type = connect_parameters[2]
                            self.db_host = connect_parameters[3]
                            self.db_port = connect_parameters[4]
                            self.db_service_name = connect_parameters[5]
                            self.db_url = \
                                connect_parameters[0] + ':' + connect_parameters[1] + ':' +\
                                connect_parameters[2] + '://' + connect_parameters[3] + ':' + \
                                connect_parameters[4] + ':/' + connect_parameters[5]
                        else:
                            print("db_type = [" + str(self.db_type) + "]")
                            print("db_host = [" + str(self.db_host) + "]")
                            print("db_port = [" + str(self.db_port) + "]")
                            print("db_service_name = [" + str(self.db_service_name) + "]")
                            print("db_url = [" + str(self.db_url) + "]")
                            raise SQLCliException("Unexpeced env SQLCLI_CONNECTION_URL\n." +
                                                  "jdbc:[db type]:[driver type]://[host]:[port]/[service name]")
                    else:
                        # 用户第一次连接，而且没有指定环境变量
                        raise SQLCliException("Missing required argument\n." + "connect [user name]/[password]@" +
                                              "jdbc:[db type]:[driver type]://[host]:[port]/[service name]")
        else:
            # 不知道的参数写法
            raise SQLCliException("Missing required argument\n." + "connect [user name]/[password]@" +
                                  "jdbc:[db type]:[driver type]://[host]:[port]/[service name]")

        # 连接数据库
        try:
            if self.db_type.upper() == "ORACLE":
                self.db_conn = jaydebeapi.connect(self.driver_class,
                                                  'jdbc:' + self.db_type + ":" + self.db_driver_type + ":@" +
                                                  self.db_host + ":" + self.db_port + "/" + self.db_service_name,
                                                  [self.db_username, self.db_password],
                                                  [self.jar_file, ])
            elif self.db_type.upper() == "LINKOOPDB":
                self.db_conn = jaydebeapi.connect(self.driver_class,
                                                  'jdbc:' + self.db_type + ":" + self.db_driver_type + "://" +
                                                  self.db_host + ":" + self.db_port + "/" + self.db_service_name +
                                                  ";query_iterator=1",
                                                  [self.db_username, self.db_password],
                                                  [self.jar_file, ])
            else:
                self.db_conn = jaydebeapi.connect(self.driver_class,
                                                  'jdbc:' + self.db_type + ":" + self.db_driver_type + "://" +
                                                  self.db_host + ":" + self.db_port + "/" + self.db_service_name,
                                                  [self.db_username, self.db_password],
                                                  self.jar_file, )
            self.SQLExecuteHandler.set_connection(self.db_conn)
        except Exception as e:  # Connecting to a database fail.
            print('traceback.print_exc():\n%s' % traceback.print_exc())
            print('traceback.format_exc():\n%s' % traceback.format_exc())
            print("db_type = [" + str(self.db_type) + "]")
            print("db_host = [" + str(self.db_host) + "]")
            print("db_port = [" + str(self.db_port) + "]")
            print("db_service_name = [" + str(self.db_service_name) + "]")
            print("db_url = [" + str(self.db_url) + "]")
            print("jar_file = [" + str(self.jar_file) + "]")
            print("driver_class = [" + str(self.driver_class) + "]")
            raise SQLCliException(repr(e))

        yield (
            None,
            None,
            None,
            'Database connected.'
        )

    # 从文件中执行SQL
    def execute_from_file(self, arg, **_):
        if not arg:
            message = "Missing required argument, filename."
            return [(None, None, None, message)]
        try:
            with open(os.path.expanduser(arg), encoding="utf-8") as f:
                query = f.read()
        except IOError as e:
            return [(None, None, None, str(e))]
        return self.SQLExecuteHandler.run(query)

    # 设置一些选项
    def set_options(self, arg, **_):
        if arg is None:
            raise Exception("Missing required argument. set parameter parameter_value.")
        elif arg == "":
            m_Result = []
            for key, value in self.SQLExecuteHandler.options.items():
                m_Result.append([str(key), str(value)])
            yield (
                "Current set options: ",
                m_Result,
                ["option", "value"],
                ""
            )
        else:
            options_parameters = str(arg).split()
            if len(options_parameters) != 2:
                raise Exception("Missing required argument. set parameter parameter_value.")

            # 处理DEBUG选项
            if options_parameters[0].upper() == "DEBUG":
                if options_parameters[1].upper() == 'ON':
                    os.environ['SQLCLI_DEBUG'] = "1"
                else:
                    if 'SQLCLI_DEBUG' in os.environ:
                        del os.environ['SQLCLI_DEBUG']

            # 如果不是已知的选项，则直接抛出到SQL引擎
            if options_parameters[0].upper() in self.SQLExecuteHandler.options:
                self.SQLExecuteHandler.options[options_parameters[0].upper()] = options_parameters[1].upper()
                yield (
                    None,
                    None,
                    None,
                    '')
            else:
                raise CommandNotFound

    # 执行特殊的命令
    def execute_internal_command(self, arg, **_):
        # 去掉回车换行符，以及末尾的空格
        strSQL = str(arg).replace('\r', '').replace('\n', '').strip()

        # 创建数据文件
        matchObj = re.match(r"create\s+file\s+(.*?)\((.*)\)(\s+)?rows\s+([1-9]\d*)(\s+)?(;)?$",
                            strSQL, re.IGNORECASE)
        if matchObj:
            # create file command  将根据格式要求创建需要的文件
            Create_file(p_filename=str(matchObj.group(1)),
                        p_formula_str=str(matchObj.group(2)),
                        p_rows=int(matchObj.group(4)),
                        p_options=self.SQLExecuteHandler.options)
            yield (
                None,
                None,
                None,
                str(matchObj.group(4)) + ' rows created Successful.')
            return

        # 创建随机数Seed的缓存文件
        matchObj = re.match(r"create\s+seeddatafile(\s+)?;$",
                            strSQL, re.IGNORECASE)
        if matchObj:
            Create_SeedCacheFile()
            yield (
                None,
                None,
                None,
                'file created Successful.')
            return

        # 不认识的internal命令
        raise SQLCliException("Unknown internal Command. Please double check.")

    # 逐条处理SQL语句
    # 如果执行成功，返回true
    # 如果执行失败，返回false
    def one_iteration(self, text=None):
        # 判断传入SQL语句， 如果没有传递，则表示控制台程序，需要用户输入SQL语句
        if text is None:
            full_text = None
            while True:
                # 用户一行一行的输入SQL语句
                try:
                    if full_text is None:
                        text = self.prompt_app.prompt('SQL> ')
                    else:
                        text = self.prompt_app.prompt('   > ')
                except KeyboardInterrupt:
                    # KeyboardInterrupt 表示用户输入了CONTROL+C
                    return True
                # 拼接SQL语句
                if full_text is None:
                    full_text = text
                else:
                    full_text = full_text + '\n' + text
                # 判断SQL语句是否已经结束
                (ret_bSQLCompleted, ret_SQLSplitResults, ret_SQLSplitResultsWithComments) = SQLAnalyze(full_text)
                if ret_bSQLCompleted:
                    # SQL 语句已经结束
                    break
            text = full_text

        # 如果文本是空行，直接跳过
        if not text.strip():
            return True

        try:
            # 执行需要的SQL语句
            result = self.SQLExecuteHandler.run(text)

            # 输出显示结果
            self.formatter.query = text
            for title, cur, headers, status in result:
                # 不控制每行的长度
                max_width = None

                # title 包含原有语句的SQL信息，如果ECHO打开的话
                # headers 包含原有语句的列名
                # cur 是语句的执行结果
                # output_format 输出格式
                #   ascii              默认，即表格格式
                #   vertical           分行显示，每行、每列都分行
                #   csv                csv格式显示
                formatted = self.format_output(
                    title, cur, headers,
                    self.SQLExecuteHandler.options["OUTPUT_FORMAT"].lower(),
                    max_width
                )

                # 输出显示信息
                try:
                    self.output(formatted, status)
                except KeyboardInterrupt:
                    # 显示过程中用户按下了CTRL+C
                    pass

            # 返回正确执行的消息
            return True
        except EOFError as e:
            # 当调用了exit或者quit的时候，会受到EOFError，这里直接抛出
            raise e
        except SQLCliException as e:
            # 用户执行的SQL出了错误, 由于SQLExecute已经打印了错误消息，这里直接退出
            self.output(None, e.message)
            return False
        except Exception as e:
            if "SQLCLI_DEBUG" in os.environ:
                print('traceback.print_exc():\n%s' % traceback.print_exc())
                print('traceback.format_exc():\n%s' % traceback.format_exc())
            self.echo(repr(e), err=True, fg="red")
            return False

    # 主程序
    def run_cli(self):
        # 程序运行的结果
        m_runCli_Result = True

        # 打开输出日志, 如果打开失败，就直接退出
        try:
            if self.logfilename is not None:
                self.logfile = open(self.logfilename, mode="w", encoding="utf-8")
                self.SQLExecuteHandler.logfile = self.logfile
        except IOError as e:
            if "SQLCLI_DEBUG" in os.environ:
                print('traceback.print_exc():\n%s' % traceback.print_exc())
                print('traceback.format_exc():\n%s' % traceback.format_exc())
            self.echo("Can not open logfile for write [" + self.logfilename + "]", err=True, fg="red")
            self.echo(repr(e), err=True, fg="red")
            return False

        # 处理传递的映射文件
        if self.sqlmap is not None:   # 如果传递的参数，有Mapping，以参数为准，先加载参数中的Mapping文件
            self.SQLMappingHandler.Load_SQL_Mappings(self.sqlscript, self.sqlmap)
        elif "SQLCLI_SQLMAPPING" in os.environ:     # 如果没有参数，则以环境变量中的信息为准
            self.SQLMappingHandler.Load_SQL_Mappings(self.sqlscript, os.environ["SQLCLI_SQLMAPPING"])
        else:  # 任何地方都没有sql mapping信息，设置QUERYREWRITE为OFF
            self.SQLExecuteHandler.options["SQLREWRITE"] = "OFF"

        # 给Page做准备，PAGE显示的默认换页方式.
        if not os.environ.get("LESS"):
            os.environ["LESS"] = "-RXF"

        # 如果参数要求不显示版本，则不再显示版本
        if not self.nologo:
            self.echo("SQL*Cli Release " + __version__)

        # 如果运行在脚本方式下，不在调用PromptSession
        # 调用PromptSession会导致程序在IDE下无法运行
        # 对于脚本程序，在执行脚本完成后就会自动退出
        if self.sqlscript is None:
            self.prompt_app = PromptSession()

        # 开始依次处理SQL语句
        try:
            # 如果环境变量中包含了SQLCLI_CONNECTION_JAR_NAME或者SQLCLI_CONNECTION_CLASS_NAME
            # 则直接加载
            if "SQLCLI_CONNECTION_JAR_NAME" in os.environ and \
                    "SQLCLI_CONNECTION_CLASS_NAME" in os.environ:
                self.one_iteration(
                    'loaddriver ' +
                    os.environ["SQLCLI_CONNECTION_JAR_NAME"] + ' ' +
                    os.environ["SQLCLI_CONNECTION_CLASS_NAME"])

            # 如果用户制定了用户名，口令，尝试直接进行数据库连接
            if self.logon:
                if not self.one_iteration("connect " + str(self.logon)):
                    m_runCli_Result = False
                    raise EOFError

            # 如果传递的参数中有SQL文件，先执行SQL文件, 执行完成后自动退出
            if self.sqlscript:
                if not self.one_iteration('start ' + self.sqlscript):
                    m_runCli_Result = False
                    raise EOFError
                self.one_iteration('exit')

            # 循环从控制台读取命令
            while True:
                if not self.one_iteration():
                    m_runCli_Result = False
                    raise EOFError
        except EOFError:
            # 用户调用了Exit或者Quit信息
            self.echo("Disconnected.")
            return m_runCli_Result

    def log_output(self, output):
        if self.logfile:
            click.echo(output, file=self.logfile)

    def echo(self, s, **kwargs):
        """Print a message to stdout.

        All keyword arguments are passed to click.echo().

        """
        self.log_output(s)
        click.secho(s, **kwargs)

    def output(self, output, status=None):
        """
        Output text to stdout or a pager command.

        The status text is not outputted to pager or files.

        The message will be written to the output file, if enabled.

        """
        if output:
            # size    记录了 每页输出最大行数，以及行的宽度。  Size(rows=30, columns=119)
            # margin  记录了每页需要留下多少边界行，如状态显示信息等 （2 或者 3）
            m_size_rows = 30
            m_size_columns = 119
            margin = 3

            # 打印输出信息
            fits = True
            buf = []
            output_via_pager = ((self.SQLExecuteHandler.options["PAGE"]).upper() == "ON")
            for i, line in enumerate(output, 1):
                self.log_output(line)       # 输出文件中总是不考虑分页问题
                if fits or output_via_pager:
                    # buffering
                    buf.append(line)
                    if len(line) > m_size_columns or i > (m_size_rows - margin):
                        # 如果行超过页要求，或者行内容过长，且没有分页要求的话，直接显示
                        fits = False
                        if not output_via_pager:
                            # doesn't fit, flush buffer
                            for bufline in buf:
                                click.secho(bufline)
                            buf = []
                else:
                    click.secho(line)

            if buf:
                if output_via_pager:
                    click.echo_via_pager("\n".join(buf))
                else:
                    for line in buf:
                        click.secho(line)

        if status:
            self.log_output(status)
            click.secho(status)

    def format_output(self, title, cur, headers, p_format_name, max_width=None):
        output = []

        output_kwargs = {
            "dialect": "unix",
            "disable_numparse": True,
            "preserve_whitespace": True,
            "preprocessors": (preprocessors.align_decimals,),
            "style": self.output_style,
        }

        if title:  # Only print the title if it's not None.
            output = itertools.chain(output, [title])

        if cur:
            # 列的数据类型，如果不存在，按照None来处理
            column_types = None
            if hasattr(cur, "description"):
                def get_col_type(col):
                    # col_type = FIELD_TYPES.get(col[1], text_type)
                    # return col_type if type(col_type) is type else text_type
                    return str

                column_types = [get_col_type(col) for col in cur.description]

            if max_width is not None:
                cur = list(cur)

            formatted = self.formatter.format_output(
                cur,
                headers,
                format_name=p_format_name,
                column_types=column_types,
                **output_kwargs
            )
            if isinstance(formatted, str):
                formatted = formatted.splitlines()
            formatted = iter(formatted)

            # 获得输出信息的首行
            first_line = next(formatted)
            # 获得输出信息的格式控制
            formatted = itertools.chain([first_line], formatted)
            # 返回输出信息
            output = itertools.chain(output, formatted)
        return output


@click.command()
@click.option("--version", is_flag=True, help="Output sqlcli's version.")
@click.option(
    "--logon",
    type=str,
    help="logon user name and password. user/pass",
)
@click.option(
    "--logfile",
    type=str,
    help="Log every query and its results to a file.",
)
@click.option("--execute", type=str, help="Execute SQL script.")
@click.option("--sqlmap", type=str, help="SQL Mapping file.")
@click.option("--nologo", is_flag=True, help="Execute with silent mode.")
def cli(
        version,
        logon,
        logfile,
        execute,
        sqlmap,
        nologo
):
    if version:
        print("Version:", __version__)
        sys.exit(0)

    sqlcli = SQLCli(
        logfilename=logfile,
        logon=logon,
        sqlscript=execute,
        sqlmap=sqlmap,
        nologo=nologo
    )

    # 运行主程序
    sqlcli.run_cli()


if __name__ == "__main__":
    cli()
